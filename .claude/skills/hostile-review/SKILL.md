---
name: hostile-review
description: Run a hostile multi-agent review of the distributed-systems code in this repo — validate external Kinesis/KCL/AWS facts against authoritative sources, hunt race conditions, and analyze crash/recovery scenarios. Use before a release, after touching the receiver coordinator/poller/lease code or the exporter, or to periodically re-validate that external-behavior assumptions still hold.
---

# hostile-review

This repo's correctness rests on assumptions about **external systems** (AWS
Kinesis API semantics, KCL behavior) and on **concurrent, failure-prone**
coordination code. Both rot silently: AWS changes API behavior, and refactors
introduce races or recovery gaps. This skill runs three independent, adversarial
reviewers through fresh context to catch that drift.

Run it before a release, after any change to `receiver/awskinesisreceiver/`
(coordinator, poller, lease coordination), `exporter/awskinesisexporter/`, or
`internal/lease/`, and on a regular cadence to re-validate external facts.

## How to run

Launch the three agents **in parallel** (one message, three `Agent` tool calls,
`subagent_type: general-purpose`). Each is independent and read-only. Then
triage their findings yourself: dedupe, confirm severity, and fix real issues
with tests — do not rubber-stamp an agent's claim, and do not let a "no issues"
report end the review without spot-checking.

Tell every agent: be HOSTILE, assume the code is wrong until proven safe, cite
sources, report `severity` + `file:line` + concrete failure timeline + fix, and
separate CONFIRMED-correct from PROBLEMS.

### Agent 1 — external-fact validator (research + cite)

Validate every claim the code/docs make about real Kinesis/KCL/AWS behavior
against authoritative sources (AWS API reference, KCL GitHub/docs) using
WebSearch/WebFetch. Targets that have bitten us before, re-check each time:

- **Lease/shard cleanup signal.** Is "absent from `ListShards`" safe, given
  ListShards is eventually consistent and paginated? What signal does KCL's
  `LeaseCleanupManager` actually use (SHARD_END + child leases PROCESSING)?
- **SHARD_END detection.** Is nil `NextShardIterator` the right signal? Should
  `GetRecords` `ChildShards` be consumed for the reshard graph?
- **Reshard fields.** `ParentShardId` / `AdjacentParentShardId` semantics for
  splits vs merges; parent-before-child correctness for both.
- **Partition-key → shard.** MD5 hashing into hash-key ranges; is tag locality
  stable across a reshard? (It is not.)
- **Iterator expiry vs throttling.** ExpiredIteratorException (~5 min) vs
  ProvisionedThroughputExceededException; the 5 reads/s/shard and
  GetShardIterator 5 TPS/shard limits vs our poll interval and retry behavior.
- **PutRecords partial failure.** Duplicate re-send on whole-batch retry; per-
  record `ErrorCode` handling vs KPL.
- **KCL lease-table schema.** Exact column names; which we omit
  (`checkpointSubSequenceNumber`, `ownerSwitchesSinceCheckpoint`, ...); whether
  the "KCL can share the table" claim is justified.
- **Lease timing.** heartbeat/lease-duration ratio (~duration/3) and our
  defaults.

Have it explicitly validate the claims in `docs/kcl-lite.md`.

### Agent 2 — race-condition hunter (only concurrency)

Find ONLY data races, lock-ordering problems, goroutine leaks, and
unsynchronized shared state. Nothing else. Cover:

- `coordinator.go` shared maps (`active`, `observed`, `liveShards`, `absent`)
  under `c.mu`; the generation-token defer in `startPoller`; reconcile vs
  poller-exit vs `cleanupOrphans` vs `drainAndStop` interleavings; cross-process
  hazards fenced only by the lease counter.
- `poller.go` two-goroutine design (poll + heartbeat), `leaseMu` discipline on
  `p.leased`, `drainCh`/`drainOnce`, `stop()` vs `drain()`, locks held across
  network calls.
- `receiver.go` `Shutdown` wait goroutine + hard-cancel fallback.
- `internal/lease/memory.go` mutex coverage and `List` returning clones.
- `internal/encoding/codec.go` shared pooled zstd encoder/decoder; exporter
  fields shared across concurrent ConsumeTraces/ConsumeMetrics.

Ask it to state, per finding, whether `go test -race` would catch or miss it.

### Agent 3 — crash & recovery analyst

Trace concrete failure timelines for DATA LOSS / UNBOUNDED DUPLICATION / STUCK
SHARDS / LEAKS:

- Consumer crash mid-batch before checkpoint (verify checkpoint strictly after
  delivery → at-least-once).
- Forced steal during an in-flight batch (duplication window bounded? clean
  exit?).
- Crash before/during `writeShardEnd` (children stuck? recoverable?).
- `cleanupOrphans` deleting a lease that is still needed (blast radius).
- Restart with random vs stable `worker_id` (reclaim delay).
- Permanent downstream backpressure (does it keep heartbeating, or trigger a
  steal storm?).
- Exporter PutRecords partial failure (duplicate re-send; infinite retry on a
  permanently-rejected record?).
- Shutdown deadline racing a stuck GetRecords (release still runs? wait()
  hangs?).
- An indivisible oversize leaf (dropped + counted; no wedge/loop?).

## After the agents return

1. Dedupe (the external-fact and crash agents often converge on the same bug).
2. For each real finding, write or update a test that fails before the fix, then
   fix it. Distributed-systems bugs without a regression test will return.
3. Update `docs/kcl-lite.md`'s coverage matrix and caveats if an assumption
   changed.
4. If an external fact was refuted, fix the code AND the doc claim, and add the
   cited source so the next run starts from corrected ground.
