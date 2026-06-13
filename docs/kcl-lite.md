# KCL-lite: shard coordination design and coverage

The receiver's shard ownership, rebalancing, checkpointing, and reshard
handling are a deliberately small re-implementation of the proven design of the
**Amazon Kinesis Client Library (KCL) 2.x**. This document maps our components
onto KCL's, states the algorithm we borrow, and is explicit about which KCL
features we implement, partially implement, or omit — so operators set correct
expectations and contributors know where the edges are.

We do not vendor KCL (it has no idiomatic Go port — that absence is the whole
reason this project exists). We instead copy KCL's decomposition and its
lease-count load-balancing algorithm, and we keep the **DynamoDB lease table
schema KCL-compatible** so a real KCL consumer can share the table.

## Component mapping

| KCL 2.x | This project | Role |
|---------|--------------|------|
| `Scheduler` / `LeaseCoordinator` | `coordinator` (receiver) | Orchestrates discovery, taking, and the per-shard processors. |
| `LeaseTaker.takeLeases()` | `lease.Plan` (`internal/lease`) | Pure fair-share decision: which leases to acquire, release, or steal this pass. |
| `LeaseRenewer` | the poller's heartbeat goroutine | Renews held leases by bumping `leaseCounter`. **Divergence:** renewal is per-shard-consumer here, not a single centralized renewer. |
| `LeaseRefresher` (DynamoDB DAO) | `lease.Store` (`memory`, `dynamodb`) | Conditional reads/writes against the lease table. |
| `ShardConsumer` + `RecordProcessor` | the poller + the `sink` | `GetRecords` loop, decode, deliver, checkpoint. |
| `PeriodicShardSyncManager` | `coordinator.discoverShards` | `ListShards`, ensure a lease per shard, record parent links. **Divergence:** every replica runs discovery (no leader election). |
| `Lease` | `lease.Lease` | `leaseKey`, `leaseOwner`, `leaseCounter`, `checkpoint`, `parentShardId`. |

## The load-balancing algorithm (LeaseTaker)

Each replica runs the same computation every discovery interval, identical in
shape to KCL 2.x:

- **Target** = `ceil(activeShards / activeWorkers)`, where an active worker is a
  distinct owner whose `leaseCounter` advanced within the lease duration (plus
  self). KCL computes the same `ceil(numLeases / numWorkers)`.
- **Take expired first.** A lease whose counter has not advanced for
  `lease_duration` is expired and freely acquirable. We acquire unowned and
  expired leases up to target before considering a steal — matching KCL's rule
  that stealing happens only when no expired leases are available.
- **Steal one per pass.** If still under target and nothing is free, take one
  lease from the most-loaded fresh owner (one holding strictly more than
  target). One steal per pass bounds churn, as in KCL.
- **Fencing.** Every mutating write is conditional on `leaseCounter`; a stale
  writer loses the conditional write. This is KCL's lease-theft detection.

The decision function `lease.Plan` is pure and table-tested; the conditional
writes live in `lease.Store`.

## Checkpoint, handoff, reshard

- **Checkpoint after delivery.** The checkpoint advances only after downstream
  acceptance, so a crash re-reads rather than loses — KCL's at-least-once
  contract.
- **Lease cleanup.** Dead lease rows for trimmed shards are reaped so the table
  does not accumulate `SHARD_END` parent rows across a stream's resharding
  history. Safety is layered, because `ListShards` is eventually consistent and
  a transient omission of a live shard must never destroy a checkpoint: only a
  lease already at `SHARD_END` is reapable (a row with a real sequence-number
  checkpoint is an *active* shard and is never deleted), and only after the
  shard is absent from `ListShards` for several consecutive discovery passes.
  The conditional delete fences on the lease counter. We do NOT use a DynamoDB
  TTL — it is time-based and could delete an active lease. This is a simplified
  form of KCL's `LeaseCleanupManager` (KCL reaps after child leases enter
  PROCESSING; we reap on sustained trim, which is safe for the same reason —
  a still-needed shard is never gone from `ListShards`).
- **Graceful handoff.** A planned release (rebalance shed) or shutdown drains:
  finish the in-flight batch, checkpoint, then release. The next owner resumes
  cleanly. This is KCL's `shutdownRequested` (vs `leaseLost`) cooperative path.
- **Parent-before-child.** A child shard's lease is not acquired until every
  parent's checkpoint is `SHARD_END`. KCL blocks child leases the same way.

## DynamoDB lease table schema

KCL-compatible columns we write: `leaseKey` (shard id, HASH), `leaseOwner`,
`leaseCounter`, `checkpoint` (sequence number or `TRIM_HORIZON` / `SHARD_END`),
`parentShardId` (comma-joined). KCL's other columns
(`checkpointSubSequenceNumber`, `ownerSwitchesSinceCheckpoint`, `childShardIds`,
`startingHashKey`/`endingHashKey`, multi-stream `streamName`/`shardID`) are not
written.

The table is **KCL-schema-shaped**, not certified for live bidirectional
interop: a KCL consumer reading our rows would default the missing columns, but
we have not validated that a stock KCL version resumes cleanly from a checkpoint
row lacking `checkpointSubSequenceNumber`. Treat KCL interop as a migration aim,
not a tested guarantee, until exercised against a real KCL consumer.

## Feature coverage vs KCL 2.x/3.x

| Feature | Status | Notes |
|---------|--------|-------|
| Lease acquisition (conditional take) | **Implemented** | Counter 0 → 1 conditional write. |
| Lease renewal + fencing | **Implemented** | Per-shard heartbeat; counter CAS. |
| Expiry-based reclaim | **Implemented** | Counter not advanced within `lease_duration`. |
| Lease-count load balancing (steal) | **Implemented** | `ceil` target, steal one from most-loaded; matches KCL 2.x. |
| Checkpointing | **Implemented** | After downstream acceptance. |
| `SHARD_END` handling | **Implemented** | Sentinel checkpoint; unblocks children. |
| Parent-before-child resharding | **Implemented** | Gated on parent `SHARD_END`. Verified with a synthetic split (emulators do not model resharding faithfully). |
| Shard discovery / sync | **Partial** | Every replica runs `ListShards`; **no leader election**, so discovery API calls scale with replica count. |
| Graceful handoff | **Implemented** | Cooperative drain + final checkpoint. |
| Leader election for shard sync | **Not implemented** | KCL elects one PeriodicShardSyncManager; we do not. |
| Lease cleanup (delete old leases) | **Implemented** | Leases for shards trimmed from the stream (gone from `ListShards`) are conditionally deleted, so old `SHARD_END` parent rows do not accumulate. Keyed on shard liveness, not a DynamoDB TTL (TTL is time-based and would risk deleting active leases). |
| Worker-utilization (CPU) balancing | **Not implemented** | KCL 3.x feature; we balance on lease count only. |
| KPL sub-sequence checkpoints | **Not implemented** | No `checkpointSubSequenceNumber`; whole-record granularity. |
| Multi-stream | **Not implemented** | One stream per receiver instance. |
| Enhanced fan-out (EFO) | **Not implemented** | Polling (`GetRecords`) only. |
| KCL-table compatibility | **Implemented** | Columns above; migration to/from KCL intended. |

## Guarantees and sharp edges (inherited from KCL)

- **At-least-once, not exactly-once.** A crash or a forced steal can re-deliver
  the in-flight, not-yet-checkpointed batch. Graceful handoff avoids this on
  planned transitions; crashes and bootstrap steals do not.
- **Clock-based expiry.** Reclaim uses elapsed wall-clock between observed
  counter changes. Large clock skew across replicas can cause premature or late
  reclaim — keep hosts NTP-synced, as KCL requires.
- **`heartbeat_interval` < `lease_duration`** is required (validated). KCL keeps
  renewal at roughly `lease_duration / 3`; the defaults here (5s / 30s) follow
  that ratio.
- **Hot spots from mismatched counts.** With more shards than the even split
  allows, some workers carry one extra shard (KCL's "stable disbalance").
  Observability, not finer balancing, is the answer at this scope.
