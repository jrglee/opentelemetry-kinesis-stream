# 0014. Model shard coordination on KCL's proven design

- **Status:** Accepted
- **Date:** 2026-06-13

## Context

Shard ownership, rebalancing, checkpointing, and reshard handling are the
highest-risk part of this project: they either work or silently corrupt
downstream telemetry, and there is no idiomatic Go port of the Kinesis Client
Library to lean on (the reason this project exists). The features were built
incrementally (ADR-0006 lease coordination, ADR-0008 fair-share rebalancing,
ADR-0009 graceful handoff). The risk is that a hand-grown coordination protocol
has subtle bugs a production workload would eventually hit.

The strongest way to derisk a hand-written distributed protocol is to not invent
one: copy a design that has been proven at scale.

## Decision

Model the coordination explicitly on **KCL 2.x's** decomposition and algorithm,
make the mapping a first-class, documented artifact, and bound expectations by
stating the coverage gaps.

- **Same decomposition.** Our `coordinator` plays KCL's
  `LeaseCoordinator`/`LeaseTaker`/`PeriodicShardSyncManager`; the poller's
  heartbeat plays `LeaseRenewer`; `lease.Store` plays `LeaseRefresher`; the
  poller + sink play `ShardConsumer` + `RecordProcessor`.
- **Same lease-count load-balancing algorithm.** `lease.Plan` (the fair-share
  decision) reproduces `LeaseTaker.takeLeases()`: `ceil(shards/workers)` target,
  take expired before stealing, steal one lease per pass from the most-loaded
  owner, conditional-write fencing on `leaseCounter`.
- **KCL-compatible lease table** so a real KCL consumer can share it.
- **Documented coverage** in [`docs/kcl-lite.md`](../kcl-lite.md): a feature
  matrix marking each KCL capability implemented / partial / not-implemented,
  plus the inherited guarantees and sharp edges (at-least-once, clock-skew,
  renewal-vs-duration ratio).
- **Consolidate the KCL-lite core.** The pure decision logic (`lease.Plan`)
  lives with the lease model and store in `internal/lease`, independently
  table-tested, so the algorithm can be reviewed and hardened in one place.

## Consequences

- The riskiest subsystem now has a named, proven reference design; a reviewer
  can check our behavior against KCL's documented algorithm rather than
  reasoning from scratch.
- Gaps are explicit, not surprising: no leader election for shard sync (every
  replica lists shards), no worker-utilization balancing, no multi-stream, no
  EFO. Each is a known, bounded limitation an operator can plan around. Lease
  cleanup IS implemented — leases for trimmed shards are conditionally deleted
  on a liveness key (not a DynamoDB TTL), the KCL `LeaseCleanupManager` role —
  so the table does not accumulate dead `SHARD_END` rows.
- Deliberate divergences are recorded: lease renewal is per-shard-consumer here,
  not a centralized `LeaseRenewer`. This is simpler and works at PoC scale;
  centralizing renewal is the obvious next step if renewal cost or coordination
  becomes a concern.
- We inherit KCL's guarantees AND its caveats. Exactly-once is explicitly not
  claimed; clock sync is a deployment requirement; the renewal/duration ratio is
  validated in config.
