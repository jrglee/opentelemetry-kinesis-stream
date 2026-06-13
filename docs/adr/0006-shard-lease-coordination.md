# 0006. Shard lease coordination via a KCL-shaped lease store

- **Status:** Accepted
- **Date:** 2026-06-13

## Context

A Kinesis consumer must read each shard exactly once across a deployment,
resume from where it left off after a restart, and hand a shard's children
off only after the parent is drained. The Amazon-blessed answer is the
Kinesis Client Library, which has no idiomatic Go port — the reason no
Kinesis receiver exists in contrib. The receiver takes this on directly.

The full KCL state machine (work-stealing, capacity-aware rebalancing,
deterministic placement) is more than the workloads in scope need. What is
needed is: safe ownership transfer, durable checkpoints, and the
parent-before-child ordering guarantee.

## Decision

Coordinate shards through a small `Store` interface with two
implementations, and split lease writes between two roles.

- **Fencing token.** Each lease row carries a `Counter`. Every mutating
  write is conditional on the observed `Counter` and bumps it. A stale writer
  — a previous owner that has not yet noticed it lost the lease — fails the
  conditional write. This is what makes ownership transfer safe without a
  separate coordination service.
- **Two writer roles.** The coordinator owns `Acquire` (claim and steal);
  each shard's poller owns `Heartbeat`, `Checkpoint`, and `Release` for its
  own lease. One writer per lease at a time keeps `Counter` monotonic without
  cross-replica locking. In-process, a mutex serializes the poller's
  checkpoint with its own heartbeat goroutine.
- **Stealing.** A lease whose `Counter` has not advanced for `LeaseDuration`
  is stealable. The coordinator tracks last-observed counters locally to
  decide staleness.
- **Parent-before-child.** A shard whose parents' leases are not all at the
  `SHARD_END` checkpoint is skipped during acquisition. Original shards (no
  parents) and shards whose parent rows have been trimmed past retention are
  treated as drained.
- **Two stores.** An in-process `MemoryStore` for single-replica development
  and unit tests; a `DynamoDBStore` for multi-replica deployments. The
  DynamoDB table uses **KCL's lease-table column layout** (`leaseKey`,
  `leaseOwner`, `leaseCounter`, `checkpoint`, `parentShardId`) so a real KCL
  consumer can share the same table without re-ingesting from `TRIM_HORIZON`.
  The commitment is to the table layout, not to KCL's full state machine.

## Consequences

- Multi-replica safety rests on one mechanism — the conditional-write fencing
  token — which is simple to reason about and test. The `MemoryStore`
  contract tests are the executable spec the `DynamoDBStore` must match.
- KCL-table compatibility keeps a migration path to and from official KCL
  consumers open, which is operationally valuable, at the cost of inheriting
  KCL's column names rather than choosing our own.
- The deliberate omissions (work-stealing, capacity rebalancing) mean
  mismatched shard and replica counts produce hot spots. Observability, not
  rebalancing, is the answer in scope.
- Staleness is tracked per-coordinator in memory, so a freshly started
  replica cannot steal an expired lease until it has observed the stale
  counter for a full `LeaseDuration`. A last-heartbeat timestamp on the lease
  row would remove this warm-up; it is deferred until it bites.
- `Counter` conflates liveness (heartbeat) and progress (checkpoint): both
  bump it. This is correct but means a checkpoint cannot ride a heartbeat to
  save a write. Splitting the two is a future refinement, not a correctness
  fix.
- Reshard faithfulness against the emulator is unverified at the point this
  ADR is written — see [0004](0004-testing-strategy-middleware-ministack-real-aws.md).
  The parent-before-child logic is unit-tested against synthetic lease state;
  whether the emulator reports shard parentage the way real Kinesis does is
  the open question.
