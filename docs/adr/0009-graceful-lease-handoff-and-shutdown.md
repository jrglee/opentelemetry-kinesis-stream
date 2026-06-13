# 0009. Graceful lease handoff and shutdown drain

- **Status:** Accepted
- **Date:** 2026-06-13

## Context

Exactly-once delivery is the goal, but it cannot be strictly enforced across
Kinesis and a downstream pipeline without a distributed transaction the system
does not have. The realistic target, standard for stateful consumers and the
approach KCL takes, is: deliver exactly once on the planned paths (rebalance,
shutdown, redeploy) and accept at-least-once only on unplanned failures
(crashes).

The duplicate-causing path is a lease changing hands while the previous owner
has delivered records it has not yet checkpointed. The next owner resumes from
the last checkpoint and re-delivers that in-flight batch. Two planned events
trigger handoffs: fair-share **rebalancing** (a worker releases a surplus
shard, [0008](0008-leaderless-fair-share-rebalancing.md)) and **collector
shutdown** (redeploy, scale-down). Both were previously implemented as a hard
context cancel, which abandons the in-flight batch uncheckpointed.

## Decision

Give the poller a graceful **drain** signal, distinct from context
cancellation. On drain the poller finishes its in-flight batch, persists that
batch's checkpoint, releases the lease, and exits. Context cancellation remains
the hard path that aborts mid-batch.

- **Frequent checkpoints.** The poll loop already checkpoints after every
  batch, so the persisted position is never more than one batch behind. This
  bounds the worst-case replay and makes a clean drain leave nothing behind.
- **Rebalance release is a drain.** When a worker sheds a surplus shard, it
  drains the poller rather than cancelling it. The peer that acquires the freed
  lease resumes from a current checkpoint with no re-delivered records.
- **Shutdown is a drain with a deadline.** `Shutdown` stops shard discovery so
  no new pollers start, then drains every active poller. Pollers run on a base
  context that survives discovery-stop, so they finish cleanly. If the
  collector's shutdown deadline fires first, in-flight pollers are
  hard-cancelled (their Release calls are independently time-bounded) and the
  deadline error is returned.
- **Forced steal stays at-least-once.** A bootstrap steal — the only way a new
  idle worker becomes visible when every shard is already owned — still preempts
  a healthy owner without a drain, so it can re-deliver one shard's in-flight
  batch. This is bounded to worker-join events and matches KCL, which also
  reprocesses on lease transfer.

## Consequences

- Planned handoffs (rebalance shed, shutdown, rolling redeploy) are effectively
  exactly-once: the releasing owner checkpoints its last batch before the lease
  moves.
- Crashes and bootstrap steals remain at-least-once. The receiver does not
  claim exactly-once, only that the planned paths do not duplicate.
- Shutdown latency is bounded by one in-flight batch per shard plus the
  Release call, falling back to the collector's deadline. A poller stuck in a
  long `GetRecords` is hard-cancelled at the deadline rather than blocking
  shutdown indefinitely.
- The drain signal is local (a channel), so it adds no lease-table traffic. It
  composes with the counter fencing: a drained poller's final checkpoint and
  release are ordinary conditional writes.
- Eliminating the remaining bootstrap-steal duplicates would require a worker
  presence record so idle workers are counted without owning a lease, letting
  an over-target owner shed proactively instead of being stolen from. That is a
  future refinement; KCL-classic lives with the same bootstrap reprocessing.
