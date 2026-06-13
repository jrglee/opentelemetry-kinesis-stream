# 0008. Leaderless fair-share shard rebalancing

- **Status:** Accepted
- **Date:** 2026-06-13
- **Supersedes the rebalancing stance of:** [0006](0006-shard-lease-coordination.md)

## Context

ADR-0006 deliberately shipped no rebalancing: acquisition was first-come,
first-served, and a healthy owner was never preempted. The consequence —
acknowledged at the time — is that one replica can own every shard while peers
sit idle, and that mismatched shard/replica counts hot-spot.

For real deployments that is not good enough. Adding or removing a replica
should redistribute shards, and a replica that grabbed more than its share at
startup should give some back. The requirement is to do this **without leader
election**: instances coordinate only through the shared DynamoDB lease table,
keeping the "no external control plane" constraint (DESIGN.md §2) intact.

## Decision

Each replica runs the same fair-share computation independently on every
reconcile pass. There is no coordinator role.

- **Active workers.** A worker is "active" if it owns at least one lease whose
  fencing counter has advanced within `lease_duration` (i.e. it is
  heartbeating). The active set is the distinct fresh owners, plus self.
- **Target.** `target = ceil(activeShards / activeWorkers)`, where
  `activeShards` excludes shards already at `SHARD_END`. Every replica computes
  the same target from the same lease snapshot.
- **Release excess.** If a replica owns more than `target` leases, it releases
  the surplus (cancels those pollers; their deferred `Release` frees the
  rows). Freed leases become acquirable by under-target peers.
- **Acquire.** A replica acquires unowned or stale leases up to `target`,
  subject to parent-drains-before-child.
- **Steal.** If a replica is still below `target` and nothing is unowned, it
  takes **one** lease per pass from the most-overloaded fresh owner (an owner
  holding strictly more than `target`). The existing per-lease counter is the
  fencing token: the steal is a conditional `Acquire`, and the victim's next
  heartbeat or checkpoint fails the condition, so its poller stops. At-most-one
  steal per pass bounds churn.

Bootstrap and autoscaling fall out of the same rule: a freshly started replica
counts itself in `activeWorkers`, computes a positive target, and steals its
share, becoming visible to peers via `leaseOwner` as soon as it owns one lease.

The decision logic is a pure function (`planRebalance`) of the lease snapshot,
per-shard freshness, and the worker id; it is unit-tested without AWS, and the
coordinator only executes the returned plan.

## Consequences

- Shards redistribute as replicas join and leave, converging to an
  even-as-possible split (`S=2,W=2` → 1-1; `S=3,W=2` → 2-1, stable).
- Healthy leases are now preempted during rebalancing. The handoff is graceful
  via counter fencing, but a stolen shard can be briefly read by both the old
  and new owner around the handoff — at-least-once delivery, consistent with
  the receiver's existing guarantees. Exactly-once is not claimed.
- Balancing is driven by lease-table reads only; no worker-registry table and
  no leader. The cost is that every replica does the same O(shards) bookkeeping
  each pass, and steals converge over a few passes rather than instantly.
- Freshness is inferred from counter advancement observed over time, so a
  newly started replica needs one `lease_duration` of observation before it can
  judge a peer dead versus merely between heartbeats. This bounds how fast a
  crashed replica's shards are reclaimed, not correctness.
- Rebalancing composes with reshard: a child shard is only ever a steal/acquire
  candidate once its parents are drained, so parent-before-child ordering is
  preserved across rebalancing just as it is across plain acquisition.
