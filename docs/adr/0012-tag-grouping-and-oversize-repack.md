# 0012. Tag-hash partition keys, group-by-tag microbatching, oversize repack

- **Status:** Accepted (oversize policy section superseded in part by
  [ADR-0019](0019-oversize-recovery-chain.md))
- **Date:** 2026-06-13

## Context

Two wire-adjacent behaviors from the design were deferred in the PoC: the
partition-key strategy (random vs a deterministic hash over operator-chosen
tags) and what to do when a record exceeds the per-record size limit (the PoC
dropped it). Tag-heavy workloads (e.g. InfluxDB metrics) want records grouped so
that telemetry sharing a tag tuple lands together, and large batches must be
made to fit rather than lost.

## Decision

- **Partition keys**: `partition_key.strategy` is `random` (default, a UUID per
  record — unchanged behavior) or `tag_hash`. Under `tag_hash` the exporter
  reads an ordered list of resource attributes (`partition_key.tags`), joins
  their values, and uses an xxhash of that as the partition key. Records sharing
  a tag tuple therefore get the same key and land on the same shard, giving the
  receiver per-tuple locality.
- **Group-by-tag microbatching**: under `tag_hash`, the exporter splits an
  incoming batch into one record per distinct tag tuple (resources sharing a
  tuple are coalesced into a single record). Under `random` the whole batch is
  one record, as before.
- **Oversize repack** (this part is superseded by
  [ADR-0019](0019-oversize-recovery-chain.md)): the original decision was a
  single `oversize.policy` field with `split_half` (default), `drop_largest`,
  or `reject`, bounded by `oversize.max_attempts`. ADR-0019 replaces this
  with the `oversize.policies` ordered chain, drops `drop_largest`, and adds
  `truncate_attribute_values`. The drop-counter rationale (one meter rather
  than silent loss) stands; reason labels broadened in ADR-0019.

The grouping/repack logic is signal-agnostic; only the pdata operations differ
between traces and metrics.

## Consequences

- Operators get shard locality keyed on the tags they care about, which the
  receiver's per-shard ordering then preserves. InfluxDB tags become group keys
  once promoted to resource attributes (e.g. via `groupbyattrsprocessor`).
- Oversize records are made to fit by splitting, so large batches survive
  instead of being dropped. The bound on attempts plus the drop-and-count
  fallback keep a pathological single huge item from looping.
- ~~`drop_largest` is implemented as a thin variant of the split recursion~~
  (removed in ADR-0019: the alias was indistinguishable from `split_half` in
  practice and was misleading users about what knob they had).
- Random partitioning remains the default, so existing deployments and the
  traces E2E are unaffected.
