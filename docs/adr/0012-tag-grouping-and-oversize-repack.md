# 0012. Tag-hash partition keys, group-by-tag microbatching, oversize repack

- **Status:** Accepted
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
- **Oversize repack**: a record over `max_record_size` after compression is
  repacked per `oversize.policy` — `split_half` (default; recursively halve the
  resources, then the leaf items within a single resource), `drop_largest`, or
  `reject` — bounded by `oversize.max_attempts`. A single leaf item that is
  still too big is dropped and counted on a `kinesis.exporter.records_dropped`
  meter rather than silently lost.

The grouping/repack logic is signal-agnostic; only the pdata operations differ
between traces and metrics.

## Consequences

- Operators get shard locality keyed on the tags they care about, which the
  receiver's per-shard ordering then preserves. InfluxDB tags become group keys
  once promoted to resource attributes (e.g. via `groupbyattrsprocessor`).
- Oversize records are made to fit by splitting, so large batches survive
  instead of being dropped. The bound on attempts plus the drop-and-count
  fallback keep a pathological single huge item from looping.
- `drop_largest` is implemented as a thin variant of the split recursion; the
  irreducible case (one oversize leaf) drops that leaf. A dedicated
  "remove the single largest top-level resource" pass was not needed because the
  split recursion already resolves multi-resource oversize.
- Random partitioning remains the default, so existing deployments and the
  traces E2E are unaffected.
