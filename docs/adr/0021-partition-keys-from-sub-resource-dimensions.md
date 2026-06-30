# 0021. Partition keys from sub-resource dimensions

- **Status:** Accepted
- **Date:** 2026-06-30

## Context

[ADR-0012](0012-tag-grouping-and-oversize-repack.md) introduced the `tag_hash`
partition strategy, which derives the key from an ordered list of **resource
attributes** (`partition_key.tags`). Resource attributes are the natural anchor
because all three signals share them identically, keeping the group / encode /
PutRecords path signal-agnostic.

Real deployments hit two gaps:

- A **per-producer identifier** often lives as a **datapoint attribute** —
  attached by the ingestion receiver to each metric point, span, or log record —
  not on the resource. Partitioning by it routes all data for one producer to
  the same shard, preserving per-producer ordering.
- A single dimension is too coarse for a producer that needs to fan across
  shards. A natural second dimension is a grouping derived from the **metric
  name** (e.g. a subsystem prefix extracted by a regex). Traces and logs have no
  metric name, so the field must contribute an empty segment for them rather than
  failing at config time.

The conventional fix is a `groupbyattrs` + `transform` processor chain that
hoists the chosen attributes from the datapoint level to the resource level
before the exporter groups by them. This has two problems:

1. **Processor availability.** A minimal collector distribution built only for
   this exporter may not bundle `groupbyattrs`. Requiring it makes minimal builds
   harder to justify.
2. **Wire mutation.** Hoisting a per-series label to the resource changes the
   OTLP on the wire. A downstream Prometheus remote-write turns a resource
   attribute into a `target_info` entry, silently breaking label filters and
   queries that expect the attribute at the series level. Undoing the mutation
   requires additional downstream config (`promote_resource_attributes`) and
   careful verification.

## Decision

Extend `PartitionKeyConfig` with an ordered `keys` list of `PartitionKeySource`
entries. Each entry declares a `source` — `resource` (a resource attribute),
`datapoint` (a metric datapoint / span / log-record attribute), or `metric_name`
(the metric's name, empty for traces and logs) — plus `name` (the attribute key,
unused for `metric_name`) and an optional `regex`.

The key is derived **inside the exporter** from the resolved value at the
declared level. The OTLP payload handed to Kinesis is **byte-identical** to the
input: the source data is read but never written. `MutatesData: false` is
preserved — all writes target freshly allocated pdata objects.

`regex`, when set, reduces the resolved value to the first capture group of the
first match (or the whole match when the pattern has no group); a non-match
contributes an empty segment. A single capture group is the deliberate ceiling
— richer string rewriting belongs in a processor, not the exporter.

`promote` (optional per-source) is the single opt-in exception: when set, the
resolved post-regex value is written as an attribute under the given name at the
source-native level (`resource` → resource attribute; `datapoint`/`metric_name`
→ the record leaf), **only if that attribute is absent**. This lets a derived
partition dimension (e.g. a subsystem extracted from the metric name) also exist
downstream as a real queryable label without the relocate/destroy semantics of a
transform processor.

The existing `tags` shorthand (an ordered list of resource attribute keys) is
retained as-is and remains equivalent to listing the same keys under `keys` with
`source: resource`. The two are mutually exclusive. All existing `tag_hash`
configs are unaffected.

## Consequences

- A `datapoint` or `metric_name` source groups below the resource, so one input
  batch can fan into multiple records — one per distinct key tuple. This is the
  intended effect (spreading a producer across shards), but operators must choose
  dimensions coarse enough to keep records usefully sized. A regex-derived prefix
  on the metric name bounds the fan-out; using the raw metric name approaches one
  record per series.
- Without `promote`, the wire payload is byte-identical end to end. Downstream
  label mapping, Prometheus `target_info`, and queries that filter on per-series
  labels are unaffected — nothing was moved or destroyed.
- With `promote`, the payload is strictly additive: only absent attributes are
  added, and only to freshly allocated output objects. The absent-only guard
  preserves idempotency under retry and ensures that an upstream value is never
  silently overwritten.
- The `tags` shorthand and all existing resource-only `keys` configs produce
  byte-identical results to the pre-0021 behavior. No migration is needed.
- Records sharing a key tuple continue to route to the same partition key and
  therefore the same shard, in order. Tag→shard locality is not stable across a
  reshard, as before.
- The metrics E2E now demonstrates sub-resource keys directly (using a
  `datapoint` source for `device.id`), replacing the earlier `groupbyattrs`
  processor step. The `groupbyattrs` approach remains valid for builds that
  include the processor and need the resource-level attribute location.
