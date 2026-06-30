# Proposal: partition keys from sub-resource dimensions

**Status:** proposed (implementation pending)
**Component:** `exporter/awskinesisexporter`
**Scope:** additive, backward compatible

This is an implementation spec, not a decision record. It is self-contained:
an implementer should be able to work from this document plus the existing
exporter code without further context.

## Summary

Extend the `tag_hash` partition strategy so a key can be built from dimensions
that live **below the resource** — a **datapoint/record attribute** and/or the
**metric name** — in addition to resource attributes. The derivation happens
inside the exporter and **does not mutate the telemetry payload**.

## Motivation

Today `tag_hash` derives the partition key only from **resource attributes**
(see the `Tags` doc comment in `config.go` and `tagKey` in `signal.go`). That is
the one level shared identically by traces, metrics, and logs, which keeps the
group/compress/PutRecords path signal-agnostic.

But real deployments need to partition by dimensions that are not at the
resource level:

- A **per-producer identifier** often arrives as a **datapoint attribute** (for
  example, a tag attached to each metric point by an ingestion receiver), not a
  resource attribute. Partitioning by it lets a single high-volume producer's
  data stay ordered per-producer.
- A producer can emit enough data that one partition key is still too coarse, so
  a **second dimension** is needed to fan one producer across shards while
  preserving per-tuple ordering. A natural coarse second dimension is a grouping
  derived from the **metric name** (e.g. a subsystem prefix).

The conventional way to expose these to a resource-only `tag_hash` is to run
`groupbyattrs` + `transform` processors upstream that hoist the values to the
resource level. That has two problems:

1. **It requires processors a given collector build may not bundle.** A minimal
   distribution built only for this exporter may ship just `batch`.
2. **It mutates the OTLP on the wire.** Hoisting a datapoint attribute to the
   resource changes what a downstream consumer sees — e.g. a Prometheus
   remote-write turns a per-series label into a `target_info` attribute, which
   silently breaks queries that filter on that label. Undoing it requires
   downstream config (`promote_resource_attributes`) and verification.

Deriving the key inside the exporter avoids both: no extra processors, and the
**payload is left byte-identical end to end**, so there is zero downstream
label-mapping risk.

## Design

### Configuration

`PartitionKeyConfig` keeps `strategy` / `tags` / `hash` and gains an ordered,
heterogeneous `keys` list. `tags` remains as documented shorthand for "every
entry is a `resource` source" and keeps working unchanged.

```go
// PartitionKeyConfig selects the partition-key strategy. "random" spreads
// records uniformly across shards; "tag_hash" co-locates records that share the
// same ordered key tuple onto a stable partition key, so a downstream consumer
// sees them in order and per-tuple microbatching is possible.
type PartitionKeyConfig struct {
	Strategy string `mapstructure:"strategy"` // "random" (default) | "tag_hash"

	// Tags is shorthand for an ordered list of *resource* attribute keys —
	// equivalent to listing each under Keys with source "resource". Retained for
	// backward compatibility; prefer Keys when any component lives below the
	// resource. Mutually exclusive with Keys.
	Tags []string `mapstructure:"tags"`

	// Keys is the ordered list of components hashed, in order, into the
	// partition key. Each component reads from a different level of the
	// telemetry, so a key can combine dimensions that live below the resource
	// without a preprocessing step. Required (non-empty) for tag_hash unless
	// Tags is set.
	Keys []PartitionKeySource `mapstructure:"keys"`

	Hash string `mapstructure:"hash"` // "xxhash" (default)
}

// PartitionKeySource is one ordered component of a tag_hash key.
type PartitionKeySource struct {
	// Source selects where the value is read from:
	//   - "resource":    a resource attribute (shared by every record under the
	//                    resource; grouping stays resource-granular).
	//   - "datapoint":   a record-level attribute — a metric datapoint
	//                    attribute, a span attribute, or a log-record attribute.
	//                    Selecting it makes the exporter group at record
	//                    granularity (see Grouping), which can raise record
	//                    count.
	//   - "metric_name": the metric's name. Contributes an empty segment for
	//                    traces and logs (no metric name exists there).
	Source string `mapstructure:"source"`

	// Name is the attribute key read for "resource" and "datapoint". Unused (and
	// must be empty) for "metric_name".
	Name string `mapstructure:"name"`

	// Regex optionally reduces the resolved value to a substring. When set, the
	// contributed value is the first capture group of the first match (or the
	// whole match if the pattern has no group); a non-matching value contributes
	// an empty segment, i.e. a deterministic catch-all bucket. Typical use:
	// derive a coarse grouping from a structured metric name, e.g. "^([a-z]+)_"
	// turns "charging_state" into "charging". Deliberately limited to a single
	// capture group — richer rewriting belongs in a processor, not the exporter.
	Regex string `mapstructure:"regex"`
}
```

Source constants (alongside the existing partition consts in `config.go`):

```go
const (
	keySourceResource   = "resource"
	keySourceDatapoint  = "datapoint"
	keySourceMetricName = "metric_name"
)
```

`Validate()` additions in the `tag_hash` branch:

- error if both `tags` and `keys` are set;
- require non-empty `tags` **or** `keys`;
- for each `keys[i]`:
  - `source` must be one of `resource` / `datapoint` / `metric_name` (empty
    defaults to `resource`);
  - `name` is required for `resource` and `datapoint`, and must be empty for
    `metric_name`;
  - `regex`, if set, must compile (`regexp.Compile`) — fail fast with the index
    in the message.

### Resolved plan

Resolve config once into a compiled plan held on the exporter, so the hot path
never recompiles a regex. Put the new types/helpers in a new file
`exporter/awskinesisexporter/partition.go`.

```go
type keySource struct {
	source string
	name   string
	re     *regexp.Regexp // nil when no regex
}
type keyPlan []keySource

// resourceOnly reports whether every component reads a resource attribute, which
// enables the whole-resource fast path.
func (p keyPlan) resourceOnly() bool

// resolveKeyPlan turns config into a compiled plan. Returns nil for the random
// strategy (no grouping). Tags expand to resource sources.
func (c *Config) resolveKeyPlan() (keyPlan, error)
```

A single key-derivation function serves both grouping paths:

```go
var emptyAttrs = pcommon.NewMap() // shared read-only empty map for the fast path

// partitionValue joins the plan's ordered components with tagSep. res is the
// resource attributes; metricName and leaf are "" / empty for sources that don't
// apply (e.g. the resource-only fast path, or metric_name on traces/logs).
func partitionValue(plan keyPlan, res pcommon.Map, metricName string, leaf pcommon.Map) string {
	parts := make([]string, len(plan))
	for i, ks := range plan {
		var v string
		switch ks.source {
		case keySourceResource:
			if a, ok := res.Get(ks.name); ok {
				v = a.AsString()
			}
		case keySourceDatapoint:
			if a, ok := leaf.Get(ks.name); ok {
				v = a.AsString()
			}
		case keySourceMetricName:
			v = metricName
		}
		if ks.re != nil {
			v = firstCapture(ks.re, v)
		}
		parts[i] = v
	}
	return strings.Join(parts, tagSep) // tagSep (0x1f) is already defined in signal.go
}

// firstCapture returns the first capture group of the first match, or the whole
// match when the pattern has no group, or "" when there is no match.
func firstCapture(re *regexp.Regexp, s string) string {
	m := re.FindStringSubmatch(s)
	switch {
	case m == nil:
		return ""
	case len(m) > 1:
		return m[1]
	default:
		return m[0]
	}
}
```

### Grouping: fast path unchanged, leaf path is the new work

The three `groupXByTags` funcs in `signal.go` become thin dispatchers that take
a `keyPlan` instead of `[]string`:

```go
func groupMetricsByTags(md pmetric.Metrics, plan keyPlan) []taggedBatch[pmetric.Metrics] {
	if len(plan) == 0 {
		return []taggedBatch[pmetric.Metrics]{{key: "", batch: md}}
	}
	if plan.resourceOnly() {
		return groupMetricsByResource(md, plan) // existing whole-resource bucketing; key via partitionValue
	}
	return groupMetricsByLeaf(md, plan) // new, in partition.go
}
```

**Fast path (`resourceOnly`)** is today's behavior exactly: bucket whole
`ResourceMetrics` / `ResourceSpans` / `ResourceLogs` by the resource-attribute
key, now computed with `partitionValue(plan, attrs, "", emptyAttrs)` so a
resource source may also carry a regex. `O(resources)`, identity preserved by
`CopyTo`. **Existing `tag_hash` configs are byte-for-byte unaffected** — this is
what the unchanged existing tests guard.

**Leaf path** descends to the record leaf, computes a key per leaf, and
reassembles leaves into per-key batches while preserving resource / scope /
(metric) identity. Traces and logs are the simple cases (leaf = span / log
record; no type switch; `metricName` is `""`). Metrics is the involved one:

```go
// metricBucket coalesces leaves under preserved identity, keyed by the leaf's
// ORIGIN index (i,j,k) so distinct origin metrics never accidentally merge and
// same-origin datapoints recombine into one metric.
type metricBucket struct {
	out    pmetric.Metrics
	rm     map[int]pmetric.ResourceMetrics
	sm     map[[2]int]pmetric.ScopeMetrics
	metric map[[3]int]pmetric.Metric
}

func groupMetricsByLeaf(md pmetric.Metrics, plan keyPlan) []taggedBatch[pmetric.Metrics] {
	buckets := map[string]*metricBucket{}
	var order []string
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		res := rm.Resource().Attributes()
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			ms := sm.Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				// One type switch computes the key per datapoint and appends it
				// into the dest metric's matching typed slice (Gauge / Sum /
				// Histogram / ExponentialHistogram / Summary — mirrors the
				// switches already in splitMetricsHalf and clampMetricDataPoints).
				eachMetricDataPoint(m, func(dpAttrs pcommon.Map, appendInto func(pmetric.Metric)) {
					key := partitionValue(plan, res, m.Name(), dpAttrs)
					b := buckets[key]
					if b == nil {
						b = newMetricBucket()
						buckets[key] = b
						order = append(order, key)
					}
					appendInto(b.ensureMetric(i, j, k, rm, sm, m))
				})
			}
		}
	}
	return assembleMetrics(order, buckets)
}
```

Supporting helpers (in `partition.go`):

- `ensureMetric(i, j, k, rm, sm, m)` find-or-creates the dest `ResourceMetrics(i)`
  / `ScopeMetrics(i,j)` / `Metric(i,j,k)` inside the bucket, copying resource,
  scope, schema URLs, and the **metric shell** (no datapoints).
- `copyMetricShell(src, dst)` copies `Name` / `Description` / `Unit` /
  `Metadata()` and the typed-empty body **with its settings** preserved:
  - `Sum`: `AggregationTemporality` + `IsMonotonic`
  - `Histogram` / `ExponentialHistogram`: `AggregationTemporality`
  - `Gauge` / `Summary`: none

  Getting this right is what the metric-shell-fidelity tests guard.
- `eachMetricDataPoint(m, fn)` is the single Gauge / Sum / Histogram /
  ExponentialHistogram / Summary switch; `appendInto` does
  `dp.CopyTo(dst.<Type>().DataPoints().AppendEmpty())`.
- `assembleMetrics(order, buckets)` emits `taggedBatch`es in first-seen key
  order.

**Invariants to preserve:**

- Determinism: datapoints append in input order; buckets emit in first-seen
  order.
- `Capabilities{MutatesData: false}` stays true — every write targets freshly
  allocated pdata; the input is only read and `CopyTo`'d.
- `splitHalf`, `truncateAttributes`, `itemCount`, the oversize policy chain, and
  `flush` are untouched: they operate on the already-grouped batch.

### Wiring

- `signalCodec.groupByTags` signature becomes `func(b T, plan keyPlan) []taggedBatch[T]`.
- `kinesisExporter` gains a `keyPlan keyPlan` field; `newExporter` calls
  `cfg.resolveKeyPlan()` so a bad plan surfaces at startup.
- `emit` passes `e.keyPlan` instead of building a `[]string` from `Tags`.
- Remove the now-unused `tagKey`; `partitionValue` subsumes it.
- `factory.go` default config is unchanged (`strategy: random`).

## Behavior and trade-offs

- **Record count.** A `datapoint` or `metric_name` source groups below the
  resource, so one input batch can fan into many records (one per distinct key
  tuple). This is intended — it is what spreads a producer across shards — but
  choose key dimensions coarse enough to keep records usefully sized. A whole
  metric name as a key element approaches one record per series; a coarse
  derived prefix (via `regex`) keeps the fan-out bounded.
- **Ordering.** Records sharing a tuple route to the same partition key and
  therefore the same shard, in order. As today, tag→shard locality is not stable
  across a reshard.
- **Payload.** Unchanged. The key is computed from the data; the data written to
  Kinesis is the same bytes regardless of partition configuration.

## Configuration examples

Partition by a per-record producer id plus a coarse metric-name prefix (the
common "fan one producer across shards" shape):

```yaml
exporters:
  awskinesis:
    stream_name: otel-metrics
    region: us-east-1
    encoding: otlp_proto
    compression: zstd
    partition_key:
      strategy: tag_hash
      keys:
        - { source: datapoint, name: device.id }
        - { source: metric_name, regex: "^([a-z]+)_" }
```

Resource-only, unchanged and still valid (shorthand and explicit forms are
equivalent):

```yaml
    partition_key: { strategy: tag_hash, tags: [service.name, region] }
    # identical to:
    partition_key:
      strategy: tag_hash
      keys:
        - { source: resource, name: service.name }
        - { source: resource, name: region }
```

## Testing

Extend `record_test.go` and add a focused `partition_test.go`:

- Existing `TestMetricsTagGrouping` / `TestTracesTagGrouping` /
  `TestLogsTagGrouping` (resource-only) must pass **unchanged** — proof the fast
  path is preserved.
- **`datapoint` source:** one `ResourceMetrics` holding datapoints with mixed
  `device.id` → N records; each record contains only its device's datapoints;
  key stable per device; decode round-trips; datapoint counts and values
  preserved.
- **`metric_name` + `regex`:** metrics `foo_a`, `foo_b`, `bar_c` with
  `^([a-z]+)_` → buckets `foo` / `bar`; a non-matching name → empty bucket;
  whole-match fallback when the pattern has no capture group.
- **Mixed, ordered plan** `[{resource: service.name}, {datapoint: device.id},
  {metric_name, regex}]` → segment order and separator correctness.
- **Metric-shell fidelity:** a monotonic cumulative `Sum` and a `Histogram`
  survive regrouping with temporality / monotonicity intact (decode and assert).
- **`Validate`:** both `tags` and `keys` set → error; unknown `source` → error;
  missing `name` → error; `name` set on `metric_name` → error; bad `regex` →
  error.

## Docs to update when implementing

- `exporter/awskinesisexporter/README.md`: document `keys` / sources / `regex`
  with the example above.
- `docs/user-guide.md`, "Tag-grouped microbatching": note the sub-resource
  sources, the record-count trade-off, and that the payload is unchanged.
- Add an ADR under `docs/adr/` recording the decision: derive in the exporter to
  keep the payload (and any downstream label mapping) byte-identical, rather than
  hoisting via processors; signal-agnostic config with a metrics-meaningful
  `metric_name`; single-capture-group limit to avoid turning the exporter into a
  transformation engine.

## Estimated size

~150–250 LoC for the metrics leaf path, ~40 each for traces/logs, ~60 for
config + resolve, plus tests and docs. One new file (`partition.go`); edits to
`config.go`, `signal.go`, `record.go`, `exporter.go`. No change to `factory.go`.

## Out of scope

- New compression/encoding codecs.
- Hash functions other than `xxhash`.
- Any partition-key derivation richer than a single regex capture group — that
  is a processor's job.
