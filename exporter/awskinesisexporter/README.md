# awskinesisexporter

OpenTelemetry Collector exporter for Amazon Kinesis Data Streams.

The exporter marshals telemetry under a configured encoding, compresses the
result under a configured codec, batches the payloads into Kinesis records up
to the stream's per-record size limit, and writes them with `PutRecords`.
Records are partitioned by a configurable strategy that determines per-shard
ordering on the consuming side.

Supported encodings are `otlp_proto` (default, recommended) and `otlp_json`.
Compression (`none`/`gzip`/`zstd`/`snappy`/`x-snappy-framed`/`zlib`/`deflate`)
is the key advantage over the contrib Kinesis exporter, which ships only
`flate`/`gzip`/`zlib`/`none` at a single level — `zstd` and `snappy` are the
codecs that actually pay off here. Compressed `otlp_proto` is the recommended
configuration.

**Status:** working proof of concept for traces, metrics, and logs, including
tag-grouped microbatching and oversize-record repacking.

## Observability

The exporter holds no logging or metrics configuration of its own; it logs
through the Collector-provided logger and emits instruments through the
Collector-provided `MeterProvider`. Verbosity, encoding, and routing are
controlled by the Collector's `service::telemetry` config, and the instruments
are exported wherever that config sends them (`level: none` disables them).

Instruments (scope `awskinesisexporter`):

- `kinesis.exporter.batch.records` (histogram) — records per `PutRecords` call.
- `kinesis.exporter.batch.bytes` (histogram) — aggregate payload bytes per call.
- `kinesis.exporter.flush.duration_ms` (histogram) — `PutRecords` latency.
- `kinesis.exporter.records_dropped` (counter, `reason` = `marshal_error` |
  `compress_error` | `max_attempts` | `irreducible` | `reject_policy` |
  `chain_exhausted` | `rejected`) — items dropped rather than retried forever.
  The reason names the failure mode so silent data loss stays observable; see
  the [user guide](../../docs/user-guide.md#oversize-records) for the policy
  semantics behind each label.
- `kinesis.exporter.attributes_truncated` (counter, unit `{attribute}`) —
  attribute values clamped by `truncate_attribute_values`, regardless of
  whether truncation alone fit the record. A non-zero sustained rate is the
  canary that something upstream is generating values longer than
  `oversize.max_attribute_value_bytes`.

Set the Collector log level to `debug` to log each `emit`, `put_records`, and
oversize-recovery decision (`encode attempt`, `oversize policy: …`).

## Oversize recovery

`oversize.policies` is an ordered chain of recovery strategies tried against
a payload that compressed larger than `max_record_size`:

- **`split_half`** (default) — recursively halve resources, then leaf items
  within a resource, until each piece fits or `oversize.max_attempts` is
  reached. Lossless when the bloat is in item count.
- **`truncate_attribute_values`** — clone the batch and clamp any string
  attribute value longer than `oversize.max_attribute_value_bytes`, backing
  off to a UTF-8 codepoint boundary so the output stays valid. Targets the
  "single long tag value" failure mode that `split_half` cannot recover.
  Lossy on string attributes only; every clamp increments
  `kinesis.exporter.attributes_truncated`.
- **`reject`** — stop here; drop the remainder and count as `reject_policy`.

Strategies are applied in order to the still-oversize remainder; the first
that fits wins. `split_half` and `reject` terminate the chain by
construction and validation enforces that they appear only as the last
entry. If every policy fails the items are dropped with the specific
terminal reason (`irreducible`, `max_attempts`, `reject_policy`) or
`chain_exhausted`. For high-cardinality attribute workloads,
`[truncate_attribute_values, split_half]` is the typical chain.

## Partition keys

`partition_key.strategy: random` (default) assigns a new UUID to every record
so records spread uniformly across shards. `partition_key.strategy: tag_hash`
derives a stable key from an ordered tuple of dimensions and groups the batch
so all telemetry sharing a tuple lands in one record on one shard.

### `tags` shorthand

The simplest form of `tag_hash` lists resource attribute keys in `tags`:

```yaml
partition_key:
  strategy: tag_hash
  tags: [service.name, region]
  hash: xxhash
```

Every entry in `tags` is a resource attribute. This is equivalent to the
explicit `keys` form below with `source: resource` on each entry. `tags` and
`keys` are mutually exclusive.

### `keys` — sub-resource dimensions

`keys` is an ordered list of `PartitionKeySource` entries. Each entry can pull
a value from a different level of the telemetry:

| `source`      | Reads from | `name` | Notes |
|---------------|------------|--------|-------|
| `resource`    | Resource attribute | required | Default when `source` is omitted. Groups at resource granularity, same as `tags`. |
| `datapoint`   | Record-leaf attribute — metric datapoint, span, or log-record attribute | required | Groups below the resource; one batch can fan into multiple records. |
| `metric_name` | The metric's name | must be empty | Contributes an empty segment for traces and logs. |

`regex` (optional): reduces the resolved value to the first capture group of
the first match, or the whole match when the pattern has no capture group. A
non-matching value contributes an empty segment (a catch-all bucket). Limited
to a single capture group — richer rewriting belongs in a processor.

`promote` (optional): when set, the resolved (post-regex) value is written as
an attribute on outgoing records under that name, **only if the attribute is
absent** (never overwrites). For `resource` sources the attribute is written at
the resource level; for `datapoint` and `metric_name` sources it is written at
the record leaf (datapoint, span, or log record). Without `promote` the OTLP
payload is byte-identical end to end — the key is derived from the data but
the data itself is not touched. With `promote` the payload is strictly
additive: only absent attributes are added.

#### Canonical example

Fan one high-volume producer across shards by `device.id`, further
coarse-grouped by a metric-name subsystem prefix. The `promote` on the second
key materialises the derived subsystem as a real attribute on each record so it
is available as a label downstream — without relocating or destroying any
existing data:

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
        - { source: metric_name, regex: "^([a-z]+)_", promote: subsystem }
```

The two forms below are equivalent:

```yaml
# shorthand
partition_key: { strategy: tag_hash, tags: [service.name, region] }

# identical explicit form
partition_key:
  strategy: tag_hash
  keys:
    - { source: resource, name: service.name }
    - { source: resource, name: region }
```

#### Record fan-out and sizing

A `datapoint` or `metric_name` source groups below the resource, so one input
batch can become many records — one per distinct key tuple. This is intentional
(it spreads a producer across shards), but choose dimensions coarse enough to
keep records usefully sized. A `regex`-derived prefix, rather than the raw
metric name, bounds fan-out.

Records sharing a key tuple route to the same partition key and therefore to
the same shard, in order. Tag→shard locality is not stable across a reshard:
Kinesis maps partition keys onto hash-key ranges, and a split or merge changes
those ranges.

#### Why not a processor?

The conventional alternative — running `groupbyattrs` + `transform` processors
to hoist datapoint attributes to the resource level before the exporter groups
by them — has two problems: (a) those processors may not be available in a
minimal collector build, and (b) they mutate the OTLP on the wire, which can
break downstream label mapping (for example, promoting a per-series label
turns it into a Prometheus `target_info` attribute, silently breaking queries
that filter on it). Deriving the key inside the exporter avoids both: no extra
processors, and the payload is unchanged.
