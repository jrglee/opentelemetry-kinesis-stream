# awskinesisexporter

OpenTelemetry Collector exporter for Amazon Kinesis Data Streams.

The exporter marshals telemetry under a configured encoding, compresses the
result under a configured codec, batches the payloads into Kinesis records up
to the stream's per-record size limit, and writes them with `PutRecords`.
Records are partitioned by a configurable strategy that determines per-shard
ordering on the consuming side.

Supported encodings are `otlp_proto` (default, recommended), `otlp_json`, and
`otel_arrow`. Arrow records are self-contained per Kinesis record (fresh
producer per record) so the receiver can decode any single record in
isolation; the per-record schema overhead is paid every time. Compression
(`none`/`gzip`/`zstd`/`snappy`/`x-snappy-framed`/`zlib`/`deflate`) is the key
advantage over the contrib Kinesis exporter, which ships only
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
