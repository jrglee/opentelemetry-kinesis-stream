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
advantage over the contrib Kinesis exporter, which does not compress —
compressed `otlp_proto` is the recommended configuration.

**Status:** working proof of concept for traces and metrics, including
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
- `kinesis.exporter.records_dropped` (counter, `reason` = `oversize` |
  `rejected`) — items dropped rather than retried forever.

Set the Collector log level to `debug` to log each `emit` and `put_records`.
