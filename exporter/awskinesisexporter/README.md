# awskinesisexporter

OpenTelemetry Collector exporter for Amazon Kinesis Data Streams.

The exporter marshals telemetry under a configured encoding, compresses the
result under a configured codec, batches the payloads into Kinesis records up
to the stream's per-record size limit, and writes them with `PutRecords`.
Records are partitioned by a configurable strategy that determines per-shard
ordering on the consuming side.

**Status:** scaffolding stub. The factory registers and produces a no-op
exporter; encoding, compression, partitioning, batching, and the `PutRecords`
path are not implemented yet.
