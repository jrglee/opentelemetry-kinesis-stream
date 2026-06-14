# opentelemetry-kinesis-stream

A paired OpenTelemetry Collector **exporter** and **receiver** for Amazon
Kinesis Data Streams: send OTLP telemetry into a stream on one side, read it
back out on the other, with shard ownership coordinated across receiver
replicas.

New here and want to *run* it? Start with the **[user guide](docs/user-guide.md)**.
This README is about *why the project exists* and *how it is built* — the
motivations and architectural choices, for evaluators and contributors.

**Status:** working proof of concept. All three signals (traces, metrics, and
logs) flow end-to-end with `otlp_proto`/`otlp_json` encodings and
the full collector codec set
(`none`/`gzip`/`zstd`/`snappy`/`x-snappy-framed`/`zlib`/`deflate`), and shard
ownership is coordinated and rebalanced across replicas via a KCL-shaped
DynamoDB lease table. Remaining gaps (real-AWS reshard verification, EFO) are
tracked in [ADR-0005](docs/adr/0005-poc-milestone-scope-cuts.md).

## Why this exists

The `opentelemetry-collector-contrib` project ships an `awskinesisexporter`
(Beta — traces, metrics, logs; encodings
`otlp_proto`/`otlp_json`/`jaeger_proto`/`zipkin_proto`/`zipkin_json`;
compressions `flate`/`gzip`/`zlib`/`none` at a single level; random-UUID
partition key per record) but **no Kinesis Data Streams receiver** — there is
no first-party way to consume OTLP telemetry back off a stream. Closing that
gap is this project's reason to exist.

Along the way it addresses three gaps the contrib exporter leaves in place.
**Compression breadth**: this exporter exposes the Collector's full
`configcompression` set
(`none`/`gzip`/`zstd`/`snappy`/`x-snappy-framed`/`zlib`/`deflate` — only
`lz4` omitted) with pooled `zstd` as the recommended default. The codec
choice is independent of the encoding choice
(`otlp_proto`/`otlp_json`) — compression runs on the marshaled
bytes either way. **Partition keys and microbatching**: contrib writes one
record per `ConsumeXxx` call with a random UUID key; this exporter adds a
stable `tag_hash` strategy and resource-tuple grouping so related telemetry
co-locates on the same shard in order. **Record size**: `max_record_size` is
an operator knob (raise to Kinesis's [10 MiB opt-in
ceiling](https://aws.amazon.com/blogs/big-data/amazon-kinesis-data-streams-now-supports-10x-larger-record-sizes-simplifying-real-time-data-processing)),
backed by an oversize-recovery chain (`split_half`,
`truncate_attribute_values`, `reject`) so an oversize microbatch is repacked
rather than dropped. Default stays at the conservative 1 MiB.

The hard part is not writing to or reading from Kinesis — it is consuming a
sharded, resharding stream correctly across multiple collector replicas without
dropping, duplicating, or stalling telemetry. Most of the architecture below is
about that.

## Architectural choices

Each decision below is recorded as an ADR; follow the link for the full context
and consequences. The overall architecture lives in [`DESIGN.md`](DESIGN.md).

- **Shard coordination modeled on the KCL**, via a lease store rather than a
  bespoke scheme. A DynamoDB-backed, KCL-shaped lease table makes the behavior
  familiar and the fencing semantics auditable.
  [ADR-0006](docs/adr/0006-shard-lease-coordination.md),
  [ADR-0014](docs/adr/0014-model-shard-coordination-on-kcl.md);
  deep dive in [`docs/kcl-lite.md`](docs/kcl-lite.md).
- **Leaderless fair-share rebalancing.** Every replica runs the same fair-share
  computation against the same lease-table snapshot, so they converge to an even
  split with no elected coordinator.
  [ADR-0008](docs/adr/0008-leaderless-fair-share-rebalancing.md).
- **Graceful lease handoff.** On rebalance or shutdown a replica drains its
  in-flight batch and checkpoints before releasing, so the next owner resumes
  cleanly instead of re-reading.
  [ADR-0009](docs/adr/0009-graceful-lease-handoff-and-shutdown.md).
- **A single signal-agnostic seam.** Traces, metrics, and logs share one
  group/compress/PutRecords path and one poll/decode path; only the pdata
  marshaling differs per signal.
  [ADR-0011](docs/adr/0011-metrics-signal-via-sink-seam.md).
- **Pluggable wire encoding and compression.** Encoding (`otlp_proto`,
  `otlp_json`) and codec
  (`none`/`gzip`/`zstd`/`snappy`/`x-snappy-framed`/`zlib`/`deflate` — the
  collector's set minus `lz4`) are config, with a headerless wire contract that
  lets exporter and receiver agree. Compressed OTLP-proto is the recommended
  path and the contrib-gap closer. `otel_arrow` was prototyped, benchmarked,
  and then removed: OTAP's compression win depends on a stateful gRPC stream
  with cross-batch dictionary deltas, which the Kinesis store-and-forward model
  forfeits — the per-record self-contained variant carries the schema overhead
  without the payoff. The benchmark numbers and rationale live in
  [`benchmark.md`](benchmark.md) and
  [ADR-0020](docs/adr/0020-remove-otel-arrow-encoding.md).
  [ADR-0010](docs/adr/0010-codecs-and-deferred-arrow.md),
  [ADR-0016](docs/adr/0016-add-otlp-json-encoding.md),
  [ADR-0018](docs/adr/0018-implement-otel-arrow-encoding.md).
- **Deterministic partition keys and tag-grouped microbatching**, so records
  that should stay ordered share a shard and related records ride one record.
  [ADR-0012](docs/adr/0012-tag-grouping-and-oversize-repack.md).
- **Dead-letter via pipeline re-emit.** Unprocessable records are re-emitted as
  telemetry the operator routes with standard components — no bespoke sink.
  [ADR-0013](docs/adr/0013-dead-letter-via-pipeline-reemit.md).
- **Observability delegated to the collector.** No custom logging or metrics
  config: the components log through the collector's logger and emit instruments
  through its MeterProvider, so `service::telemetry` governs everything and
  CloudWatch is the ordinary `awsemfexporter` / `awslogs` setup.
  [ADR-0015](docs/adr/0015-delegate-observability-to-collector-telemetry.md).

## Documentation

- **[`docs/user-guide.md`](docs/user-guide.md)** — getting started: quickstart,
  configuration reference, observability, and a real-AWS walkthrough. Start here
  to use the components.
- **[`DEVELOPMENT.md`](DEVELOPMENT.md)** — local development: toolchain, Makefile
  targets, the docker-compose stack, and running the E2E test.
- **[`docs/troubleshooting.md`](docs/troubleshooting.md)** — symptom → cause →
  fix, keyed off the log lines and metrics the components emit.
- [`docs/kcl-lite.md`](docs/kcl-lite.md) — the shard-coordination design in depth.
- [`DESIGN.md`](DESIGN.md) — overall architecture.
- [`docs/adr/`](docs/adr/) — the decision log.

## License

Apache-2.0. See [`LICENSE`](LICENSE).
