# opentelemetry-kinesis-stream

A paired OpenTelemetry Collector **exporter** and **receiver** for Amazon
Kinesis Data Streams: send OTLP telemetry into a stream on one side, read it
back out on the other, with shard ownership coordinated across receiver
replicas.

New here and want to *run* it? Start with the **[user guide](docs/user-guide.md)**.
This README is about *why the project exists* and *how it is built* — the
motivations and architectural choices, for evaluators and contributors.

**Status:** working proof of concept. Traces and metrics flow end-to-end, and
shard ownership is coordinated and rebalanced across replicas via a KCL-shaped
DynamoDB lease table. Several capabilities are deliberately deferred — see
[ADR-0005](docs/adr/0005-poc-milestone-scope-cuts.md).

## Why this exists

The `opentelemetry-collector-contrib` project ships a Kinesis **exporter** but
no Kinesis Data Streams **receiver** — there is no first-party way to consume
OTLP telemetry back off a stream. This project closes that gap, and along the
way lifts ceilings the contrib exporter leaves in place (5 MiB records,
additional codecs, deterministic partition keys).

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
- **A single signal-agnostic seam.** Traces and metrics share one
  group/compress/PutRecords path and one poll/decode path; only the pdata
  marshaling differs per signal.
  [ADR-0011](docs/adr/0011-metrics-signal-via-sink-seam.md).
- **Pluggable wire encoding and compression.** Encoding and codec are config,
  with a wire contract that lets exporter and receiver agree.
  [ADR-0010](docs/adr/0010-codecs-and-deferred-arrow.md).
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
