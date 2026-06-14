# 0005. PoC milestone scope cuts

- **Status:** Accepted. Most cuts are now delivered: zstd/snappy ([0010]
  (0010-codecs-and-deferred-arrow.md)), metrics signal ([0011]
  (0011-metrics-signal-via-sink-seam.md)), tag-hash partition keys /
  microbatching / oversize repack ([0012](0012-tag-grouping-and-oversize-repack.md)),
  dead-letter ([0013](0013-dead-letter-via-pipeline-reemit.md)), `otlp_json`
  ([0016](0016-add-otlp-json-encoding.md)), `otel_arrow` shipped
  ([0018](0018-implement-otel-arrow-encoding.md)) then removed after
  benchmarking ([0020](0020-remove-otel-arrow-encoding.md)), and the logs
  signal (wired
  on the same signal-agnostic seam as metrics, see [0011]
  (0011-metrics-signal-via-sink-seam.md)). Still open: live-AWS reshard
  verification, and EFO.
- **Date:** 2026-06-13

## Context

The first working milestone is an end-to-end round trip: OTLP traces in
through an exporter, across a Kinesis stream, out through a receiver, with
shard ownership coordinated across replicas. Getting that path to work — and
proving it against an AWS-compatible emulator — is worth more at this stage
than breadth of features. Several capabilities the architecture calls for are
deliberately deferred so the round trip lands first.

These cuts are recorded here so the absence of each is visible as a decision,
not mistaken for an oversight by the next contributor.

## Decision

The first milestone ships the following cuts. Each is independently
re-openable.

- **Encoding: OTLP proto only.** No OTLP JSON, no OTel-Arrow. The encoder
  registry exists; only one encoder is wired.
- **Compression: `none` and `gzip` only.** No zstd. The codec registry
  exists; zstd returns a "not implemented" error at config-validation time.
- **Partition key: random (UUID) only.** No tag-hash strategy. The exporter
  writes a fresh UUID per record.
- **Traces signal only.** Metrics and logs are not wired through either
  component. (Both signals were subsequently added: metrics on the
  signal-agnostic seam from [0011](0011-metrics-signal-via-sink-seam.md),
  logs on the same seam.)
- **Microbatching deferred.** The exporter encodes one `ConsumeTraces` call
  into one Kinesis record and relies on the upstream `batchprocessor` for
  batching. `PutRecords` (not `PutRecord`) is used so in-exporter batching is
  a body change later, not a call-site change.
- **No oversize-record repacking.** A post-compression payload over the
  configured limit is logged and dropped, not split. The repack policies in
  the architecture land when record sizes actually approach the limit.
- **Failure policy: log-and-continue, hard-coded.** Decode, decompress, and
  downstream-consume failures are logged and skipped. No per-failure-class
  configuration, no dead-letter wrapping. The receiver advances its
  checkpoint past skipped records so a takeover does not replay a poison
  pill forever.
- **No EFO.** Polling (`GetRecords`) only. `SubscribeToShard` is out of scope.

## Consequences

- The round trip is demonstrable end-to-end with a small, reviewable surface.
- Each cut is a known gap with a clear re-entry point, not hidden technical
  debt. The encoder/codec registries and the `PutRecords` batch shape are the
  seams the deferred features slot into.
- Wire compatibility is preserved at the one encoding/codec pair that is
  wired; adding encoders or codecs is additive and does not break deployed
  producers or consumers.
- Operators cannot yet tune partitioning, batching, or failure handling. The
  defaults are PoC-grade and must not be read as production recommendations.
- Revisit each cut on its own merits; none blocks the others.
