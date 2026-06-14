# 0010. zstd/snappy codecs; OTel-Arrow deferred

- **Status:** Accepted. The zstd/snappy codec decision stands; the OTel-Arrow
  "no usable Go module" rationale is superseded by
  [ADR-0016](0016-add-otlp-json-encoding.md) — the module now exists, and Arrow
  is the next encoding to land. Arrow deferral resolved by
  [ADR-0018](0018-implement-otel-arrow-encoding.md): `otel_arrow` is now
  implemented as a self-contained per-record batch.
- **Date:** 2026-06-13

## Context

The wire contract allows a codec choice. The PoC shipped `none` and `gzip`;
`zstd` and OTel-Arrow were named but unimplemented. Operators want zstd for its
ratio/speed, and Snappy for cheap low-latency compression. OTel-Arrow was
listed as the motivating new *encoding*.

## Decision

Add `zstd` and `snappy` compression codecs. Use `github.com/klauspost/compress`
for both: zstd via a shared, concurrency-safe pooled encoder/decoder (building
them is expensive, the API is allocation-light), Snappy via its stateless block
format. Codecs are signal-agnostic — they compress bytes, so they apply equally
to traces and metrics.

**Defer OTel-Arrow.** Its Go encoding library is not available as a usable
module: at v0.47.0 the `github.com/open-telemetry/otel-arrow` repository ships
no `go.mod` and no `arrow_record` package in the tree — the Go encoder was
relocated into `opentelemetry-collector-contrib`'s internal packages, which are
not a clean public dependency. The `otel_arrow` encoding name stays a
validation error. Effort was redirected to the metrics signal, which is a
higher-priority real-world use case.

## Consequences

- Four codecs (`none`, `gzip`, `zstd`, `snappy`) cover the practical range from
  no-cost to high-ratio. Round-trip and concurrent-stress tests cover all four.
- zstd's shared pooled instances are the one real concurrency risk; the
  concurrent test exercises exactly that path under `-race`.
- OTel-Arrow remains a future addition. Picking it up later means either
  vendoring contrib's internal Arrow packages or waiting for a published Go
  module — a deliberate, revisitable choice, not an oversight.
- The wire stays headerless: encoding and codec are agreed by configuration on
  both ends, so adding codecs does not break deployed producers or consumers.
