# 0016. Add OTLP JSON encoding; recommend compressed OTLP-proto

- **Status:** Accepted for the `otlp_json` addition. The Arrow follow-up
  landed in [ADR-0018](0018-implement-otel-arrow-encoding.md) and was
  later removed by [ADR-0020](0020-remove-otel-arrow-encoding.md); the
  forward-looking Arrow framing below is preserved as historical context.
- **Date:** 2026-06-13
- Supersedes the encoding cut of [0005](0005-poc-milestone-scope-cuts.md) and the
  "no usable Go module" rationale for OTel-Arrow in
  [0010](0010-codecs-and-deferred-arrow.md).

## Context

Two wire encodings were reserved names that failed validation: `otlp_json` and
`otel_arrow`. `otlp_json` is trivial to support via pdata's OTLP JSON marshalers
and is broadly useful (interop, debugging). For `otel_arrow`, ADR-0010's reason
for not shipping it — "its Go encoding library is not available as a usable
module" — is now stale: the library exists as a tagged module
(`github.com/open-telemetry/otel-arrow/go`, package `arrow_record`).

Separately, the realized advantage over the contrib Kinesis exporter is
**compression**: contrib does not compress at all, a real limiter against
Kinesis's per-record size cap, while this exporter offers `gzip`/`zstd`/`snappy`
over OTLP-proto (ADR-0010).

## Decision

- **Add `otlp_json`** as a fully supported encoding for traces and metrics. It
  is more verbose than proto, so pair it with a codec (e.g. `zstd`).
- **Recommend compressed OTLP-proto** (`encoding: otlp_proto` + `compression:
  zstd`) as the default adoption path and the contrib-gap closer.
- **`otel_arrow` is the next encoding to land.** Its blocker is resolved (the Go
  module now exists); it remains a reserved name that fails validation only
  until that work lands, and its design and trade-offs will be recorded in a
  follow-up ADR.

## Consequences

- Encodings available today: `otlp_proto` (default, compact, contrib-compatible)
  and `otlp_json` (interoperable/debuggable). `otel_arrow` is reserved and
  rejected fast until implemented.
- The encoder/decoder registry stays open and headerless — `otlp_json` slotted
  in without a wire-format break, and `otel_arrow` will too.
- **Design note for the upcoming Arrow work:** OTel-Arrow is a streaming format
  (schema + dictionary state across batches), while Kinesis here is
  store-and-forward (each record independent). The implementation must decide how
  a single record is made self-decodable — e.g. a fresh producer/consumer per
  record (simple, but re-ships schema/dictionaries and forgoes cross-record
  delta compression) versus batching multiple resources into one Arrow stream
  per record. That trade-off is the first thing the Arrow ADR should settle.
