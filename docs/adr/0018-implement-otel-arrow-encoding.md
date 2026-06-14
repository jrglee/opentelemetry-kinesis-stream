# 0018. Implement `otel_arrow` encoding with per-record self-contained batches

- **Status:** Accepted
- **Date:** 2026-06-13

## Context

`otel_arrow` had been a reserved-but-rejected encoding since the initial
codec ADR. [ADR-0010] deferred it on the grounds that no usable Go module
existed. That blocker is stale: `github.com/open-telemetry/otel-arrow/go`
(package `arrow_record`) is now published with a `Producer` /
`Consumer` API that returns a self-contained `BatchArrowRecords` protobuf
message per call. [ADR-0016] recorded that shift and explicitly framed
Arrow as the next encoding to land, deferring the implementation design
to this ADR.

The unresolved design question was how a streaming format fits a
store-and-forward transport. OTel-Arrow is, by construction, a stream:
the Producer accumulates schema and dictionary state across batches and
emits deltas, so a later batch is only decodable in the context of the
prior batches from the same Producer. Kinesis is the opposite shape —
each record is an independent unit, delivery is at-least-once, the
receiver dead-letters individual records, and shard ownership can hand
off mid-flight. Any state that flows from one record to the next is a
silent corruption surface: a dropped or reordered record breaks every
record after it on the shard, and a takeover by a different consumer
loses the producer-side dictionary forever.

Three shapes were considered:

1. **Fresh producer per record.** Every `Marshal` call constructs a new
   `arrow_record.Producer`, every `Unmarshal` call constructs a new
   `arrow_record.Consumer`. Each Kinesis record carries a fully
   self-decodable Arrow batch with its own schema and dictionaries.
   Simple, lossless under at-least-once delivery, no cross-record
   coupling. The per-record schema overhead is paid every time.

2. **Sticky producer per partition key.** Producer state persists across
   records targeting the same shard. Best compression — schemas amortize
   across records, dictionaries grow. Fundamentally incompatible with
   our delivery model: dead-letter, retry, and takeover all break the
   per-record sequence Arrow's streaming format assumes.

3. **Multi-pdata-batch per record.** Pack several `pmetric.Metrics` (or
   `ptrace.Traces`) into one Arrow stream inside a single Kinesis record.
   Amortizes the schema cost within a record but adds queueing /
   flush semantics at the exporter and changes the failure unit
   from one batch to many.

Codec stacking remains uniform across encodings: the configured `Codec`
(none/gzip/zstd/...) wraps the Arrow bytes the same way it wraps OTLP
proto/json. Operators who want to avoid double-compression set
`compression: none`; Arrow's IPC compression is not a separate
top-level knob.

## Decision

Implement `otel_arrow` with a fresh `Producer` per `Marshal` and a fresh
`Consumer` per `Unmarshal`. Every Kinesis record is fully self-decodable.
The codec layer wraps Arrow bytes uniformly.

We forfeit Arrow's cross-batch dictionary-delta compression as the
explicit price of compatibility with Kinesis's per-record delivery
guarantees. Compressed OTLP-proto remains the recommended path for
size-sensitive workloads. `otel_arrow` is for operators who already
standardize on Arrow at their collector edges, or who need its
richer wire-level type system. Per-record self-containment is the
load-bearing invariant: it is pinned by `TestArrowSelfContained` and by
the in-process correctness matrix `TestEncodingCodecMatrix`, which
drives every (encoding × codec) combination through the real
coordinator and shard pollers.

The `perf/` benchmark harness (Go benchmarks under `//go:build perf`,
deterministic seed-driven datasets across high-cardinality,
high-frequency, and balanced metrics profiles plus a typical-trace
profile, across batch sizes from 1 to 10000) makes the trade-off
visible: `compressed_bytes` and `compression_ratio` are stable across
machine architectures, encode/decode timings are reported per-host. The
Arrow decode-superiority claim from upstream is either confirmed or
falsified per profile by reading the numbers, not by faith.

## Consequences

What this makes easier:
- Three encoding options on the same uniform codec layer with one
  decision rule (proto for size, json for interop, arrow for Arrow-edge
  fleets).
- `otel_arrow` interop with any consumer that decodes a self-contained
  `BatchArrowRecords` protobuf, which is what `arrow_record.Consumer`
  does by construction.
- Reproducible performance numbers across hosts and CI, since dataset
  bytes are byte-identical given a fixed `PerfSeed`. Engineers can
  argue from data rather than from claims.

What this makes harder:
- Arrow records re-ship their schema every time. At small batch sizes
  this dominates the on-wire size; the perf harness shows this clearly.
  Operators choosing Arrow on this transport should batch larger.
- The transitive dependency footprint grows substantially —
  `arrow_record` brings Apache Arrow Go in, plus a handful of
  observability-leaning libraries. The license list was inspected at
  the chosen pinned tag.
- The upstream API path is
  `github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1`,
  which advertises its non-stability in the path itself. The module is
  pre-1.0 and has shipped breaking changes between minors (package paths
  moved when the module split into `/go`). We pin to a specific tag;
  bumps require a fresh round-trip test and we make no wire-compat
  promise against future upstream changes.
- `arrow_record.Producer` and `Consumer` can `panic` on inputs they cannot
  encode/decode (most notably "Too many consecutive schema updates" at
  extreme cardinality). The encoder and decoder recover those panics and
  surface `encoding.ErrArrowPanic`; the exporter drop-and-count path and
  the receiver dead-letter path then route the bad record without
  crashing the collector. Tested by `TestArrowMetricsPanicRecovered`.

What this commits us to revisit:
- If a future Kinesis-shaped streaming primitive ships (durable
  shard-affine producers with consumer-side schema recovery), we should
  re-evaluate sticky-producer-per-shard. The current decision is
  bounded by the per-record store-and-forward model.
- If the `arrow_record` API stabilizes a "self-contained batch" mode
  with reduced per-record schema overhead, we should adopt it directly.

[ADR-0010]: 0010-codecs-and-deferred-arrow.md
[ADR-0016]: 0016-add-otlp-json-encoding.md
