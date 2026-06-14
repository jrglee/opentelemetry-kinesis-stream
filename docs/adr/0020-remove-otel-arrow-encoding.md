# 0020. Remove `otel_arrow` encoding from the PoC

- **Status:** Accepted (supersedes [ADR-0018])
- **Date:** 2026-06-13

## Context

[ADR-0018] implemented `otel_arrow` as a per-record self-contained Arrow batch
— a fresh `arrow_record.Producer` per Marshal, a fresh `Consumer` per
Unmarshal — so that each Kinesis record could decode in isolation despite
OTAP being designed as a stateful gRPC stream. The encoding shipped, the
benchmark harness in [`benchmark.md`](../../benchmark.md) captured the
numbers, and the matrix E2E proved correctness across every codec.

In use the design's weak points became sharp:

- **The compression model is the wrong shape for this transport.** OTAP's
  ~40% wire-size advantage over OTLP+zstd comes from cross-batch dictionary
  deltas streamed over a long-lived gRPC connection — the spec
  ([`otap-spec.md`](https://github.com/open-telemetry/otel-arrow/blob/main/docs/otap-spec.md))
  is explicit that dictionaries are cumulative and the receiver retains
  per-stream state. Kinesis records are independently consumed, may arrive
  out-of-order across shards, and survive consumer restarts. Forcing each
  record to be a self-contained IPC payload pays Arrow's schema overhead on
  every record and forfeits the dictionary delta that motivated OTAP in the
  first place. The benchmark confirms it: `otel_arrow` does not beat
  compressed `otlp_proto` on this transport at the batch sizes a Kinesis
  record allows.

- **Liability without a payoff.** The upstream `arrow_record.Producer`
  panics on inputs it cannot encode (most notably "Too many consecutive
  schema updates" on high-cardinality attribute streams). [ADR-0018] added
  a `recover` shim and an `ErrArrowPanic` route. That guard is correct but
  it is also a live reminder that the encoder reaches into a code path the
  PoC cannot reason about end-to-end. Removing the encoding removes the
  guard, the dependency surface (`github.com/open-telemetry/otel-arrow/go`
  plus the Apache Arrow Go runtime), and the matrix cells that exist only
  to keep the encoder honest.

- **The schema design surprises operators.** Field intuition expects
  attributes to be a map type because observability tag sets are not known
  upfront. OTAP's actual design is neither map-per-record nor
  column-per-key — it's a separate, normalized attribute RecordBatch joined
  to the main batch by `parent_id`, with key and value dictionary encoding.
  That design is clever and correct for its intended transport, but it
  surfaces as "the schema keeps changing" on Kinesis because dictionary
  index widths adapt batch-to-batch and the IPC framing isn't byte-stable
  even though the *logical* schema is. Explaining that on the PoC's status
  page is a tax we're paying for a feature that doesn't earn its keep.

The proper home for an OTAP integration is a separate gRPC
exporter/receiver pair (or the upstream `otelarrowexporter` /
`otelarrowreceiver` components) where the stream model is intact.

## Decision

Remove the `otel_arrow` encoding and its supporting code:

- Delete `internal/encoding/otlparrow.go` and the `EncodingOTelArrow`
  registry entry.
- Drop the `github.com/open-telemetry/otel-arrow/go` and
  `github.com/apache/arrow-go/v18` dependencies via `go mod tidy`.
- Remove the matrix-E2E and unit-test cases that pinned Arrow's
  self-contained-IPC invariant and panic-recovery behavior.
- Strip Arrow from the perf harness (encodings list, `safeMarshal`
  panic-guard wrappers).

Keep the captured benchmark numbers in [`benchmark.md`](../../benchmark.md)
and the prior ADRs ([ADR-0010], [ADR-0018]) as the historical record of
what was tried and measured.

## Consequences

**Makes easier:**
- Smaller dependency footprint and a smaller attack surface. No Apache
  Arrow Go runtime, no OTAP module.
- One fewer encoding to validate every time a wire-adjacent change lands.
- Status banners and config tables shorten to two encodings, both of which
  earn their slot on this transport.

**Makes harder:**
- Anyone genuinely wanting Arrow on a Kinesis hop now has to bring it
  themselves: terminate the OTAP stream in a separate collector pair
  (`otelarrowexporter` → `otelarrowreceiver`), then re-emit OTLP into this
  exporter. The PoC no longer offers it directly.

**Revisit when:**
- A future revision of OTAP supports a stateless-record mode where
  dictionaries are inlined per record without forfeiting the compression
  advantage — at that point an Arrow encoding could earn its place back.
- A separate gRPC-based exporter/receiver pair is on the roadmap; the
  removed Arrow code is recoverable from git history as a starting point.

[ADR-0010]: 0010-codecs-and-deferred-arrow.md
[ADR-0016]: 0016-add-otlp-json-encoding.md
[ADR-0018]: 0018-implement-otel-arrow-encoding.md
