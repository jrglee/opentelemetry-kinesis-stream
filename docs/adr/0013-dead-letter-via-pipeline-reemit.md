# 0013. Dead-letter via pipeline re-emit; exporter observability

- **Status:** Accepted
- **Date:** 2026-06-13

## Context

Unprocessable records must not vanish silently. The design (DESIGN.md §6) calls
for the receiver to wrap a failed raw record and emit it into the Collector's
own pipeline as telemetry, where operators route it with standard components,
rather than the component implementing a bespoke dead-letter sink.

## Decision

- **Receiver** (`dead_letter.enabled`): when a record cannot be decompressed or
  decoded, wrap it and re-emit it into the receiver's own pipeline. Because the
  receiver is single-signal, the wrapper matches the signal: a span named
  `kinesis.dead_letter` for a traces receiver, a gauge metric of the same name
  for a metrics receiver. The wrapper carries the raw (still-compressed) bytes
  plus shard id, sequence number, partition key, failure class, encoding, and
  codec as attributes. The record's checkpoint still advances — the bytes are
  unprocessable, so re-reading would not help. A *transient* downstream
  rejection of a successfully decoded record is NOT dead-lettered; it is retried
  (the checkpoint does not advance) so valid telemetry is never lost.
- **Exporter**: there is no downstream pipeline to emit into, so failures
  (a single oversize leaf that cannot be repacked) are surfaced as a
  `kinesis.exporter.records_dropped` counter plus a structured warn log — no
  bespoke sink.

## Consequences

- Failed records become first-class telemetry the operator routes with
  filter/routing components to S3, another stream, or a log store — the full
  expressive power of the Collector, with no sink code in the component.
- A dead-letter wrapper is observable but lossy-by-design about the original
  structure (it carries raw bytes, not decoded fields) — the point is to make
  the failure investigable, not to recover the payload automatically.
- Putting the raw bytes in an attribute means a dead-letter record is larger
  than the original; high failure rates are themselves a signal worth alerting
  on. Dead-lettering is off by default.
- The transient-vs-permanent distinction (only permanent/undecodable failures
  are dead-lettered; transient ones retry) keeps the feature from masking
  backpressure as data loss.
