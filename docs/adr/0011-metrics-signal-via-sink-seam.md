# 0011. Metrics signal via a single signal-agnostic seam

- **Status:** Accepted
- **Date:** 2026-06-13

## Context

Metrics is a primary use case (e.g. ingesting InfluxDB line protocol, which is
metrics in OTel). The components shipped traces-only. Adding metrics naively
would duplicate the exporter's encode/group/batch path and the receiver's
lease/coordinator/poller machinery per signal.

## Decision

Add metrics as a first-class signal by isolating the signal-specific work to a
single seam on each side; everything else stays signal-agnostic.

- **Encoding**: add `MetricsEncoder`/`MetricsDecoder` (OTLP proto via
  `pmetricotlp`) beside the traces pair. Codecs already operate on bytes.
- **Exporter**: one generic record-building core (group-by-tag → compress →
  oversize-repack → `PutRecords`) parameterized by a per-signal adapter (the
  four pdata operations: group, split-half, marshal, count). `ConsumeTraces`
  and `ConsumeMetrics` differ only by which adapter they pass. Both signals are
  registered with `WithTraces`/`WithMetrics`.
- **Receiver**: the lease store, coordinator, and pollers operate purely on raw
  bytes. The only signal-specific seam is a `sink` (decode a decompressed
  payload, deliver it, and wrap a dead-letter). A traces receiver and a metrics
  receiver are the same machinery with a different sink.

A given Kinesis stream carries one signal (the pipeline it serves); the wire is
headerless, so a receiver decodes records as the signal it is configured for. A
mixed-signal stream is out of scope.

## Consequences

- Metrics works everywhere traces does — codecs, tag-grouped microbatching,
  oversize repack, dead-letter — with no duplicated coordination code.
- The `sink` seam keeps the intricate lease/rebalancing/reshard logic untouched
  and unaware of signal type, so it could not regress when metrics landed.
- Operators run one stream per signal. Routing mixed signals on one stream
  would need a framing header, which the wire deliberately omits.
- Adding logs later is the same recipe: a `LogsEncoder/Decoder`, a logs adapter
  in the exporter, and a logs sink in the receiver.
