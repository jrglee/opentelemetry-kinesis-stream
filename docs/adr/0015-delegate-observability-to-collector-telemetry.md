# 0015. Delegate observability to the collector's built-in telemetry

- **Status:** Accepted
- **Date:** 2026-06-13

## Context

For external user testing, the components need richer self-observability: debug
logging that shows normal operation (poll cycles, checkpoint advances, lease
transitions), and internal performance metrics (batch sizes, throughput,
latency, rebalance/lease activity). Collectors frequently run in ECS, where
operators expect these to land in CloudWatch.

The tempting path was a component config block — `enabled`, a metric
`namespace`, custom `tags` — emitting metrics through a self-managed
MeterProvider. Investigation of the collector core (v0.154.0 /
`service/telemetry/otelconftelemetry`) showed that block would re-implement
infrastructure the collector already owns:

- Instruments created from `TelemetrySettings.MeterProvider` are exported
  automatically through whatever `service::telemetry::metrics::readers` is
  configured with (Prometheus pull, OTLP periodic push). No `mdatagen` required.
- `service::telemetry::metrics::level: none` hands components a **noop**
  MeterProvider, so an `enabled` flag is redundant.
- The CloudWatch namespace is the downstream `awsemfexporter`'s `namespace:`
  config — a layer below this component, not its concern.
- Static tags are `service::telemetry::resource` (global) plus per-instrument
  attributes set at record time.
- The same holds for logs: the provided `*zap.Logger` is built from
  `service::telemetry::logs` (level, encoding, output paths, sampling, OTLP
  shipping). This matches the contrib pattern of using `params.Logger`.

## Decision

Delegate all observability infrastructure to the collector's built-in
telemetry. The components build no logging or metrics plumbing of their own.

- **Metrics**: create plain `otel/metric` instruments from the collector-provided
  `MeterProvider` (`exporter/awskinesisexporter/telemetry.go`,
  `receiver/awskinesisreceiver/telemetry.go`). No component config, no namespace
  prefixing, no enable flag. Instruments are low-cardinality by policy — no
  per-shard or per-sequence attributes (that detail lives in debug logs); the
  only attributes are bounded enums (`lease.events` `event`/`result`).
- **Logs**: use only the provided `*zap.Logger`; never construct a logger.
  Level, encoding, routing, and sampling are the operator's via
  `service::telemetry::logs`.
- Fixing the one real gap: the receiver factory previously passed only
  `set.Logger`, so the `MeterProvider` never reached the component. It now
  passes the full `receiver.Settings`.

This self-telemetry is distinct from the data-plane metrics *signal* (the
ConsumeMetrics path, see [0011](0011-metrics-signal-via-sink-seam.md)): one is
the component reporting on itself, the other is user telemetry flowing through.

## Consequences

- Operators control everything with standard collector config they already
  know; routing internal metrics to CloudWatch is the ordinary
  `awsemfexporter` + `service::telemetry::metrics::readers` setup, and logs reach
  CloudWatch Logs via the ECS `awslogs` driver out of the box. Documented in the
  user guide.
- The component owns no config surface for observability, so there is nothing to
  validate, default, or version — and no risk of it drifting from the collector's
  own conventions.
- The trade-off: control is collector-global, not per-component. If a future
  instrument is noisy or high-cardinality enough to want independent gating, that
  single knob would have to be added back as component config. None of the
  current instruments warrant it.
- Manual instruments (no `mdatagen`) keep the build toolchain unchanged and match
  the pre-existing `records_dropped` counter.
