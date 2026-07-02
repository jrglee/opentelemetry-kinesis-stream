# awskinesisreceiver

OpenTelemetry Collector receiver for Amazon Kinesis Data Streams.

The receiver claims shards through a lease store (an in-memory store for
single-replica development, or a DynamoDB store for multi-replica
deployments), polls each owned shard, decompresses and decodes records under
a configured codec and encoding, hands the resulting telemetry to the
downstream pipeline, and checkpoints the shard's read position only after
downstream acceptance.

## Ownership and failure handling

- Each replica heartbeats its leases and, on every reconcile pass, computes its
  fair share (`ceil(activeShards / activeWorkers)`) from the lease-table
  snapshot. A replica over its share releases surplus shards; a replica under
  its share acquires unowned or stale shards, and failing that steals one shard
  per pass from the most-overloaded peer. This converges to an even split as
  replicas join and leave, with no leader. Acquisition and stealing are fenced
  by a per-lease counter, so two replicas never make progress on the same shard
  concurrently. Stealing a healthy lease is graceful but at-least-once around
  the handoff (the new owner resumes from the last checkpoint).
- The checkpoint advances only over records that were delivered downstream or
  are permanently unprocessable (a decode/decompress failure, or a downstream
  rejection marked permanent). A record the downstream **transiently** rejects
  (backpressure, restart) is re-read rather than skipped, so valid telemetry
  is not dropped under load.
- A failed or expired shard iterator is re-opened from the persisted
  checkpoint rather than reused, so transient `GetRecords` errors recover
  instead of spinning. Polling is paced to one `GetRecords` per
  `poll_interval` per shard because the API allows five reads per second per
  shard — an unpaced loop on a busy shard would throttle itself and starve
  any co-consumer of the stream's shared read quota.
- A transient lease-store error (a DynamoDB throttle or network blip) is not
  lease loss: heartbeats retry on the next tick and checkpoints retry in
  place, because tearing pollers down on a shared blip would abandon every
  in-flight batch at once and turn one outage into a fleet-wide redelivery
  storm. Only a lease conflict — another replica provably owns the shard —
  stops a poller.
- With `dead_letter.enabled`, the checkpoint advances past an unprocessable
  record only once its dead-letter wrapper is accepted downstream. A failed
  re-emit re-reads the record instead of skipping it: advancing would lose
  the bytes exactly when the pipeline is under enough pressure to reject
  them, which is the moment dead-lettering exists for.

## Configuration

The lease store is selected by `lease_backend` (`memory` or `dynamodb`). The
`memory` backend keeps no state across restarts and does not coordinate across
replicas; use `dynamodb` (with `lease_table`) for any multi-replica or
restart-durable deployment. Timing is controlled by `poll_interval`,
`heartbeat_interval` (must be less than `lease_duration`), and
`discovery_interval`.

## Observability

The receiver holds no logging or metrics configuration of its own; it logs
through the Collector-provided logger and emits instruments through the
Collector-provided `MeterProvider`. Verbosity, encoding, and routing are
controlled by the Collector's `service::telemetry` config, and the instruments
are exported wherever that config sends them (`level: none` disables them).

Instruments (scope `awskinesisreceiver`):

- `kinesis.receiver.poll.records` (histogram) — records per `GetRecords` call.
- `kinesis.receiver.poll.bytes` (histogram) — aggregate record bytes per call.
- `kinesis.receiver.poll.duration_ms` (histogram) — `GetRecords` latency.
- `kinesis.receiver.lease.events` (counter, `event` =
  `acquire`/`release`/`steal`/`checkpoint`/`heartbeat_lost`, `result` =
  `success`/`conflict`) — shard-lease lifecycle.
- `kinesis.receiver.shards.owned` (up-down counter) — shards this replica is
  actively polling.
- `kinesis.receiver.dead_letter.records` (counter, `result` =
  `success`/`error`) — dead-letter emit attempts. A sustained `error` rate
  means the dead-letter pipeline itself is rejecting and the affected shard
  is intentionally held (see above) rather than silently dropping bytes.

Set the Collector log level to `debug` to log poll cycles, checkpoint advances,
lease acquisition, and reconcile decisions.

Supported encodings are `otlp_proto` (default) and `otlp_json`.
The encoding and codec must match the exporter's.

**Status:** working proof of concept for traces, metrics, and logs, with
leaderless fair-share rebalancing across replicas. Resharding
(parent-drains-before-child) is gated in the acquisition path but not yet
verified against a live shard split.
