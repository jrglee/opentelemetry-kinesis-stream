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

## Operating under backpressure

The receiver is synchronous and unbuffered: a shard goroutine delivers each
record downstream before reading the next batch, so a slow downstream throttles
reads at the source and unread records stay in Kinesis (iterator age rises)
instead of piling up in Collector memory. This holds only if nothing between the
receiver and its sink buffers acceptances asynchronously.

**Acceptance is not durable delivery.** The receiver checkpoints a record once
the downstream *accepts* it, and under the Collector's consumer contract
"acceptance" means "accepted for processing," not "written." An exporter
`sending_queue` — enabled by default — accepts a record the moment it enters the
queue. Under load that breaks the chain:

- Ingress outruns egress: the receiver fills the queue at read speed while the
  sink drains slower, and the checkpoint runs ahead of what was actually written.
- When the queue fills, enqueue fails transiently; the receiver re-reads,
  freezes the checkpoint, and pauses, then races ahead as the queue drains. The
  result is a sawtooth iterator age and memory sized to the queue.
- A non-persistent queue drops its contents on restart, and the checkpoint has
  already advanced past them — a silent-loss window.

**Keep the path synchronous.** For backpressure to reach Kinesis and
at-least-once to hold, don't put an async, non-persistent buffer between the
receiver and its sink. Either disable the exporter's sending queue — so delivery
blocks until the write and its retries finish — or, to smooth bursts, make the
queue block on overflow and persist it. This is operator configuration, not
something the receiver enforces.

```yaml
exporters:
  prometheusremotewrite:
    endpoint: https://backend.example/api/v1/write
    # Delivery blocks until the write completes: the checkpoint tracks durable
    # writes and reads pause when the backend is slow. Kinesis holds the backlog.
    sending_queue:
      enabled: false
    # A backend down past max_elapsed_time surfaces a retryable error; the
    # receiver re-reads (holding the lease) rather than dropping.
    retry_on_failure:
      enabled: true
      max_elapsed_time: 30s
    timeout: 10s
```

To smooth bursts instead, keep the queue but block on overflow and persist it
(needs a `file_storage` extension):

```yaml
    sending_queue:
      enabled: true
      block_on_overflow: true
      storage: file_storage
```

**Tuning.** With a blocking downstream, a transient rejection re-reads from the
last checkpoint, not mid-batch, so a smaller `max_records` (the E2E stack uses
`1000` vs. the `10000` default) checkpoints more often and re-reads less, at the
cost of more `GetRecords` calls. Keep `poll_interval` at or above the
five-reads/sec/shard quota (default `250ms`).

**Signals.** When the path is synchronous, ingress and egress converge, iterator
age measures real backlog instead of oscillating, and the
`kinesis.receiver.poll.*` histograms reflect true throughput. A rising stuck
backoff — logged after five passes that checkpoint nothing — means the sink is
genuinely rejecting the head record, so the shard is held (lease kept) rather
than dropped. A `memory_limiter` ahead of the sink behaves the same way: its
refusal is transient, so the receiver holds the shard rather than dropping —
size its limits accordingly.

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
