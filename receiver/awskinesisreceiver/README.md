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

The receiver is synchronous and holds no buffer of its own: a shard's goroutine
delivers each record downstream and only then reads the next batch, so a
downstream that cannot keep up structurally throttles reads at the source.
Kinesis is meant to be the buffer here — when the pipeline slows, unread records
stay in the stream (iterator age rises) rather than piling up in Collector
memory. Realizing that requires one thing from the downstream exporter, and it
is easy to give up by accident.

**Acceptance is not durable delivery.** The receiver checkpoints a record once
the downstream *accepts* it. Under the Collector's consumer contract,
"acceptance" means "accepted for processing," not "written to its destination."
If a component between this receiver and the sink buffers acceptances
asynchronously — most commonly an exporter `sending_queue`, which is enabled by
default — then a record is "accepted" the moment it enters the queue, not when
it is durably written. The consequences compound under load:

- The read rate stops tracking the write rate. The receiver fills the queue at
  read speed while the sink drains it slower, so ingress far outruns egress and
  the checkpoint runs ahead of what has actually been written.
- When the queue fills, the enqueue fails with a transient error, which the
  receiver treats as backpressure: it re-reads rather than skips, freezes the
  checkpoint, and pauses. As the queue drains it resumes and races ahead again.
  That cycle shows up as an oscillating (sawtooth) iterator age and a resident
  memory footprint the size of the queue.
- A non-persistent queue loses whatever it holds on restart, and because the
  checkpoint already advanced past those records, they are not re-read. What
  looks like a smoothing buffer is a silent-loss window.

**The operating contract.** For backpressure to reach Kinesis and for
at-least-once delivery to hold, do not place an asynchronous, non-persistent
buffer between this receiver and its sink. Either run the downstream exporter
with its sending queue disabled — so a delivery call blocks until the write (and
its retries) complete, making acceptance equal durable delivery — or, if you
need the queue to smooth bursts, make it both blocking on overflow and backed by
persistent storage. This is guidance the operator applies to the pipeline, not a
constraint the receiver enforces.

The synchronous shape, for a metrics pipeline writing to a remote-write backend:

```yaml
exporters:
  prometheusremotewrite:
    endpoint: https://backend.example/api/v1/write
    # Disable the async queue: delivery blocks until the write completes, so
    # the receiver's checkpoint tracks durable writes and reads pause when the
    # backend is slow. Kinesis holds the backlog.
    sending_queue:
      enabled: false
    # Keep retries: a backend that stays down past max_elapsed_time surfaces a
    # retryable error, and the receiver re-reads (holding the lease) rather than
    # dropping. Bound it so a hard outage is not retried forever in-line.
    retry_on_failure:
      enabled: true
      max_elapsed_time: 30s
    timeout: 10s
```

To smooth bursts instead of blocking outright, keep the queue but make overflow
block and persist it (requires a `file_storage` extension):

```yaml
    sending_queue:
      enabled: true
      block_on_overflow: true
      storage: file_storage
```

**Tune `max_records` and `poll_interval` for the synchronous path.** With a
blocking downstream, each poll delivers its whole batch in-line before the next
read, and a transient rejection re-reads from the last checkpoint rather than
mid-batch. A smaller `max_records` (the E2E stack uses `1000`, not the `10000`
default) checkpoints more often and re-reads less on a rejection, at the cost of
more `GetRecords` calls; keep `poll_interval` at or above the 5-reads/sec/shard
quota (default `250ms`).

**Reading the signals when the contract holds.** Ingress and egress converge,
iterator age becomes a clean measure of backlog rather than a sawtooth, and the
`kinesis.receiver.poll.*` histograms reflect true throughput. A rising stuck
backoff — logged after five consecutive passes that checkpoint nothing — means
the sink is genuinely rejecting the head record, so the shard is deliberately
held (and its lease kept) rather than dropping bytes.

This pipeline has no `memory_limiter`. If one is added ahead of the sink, its
data-refused error is transient, so the receiver backpressures (re-reads) rather
than drops under memory pressure — the correct behavior, but it means a refusing
`memory_limiter` also freezes the checkpoint and holds the shard; size its
limits with that in mind.

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
