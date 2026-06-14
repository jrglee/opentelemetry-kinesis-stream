# Troubleshooting

Symptom → cause → fix, keyed off the log lines and metrics the components emit.
Logging and metrics are controlled entirely by the collector's
`service::telemetry` config (see [observability](user-guide.md#observability));
this component adds no knobs of its own.

## Turning on detail

Set the collector's log level to see normal operation, not just failures:

```yaml
service:
  telemetry:
    logs:
      level: debug
```

At `debug` the receiver logs each poll cycle (`polled shard` with record/byte
counts and duration), checkpoint advances (`checkpoint advanced`), lease
acquisition (`lease acquired`), reconcile decisions (`reconcile pass`), iterator
opens (`opened iterator`), and heartbeats (`heartbeat ok`); the exporter logs
each `emit` and `put_records`. Leave it at `info` in production.

## No telemetry comes out the receiver

- **`memory lease backend selected; checkpoints will not survive process
  restart …`** (warn at startup). The `memory` backend does not coordinate
  across replicas or survive restarts. For anything beyond a single dev process,
  set `lease_backend: dynamodb` with a `lease_table`. See
  [lease backends](user-guide.md#lease-backends).
- **No `polled shard` debug lines at all.** The receiver never acquired a shard.
  Check for `shard discovery failed` / `list leases failed` (IAM or endpoint
  problem) and confirm the stream name and region. With DynamoDB, confirm the
  lease table exists and is writable.
- **`opened iterator` then silence.** The shard has no new records, or you are
  reading from a checkpoint past the data. Check the producer is actually
  writing, and watch `kinesis.receiver.poll.records` — a flat zero means an empty
  shard, not a stuck poller.

## Records appear duplicated downstream

Delivery is at-least-once around a handoff. A small number of duplicates right
after a rebalance, a failover, or a steal (`stealing shard to rebalance`,
`releasing shard to rebalance`) is expected: the new owner resumes from the last
checkpoint. Steady-state duplication is not — look for repeated
`heartbeat lost lease; stopping poller`, which means leases are expiring under
you (see next item).

## `heartbeat lost lease; stopping poller`

The lease's fencing counter moved out from under this replica — another replica
acquired it, usually because the heartbeat could not renew in time. Causes:

- `heartbeat_interval` too close to or above `lease_duration`. The heartbeat must
  renew comfortably within the lease window; keep `heartbeat_interval` well below
  `lease_duration`.
- DynamoDB throttling or latency stalling the heartbeat write.
- A long GC/CPU pause. Watch `kinesis.receiver.lease.events` with
  `event=heartbeat_lost`.

## `shard iterator expired; re-opening from checkpoint`

Kinesis shard iterators live ~5 minutes. If a poll cycle takes longer than that
(slow downstream, very large `max_records`), the iterator expires and is
re-opened from the checkpoint — this is self-healing and only a problem if it
repeats every cycle. Reduce `max_records` or address downstream backpressure.

## `get_records failed; backing off`

Transient Kinesis error (commonly `ProvisionedThroughputExceededException`).
The poller keeps the iterator and backs off. Persistent throttling means too
many readers per shard or too small a stream — reduce poll frequency
(`poll_interval`) or add shards. Watch `kinesis.receiver.poll.duration_ms`
climbing alongside these.

## Exporter drops or rejects records

- **`dropped oversize items during repack`** + `kinesis.exporter.records_dropped`
  with `reason=oversize`. A single item compressed larger than `max_record_size`
  and could not be split further. Raise `max_record_size` (up to the Kinesis
  ceiling) or check the oversize policy. See
  [oversize records](user-guide.md#oversize-records).
- **`kinesis record rejected`** + `records_dropped` with `reason=rejected`. A
  record failed for a non-throttling reason (e.g. a validation error) and would
  fail identically on retry, so it is dropped to avoid head-of-line blocking.
  The log line includes the Kinesis error code.
- **Throttling** is *not* dropped — it is retried, so the whole batch (including
  already-succeeded records) is re-sent, which is the accepted at-least-once
  cost. Watch `kinesis.exporter.batch.records` / `batch.bytes` to see whether
  batches are oversized for your shard capacity.

## Dead-lettered records

`decompress failed` / `decode failed` mean a record's bytes are unprocessable.
With `dead_letter.enabled`, they are re-emitted into the pipeline as telemetry
(a `kinesis.dead_letter` span or gauge) carrying the raw bytes and failure
class; route them with standard collector components. A high dead-letter rate
usually means the exporter and receiver disagree on `encoding`
(`otlp_proto`/`otlp_json`/`otel_arrow`) or `compression`
(`none`/`gzip`/`zstd`/`snappy`/`x-snappy-framed`/`zlib`/`deflate`) —
both must match on each end. See
[dead-letter handling](user-guide.md#dead-letter-handling).

## Shutdown logs `shutdown deadline expired; hard-cancelling pollers`

The graceful drain did not finish within the collector's shutdown deadline, so
pollers were hard-cancelled (their lease Release is independently bounded). A
half-drained shard simply resumes from its last checkpoint on the next owner.
If this happens every shutdown, the deadline is too short for the in-flight
batch size — reduce `max_records` or increase the collector's shutdown timeout.

## Internal metrics are missing

If `kinesis.receiver.*` / `kinesis.exporter.*` do not show up where you scrape
them, check `service::telemetry::metrics::level` is not `none` (which hands the
component a no-op meter and emits nothing) and that you have a `readers:` entry
exposing them. See [observability](user-guide.md#observability).
