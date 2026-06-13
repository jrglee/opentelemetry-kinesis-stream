# awskinesisreceiver

OpenTelemetry Collector receiver for Amazon Kinesis Data Streams.

The receiver claims shards through a lease store (an in-memory store for
single-replica development, or a DynamoDB store for multi-replica
deployments), polls each owned shard, decompresses and decodes records under
a configured codec and encoding, hands the resulting telemetry to the
downstream pipeline, and checkpoints the shard's read position only after
downstream acceptance.

## Ownership and failure handling

- Each replica heartbeats its leases; a lease whose heartbeat lapses for
  `lease_duration` may be claimed by another replica. There is no preemptive
  rebalancing — a healthy owner keeps its shards, so one replica can hold more
  than its even share. Acquisition is fenced by a per-lease counter, so two
  replicas never make progress on the same shard concurrently.
- The checkpoint advances only over records that were delivered downstream or
  are permanently unprocessable (a decode/decompress failure, or a downstream
  rejection marked permanent). A record the downstream **transiently** rejects
  (backpressure, restart) is re-read rather than skipped, so valid telemetry
  is not dropped under load.
- A failed or expired shard iterator is re-opened from the persisted
  checkpoint rather than reused, so transient `GetRecords` errors recover
  instead of spinning.

## Configuration

The lease store is selected by `lease_backend` (`memory` or `dynamodb`). The
`memory` backend keeps no state across restarts and does not coordinate across
replicas; use `dynamodb` (with `lease_table`) for any multi-replica or
restart-durable deployment. Timing is controlled by `poll_interval`,
`heartbeat_interval` (must be less than `lease_duration`), and
`discovery_interval`.

**Status:** working proof of concept for traces over a single, non-resharding
stream. Resharding (parent-drains-before-child) and capacity rebalancing are
not implemented yet.
