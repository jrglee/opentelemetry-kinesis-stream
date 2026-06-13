# awskinesisreceiver

OpenTelemetry Collector receiver for Amazon Kinesis Data Streams.

The receiver claims shards via a coordination mechanism, polls each owned
shard, decompresses and decodes records under a configured codec and
encoding, hands the resulting telemetry to the downstream pipeline, and
checkpoints the shard's read position only after downstream acceptance.
Resharding is handled by draining parent shards before reading child shards
so per-key ordering is preserved.

**Status:** scaffolding stub. The factory registers and produces a no-op
receiver; shard ownership, checkpointing, reshard handling, and the decode
path are not implemented yet.
