// Package awskinesisreceiver receives OpenTelemetry telemetry from an Amazon
// Kinesis Data Stream.
//
// The receiver claims shards via a coordination mechanism, polls each owned
// shard, decompresses and decodes each record under a configured codec and
// encoding, hands the resulting telemetry to the downstream Collector
// pipeline, and checkpoints the shard's read position only after downstream
// acceptance. Resharding (split and merge of Kinesis shards) is handled by
// draining parent shards before reading child shards so per-key ordering is
// preserved.
//
// This package is currently a scaffolding stub: the factory registers but
// the receiver is a no-op. Configuration fields will land alongside the
// implementation that consumes them.
package awskinesisreceiver
