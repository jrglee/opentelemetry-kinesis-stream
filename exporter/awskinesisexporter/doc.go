// Package awskinesisexporter exports OpenTelemetry telemetry to an Amazon
// Kinesis Data Stream.
//
// The exporter marshals telemetry under a configured encoding, compresses the
// result under a configured codec, batches the resulting payloads into
// Kinesis records up to the stream's per-record size limit, and writes them
// with PutRecords. Records are partitioned by a configurable strategy that
// determines per-shard ordering on the consuming side.
//
// This package is currently a scaffolding stub: the factory registers but
// the exporter is a no-op. Configuration fields will land alongside the
// implementation that consumes them.
package awskinesisexporter
