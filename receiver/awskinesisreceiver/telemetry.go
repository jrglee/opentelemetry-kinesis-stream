package awskinesisreceiver

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// scopeName is the instrumentation scope for this component's self-telemetry.
const scopeName = "awskinesisreceiver"

// Lease event names recorded on the lease.events counter.
const (
	leaseAcquire      = "acquire"
	leaseRelease      = "release"
	leaseSteal        = "steal"
	leaseCheckpoint   = "checkpoint"
	leaseHeartbeatGot = "heartbeat_lost"
	resultSuccess     = "success"
	resultConflict    = "conflict"
)

// receiverTelemetry holds the component's internal performance instruments.
// They are created from the collector-provided MeterProvider, so their export,
// level-gating, and resource attributes are governed by the collector's
// service::telemetry configuration — this component owns no metrics plumbing.
type receiverTelemetry struct {
	pollRecords  metric.Int64Histogram
	pollBytes    metric.Int64Histogram
	pollDuration metric.Float64Histogram
	leaseEvents  metric.Int64Counter
	shardsOwned  metric.Int64UpDownCounter
}

func newReceiverTelemetry(mp metric.MeterProvider) (*receiverTelemetry, error) {
	meter := mp.Meter(scopeName)
	pollRecords, err := meter.Int64Histogram(
		"kinesis.receiver.poll.records",
		metric.WithDescription("records returned by a single GetRecords call"),
		metric.WithUnit("{record}"),
	)
	if err != nil {
		return nil, fmt.Errorf("poll records histogram: %w", err)
	}
	pollBytes, err := meter.Int64Histogram(
		"kinesis.receiver.poll.bytes",
		metric.WithDescription("aggregate record bytes returned by a single GetRecords call"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("poll bytes histogram: %w", err)
	}
	pollDuration, err := meter.Float64Histogram(
		"kinesis.receiver.poll.duration_ms",
		metric.WithDescription("GetRecords call latency"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("poll duration histogram: %w", err)
	}
	leaseEvents, err := meter.Int64Counter(
		"kinesis.receiver.lease.events",
		metric.WithDescription("shard lease lifecycle events, tagged by event and result"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return nil, fmt.Errorf("lease events counter: %w", err)
	}
	shardsOwned, err := meter.Int64UpDownCounter(
		"kinesis.receiver.shards.owned",
		metric.WithDescription("shards currently owned (actively polled) by this replica"),
		metric.WithUnit("{shard}"),
	)
	if err != nil {
		return nil, fmt.Errorf("shards owned counter: %w", err)
	}
	return &receiverTelemetry{
		pollRecords:  pollRecords,
		pollBytes:    pollBytes,
		pollDuration: pollDuration,
		leaseEvents:  leaseEvents,
		shardsOwned:  shardsOwned,
	}, nil
}

// recordPoll observes one GetRecords call's size and latency.
func (t *receiverTelemetry) recordPoll(ctx context.Context, records int, bytes int, durationMs float64) {
	t.pollRecords.Record(ctx, int64(records))
	t.pollBytes.Record(ctx, int64(bytes))
	t.pollDuration.Record(ctx, durationMs)
}

// recordLeaseEvent counts a lease lifecycle event tagged by event and result.
func (t *receiverTelemetry) recordLeaseEvent(ctx context.Context, event, result string) {
	t.leaseEvents.Add(ctx, 1, metric.WithAttributes(
		attribute.String("event", event),
		attribute.String("result", result),
	))
}

// addOwnedShards adjusts the owned-shard gauge by delta (+1 acquire, -1 release).
func (t *receiverTelemetry) addOwnedShards(ctx context.Context, delta int) {
	t.shardsOwned.Add(ctx, int64(delta))
}
