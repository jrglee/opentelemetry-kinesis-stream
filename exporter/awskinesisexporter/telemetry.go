package awskinesisexporter

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// scopeName is the instrumentation scope for this component's self-telemetry.
const scopeName = "awskinesisexporter"

// exporterTelemetry holds the component's internal performance instruments.
// They are created from the collector-provided MeterProvider, so their export,
// level-gating, and resource attributes are governed by the collector's
// service::telemetry configuration — this component owns no metrics plumbing.
type exporterTelemetry struct {
	recordsDropped metric.Int64Counter
	batchRecords   metric.Int64Histogram
	batchBytes     metric.Int64Histogram
	flushDuration  metric.Float64Histogram
}

func newExporterTelemetry(mp metric.MeterProvider) (*exporterTelemetry, error) {
	meter := mp.Meter(scopeName)
	dropped, err := meter.Int64Counter(
		"kinesis.exporter.records_dropped",
		metric.WithDescription("items dropped because no oversize repack could fit them or a record was permanently rejected"),
		metric.WithUnit("{item}"),
	)
	if err != nil {
		return nil, fmt.Errorf("dropped counter: %w", err)
	}
	batchRecords, err := meter.Int64Histogram(
		"kinesis.exporter.batch.records",
		metric.WithDescription("records in a single PutRecords call"),
		metric.WithUnit("{record}"),
	)
	if err != nil {
		return nil, fmt.Errorf("batch records histogram: %w", err)
	}
	batchBytes, err := meter.Int64Histogram(
		"kinesis.exporter.batch.bytes",
		metric.WithDescription("aggregate record payload bytes in a single PutRecords call"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("batch bytes histogram: %w", err)
	}
	flushDuration, err := meter.Float64Histogram(
		"kinesis.exporter.flush.duration_ms",
		metric.WithDescription("PutRecords call latency"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("flush duration histogram: %w", err)
	}
	return &exporterTelemetry{
		recordsDropped: dropped,
		batchRecords:   batchRecords,
		batchBytes:     batchBytes,
		flushDuration:  flushDuration,
	}, nil
}

// recordDrop counts dropped items, tagged by reason so silent data loss is observable.
func (t *exporterTelemetry) recordDrop(ctx context.Context, n int, reason string) {
	t.recordsDropped.Add(ctx, int64(n), metric.WithAttributes(attribute.String("reason", reason)))
}

// recordPut observes one PutRecords call's size and latency.
func (t *exporterTelemetry) recordPut(ctx context.Context, records int, bytes int, durationMs float64) {
	t.batchRecords.Record(ctx, int64(records))
	t.batchBytes.Record(ctx, int64(bytes))
	t.flushDuration.Record(ctx, durationMs)
}
