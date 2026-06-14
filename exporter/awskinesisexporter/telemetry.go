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
	recordsDropped      metric.Int64Counter
	attributesTruncated metric.Int64Counter
	batchRecords        metric.Int64Histogram
	batchBytes          metric.Int64Histogram
	flushDuration       metric.Float64Histogram
}

func newExporterTelemetry(mp metric.MeterProvider) (*exporterTelemetry, error) {
	meter := mp.Meter(scopeName)
	dropped, err := meter.Int64Counter(
		"kinesis.exporter.records_dropped",
		metric.WithDescription("items dropped during encode or send; reason distinguishes the cause (marshal_error, compress_error, max_attempts, irreducible, reject_policy, chain_exhausted, rejected)"),
		metric.WithUnit("{item}"),
	)
	if err != nil {
		return nil, fmt.Errorf("dropped counter: %w", err)
	}
	truncated, err := meter.Int64Counter(
		"kinesis.exporter.attributes_truncated",
		metric.WithDescription("attribute values clamped by the truncate_attribute_values policy. Counts every value mutation, regardless of whether truncation alone fit the record or split_half ultimately shipped it — what matters is that data left the exporter in a mutated form."),
		metric.WithUnit("{attribute}"),
	)
	if err != nil {
		return nil, fmt.Errorf("attributes_truncated counter: %w", err)
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
		recordsDropped:      dropped,
		attributesTruncated: truncated,
		batchRecords:        batchRecords,
		batchBytes:          batchBytes,
		flushDuration:       flushDuration,
	}, nil
}

// recordDrop counts dropped items, tagged by reason so silent data loss is observable.
func (t *exporterTelemetry) recordDrop(ctx context.Context, n int, reason string) {
	t.recordsDropped.Add(ctx, int64(n), metric.WithAttributes(attribute.String("reason", reason)))
}

// recordTruncated counts attribute values clamped by truncate_attribute_values.
// Emitted whenever clamping happens, even if a later policy (split_half) was
// what ultimately fit the payload — the data still left the exporter mutated,
// and that mutation must be observable.
func (t *exporterTelemetry) recordTruncated(ctx context.Context, n int) {
	t.attributesTruncated.Add(ctx, int64(n))
}

// recordPut observes one PutRecords call's size and latency.
func (t *exporterTelemetry) recordPut(ctx context.Context, records int, bytes int, durationMs float64) {
	t.batchRecords.Record(ctx, int64(records))
	t.batchBytes.Record(ctx, int64(bytes))
	t.flushDuration.Record(ctx, durationMs)
}
