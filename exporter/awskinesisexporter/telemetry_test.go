package awskinesisexporter

import (
	"context"
	"testing"

	noopmetric "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestNewExporterTelemetryProviders(t *testing.T) {
	if _, err := newExporterTelemetry(noopmetric.NewMeterProvider()); err != nil {
		t.Fatalf("noop provider: %v", err)
	}
	if _, err := newExporterTelemetry(sdkmetric.NewMeterProvider()); err != nil {
		t.Fatalf("sdk provider: %v", err)
	}
}

func TestExporterTelemetryRecordPut(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	tel, err := newExporterTelemetry(mp)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	tel.recordPut(ctx, 7, 2048, 3.25)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	metrics := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			metrics[m.Name] = m
		}
	}

	if got := histInt64Sum(t, metrics, "kinesis.exporter.batch.records"); got != 7 {
		t.Fatalf("batch.records sum: got %d want 7", got)
	}
	if got := histInt64Sum(t, metrics, "kinesis.exporter.batch.bytes"); got != 2048 {
		t.Fatalf("batch.bytes sum: got %d want 2048", got)
	}
	if got := histFloat64Count(t, metrics, "kinesis.exporter.flush.duration_ms"); got != 1 {
		t.Fatalf("flush.duration_ms count: got %d want 1", got)
	}
}

func histInt64Sum(t *testing.T, metrics map[string]metricdata.Metrics, name string) int64 {
	t.Helper()
	h, ok := metrics[name].Data.(metricdata.Histogram[int64])
	if !ok {
		t.Fatalf("%s: not an int64 histogram (%T)", name, metrics[name].Data)
	}
	var sum int64
	for _, dp := range h.DataPoints {
		sum += dp.Sum
	}
	return sum
}

func histFloat64Count(t *testing.T, metrics map[string]metricdata.Metrics, name string) uint64 {
	t.Helper()
	h, ok := metrics[name].Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("%s: not a float64 histogram (%T)", name, metrics[name].Data)
	}
	var count uint64
	for _, dp := range h.DataPoints {
		count += dp.Count
	}
	return count
}
