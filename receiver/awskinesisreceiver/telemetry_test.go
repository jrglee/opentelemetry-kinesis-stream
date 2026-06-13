package awskinesisreceiver

import (
	"context"
	"testing"

	noopmetric "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestNewReceiverTelemetryProviders(t *testing.T) {
	if _, err := newReceiverTelemetry(noopmetric.NewMeterProvider()); err != nil {
		t.Fatalf("noop provider: %v", err)
	}
	if _, err := newReceiverTelemetry(sdkmetric.NewMeterProvider()); err != nil {
		t.Fatalf("sdk provider: %v", err)
	}
}

func TestReceiverTelemetryRecords(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	tel, err := newReceiverTelemetry(mp)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	tel.recordPoll(ctx, 12, 4096, 8.5)
	tel.recordLeaseEvent(ctx, leaseAcquire, resultSuccess)
	tel.recordLeaseEvent(ctx, leaseAcquire, resultConflict)
	tel.addOwnedShards(ctx, 1)
	tel.addOwnedShards(ctx, 1)
	tel.addOwnedShards(ctx, -1)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	metrics := indexMetrics(t, &rm)

	if got := histInt64Sum(t, metrics, "kinesis.receiver.poll.records"); got != 12 {
		t.Fatalf("poll.records sum: got %d want 12", got)
	}
	if got := histInt64Sum(t, metrics, "kinesis.receiver.poll.bytes"); got != 4096 {
		t.Fatalf("poll.bytes sum: got %d want 4096", got)
	}
	if got := histFloat64Count(t, metrics, "kinesis.receiver.poll.duration_ms"); got != 1 {
		t.Fatalf("poll.duration_ms count: got %d want 1", got)
	}
	if got := sumInt64(t, metrics, "kinesis.receiver.lease.events"); got != 2 {
		t.Fatalf("lease.events total: got %d want 2", got)
	}
	if got := leaseEventCount(t, metrics, leaseAcquire, resultConflict); got != 1 {
		t.Fatalf("acquire/conflict: got %d want 1", got)
	}
	if got := sumInt64(t, metrics, "kinesis.receiver.shards.owned"); got != 1 {
		t.Fatalf("shards.owned: got %d want 1", got)
	}
}

func indexMetrics(t *testing.T, rm *metricdata.ResourceMetrics) map[string]metricdata.Metrics {
	t.Helper()
	out := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
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

func sumInt64(t *testing.T, metrics map[string]metricdata.Metrics, name string) int64 {
	t.Helper()
	s, ok := metrics[name].Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("%s: not an int64 sum (%T)", name, metrics[name].Data)
	}
	var total int64
	for _, dp := range s.DataPoints {
		total += dp.Value
	}
	return total
}

func leaseEventCount(t *testing.T, metrics map[string]metricdata.Metrics, event, result string) int64 {
	t.Helper()
	s, ok := metrics["kinesis.receiver.lease.events"].Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("lease.events: not an int64 sum")
	}
	for _, dp := range s.DataPoints {
		ev, _ := dp.Attributes.Value("event")
		res, _ := dp.Attributes.Value("result")
		if ev.AsString() == event && res.AsString() == result {
			return dp.Value
		}
	}
	return 0
}
