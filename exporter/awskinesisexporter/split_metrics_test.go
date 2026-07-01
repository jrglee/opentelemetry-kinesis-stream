package awskinesisexporter

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// TestSplitMetricsHalfSingleMetricManyDatapoints guards the oversize-recovery
// fix: a resource holding a single metric with many datapoints must split by
// datapoints (preserving the metric shell), not be declared irreducible and
// dropped. This shape is common once partitioning by a datapoint attribute
// produces one origin metric per record.
func TestSplitMetricsHalfSingleMetricManyDatapoints(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.SetSchemaUrl("https://example.com/s/1")
	m := sm.Metrics().AppendEmpty()
	m.SetName("requests")
	s := m.SetEmptySum()
	s.SetIsMonotonic(true)
	s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	for i := 0; i < 8; i++ {
		s.DataPoints().AppendEmpty().SetIntValue(int64(i))
	}

	a, b, ok := splitMetricsHalf(md)
	if !ok {
		t.Fatal("single metric with 8 datapoints must be splittable, got irreducible")
	}
	if got := a.DataPointCount() + b.DataPointCount(); got != 8 {
		t.Fatalf("datapoints across halves: got %d want 8 (loss)", got)
	}
	if a.DataPointCount() == 0 || b.DataPointCount() == 0 {
		t.Fatalf("split produced an empty half: a=%d b=%d", a.DataPointCount(), b.DataPointCount())
	}

	// Shell fidelity + identity on the left half.
	am := a.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
	if am.Name() != "requests" || am.Type() != pmetric.MetricTypeSum {
		t.Fatalf("left shell wrong: name=%q type=%v", am.Name(), am.Type())
	}
	if !am.Sum().IsMonotonic() || am.Sum().AggregationTemporality() != pmetric.AggregationTemporalityCumulative {
		t.Error("left Sum settings (monotonic/cumulative) not preserved")
	}
	if sn, _ := a.ResourceMetrics().At(0).Resource().Attributes().Get("service.name"); sn.AsString() != "svc" {
		t.Error("left resource identity lost")
	}
	if a.ResourceMetrics().At(0).ScopeMetrics().At(0).SchemaUrl() != "https://example.com/s/1" {
		t.Error("left scope schema URL lost")
	}
}

// TestSplitMetricsHalfSingleDatapointIrreducible confirms the terminal case: a
// single metric with a single datapoint cannot be split further.
func TestSplitMetricsHalfSingleDatapointIrreducible(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc")
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("g")
	m.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(1)

	if _, _, ok := splitMetricsHalf(md); ok {
		t.Fatal("single metric with one datapoint must be irreducible")
	}
}
