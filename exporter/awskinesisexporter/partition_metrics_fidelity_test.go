package awskinesisexporter

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// totalDatapointsInMetric returns the datapoint count for any metric kind.
func totalDatapointsInMetric(m pmetric.Metric) int {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		return m.Gauge().DataPoints().Len()
	case pmetric.MetricTypeSum:
		return m.Sum().DataPoints().Len()
	case pmetric.MetricTypeHistogram:
		return m.Histogram().DataPoints().Len()
	case pmetric.MetricTypeExponentialHistogram:
		return m.ExponentialHistogram().DataPoints().Len()
	case pmetric.MetricTypeSummary:
		return m.Summary().DataPoints().Len()
	}
	return 0
}

// Silence the unused-function linter for totalDatapointsInMetric in case
// some tests use it via inline assertions.
var _ = totalDatapointsInMetric

// --- test 4: metric-shell fidelity (Sum + Histogram settings) ---

func TestGroupMetricsByLeaf_MetricShellFidelity(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.SetSchemaUrl("https://example.com/schema/1")

	// Monotonic cumulative Sum
	mSum := sm.Metrics().AppendEmpty()
	mSum.SetName("my.sum")
	mSum.SetDescription("a sum metric")
	mSum.SetUnit("bytes")
	s := mSum.SetEmptySum()
	s.SetIsMonotonic(true)
	s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := s.DataPoints().AppendEmpty()
	dp.Attributes().PutStr("instance", "i1")
	dp.SetIntValue(42)

	// Histogram with AggregationTemporality
	mHist := sm.Metrics().AppendEmpty()
	mHist.SetName("my.hist")
	h := mHist.SetEmptyHistogram()
	h.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	hdp := h.DataPoints().AppendEmpty()
	hdp.Attributes().PutStr("instance", "i1")
	hdp.SetCount(5)

	// Group by datapoint instance (all same → 1 batch)
	plan := resolveTestPlan(t, []PartitionKeySource{
		{Source: keySourceDatapoint, Name: "instance"},
	})

	batches := groupMetricsByLeaf(md, plan)

	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	batch := batches[0].batch

	// Find the Sum and Histogram metrics in the output
	var destSum, destHist pmetric.Metric
	rms := batch.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				switch m.Name() {
				case "my.sum":
					destSum = m
				case "my.hist":
					destHist = m
				}
			}
		}
	}

	if destSum == (pmetric.Metric{}) {
		t.Fatal("Sum metric not found in output batch")
	}
	if destHist == (pmetric.Metric{}) {
		t.Fatal("Histogram metric not found in output batch")
	}

	// Verify Sum settings
	if !destSum.Sum().IsMonotonic() {
		t.Error("Sum: IsMonotonic should be true")
	}
	if destSum.Sum().AggregationTemporality() != pmetric.AggregationTemporalityCumulative {
		t.Errorf("Sum: AggregationTemporality = %v; want Cumulative", destSum.Sum().AggregationTemporality())
	}
	if destSum.Description() != "a sum metric" {
		t.Errorf("Sum: Description = %q; want %q", destSum.Description(), "a sum metric")
	}
	if destSum.Unit() != "bytes" {
		t.Errorf("Sum: Unit = %q; want %q", destSum.Unit(), "bytes")
	}

	// Verify Histogram settings
	if destHist.Histogram().AggregationTemporality() != pmetric.AggregationTemporalityDelta {
		t.Errorf("Histogram: AggregationTemporality = %v; want Delta", destHist.Histogram().AggregationTemporality())
	}

	// Round-trip via OTLP proto marshal/unmarshal
	enc, err := encoding.NewMetricsEncoder(encoding.EncodingOTLPProto)
	if err != nil {
		t.Fatalf("encoder: %v", err)
	}
	raw, err := enc.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	dec, err := encoding.NewMetricsDecoder(encoding.EncodingOTLPProto)
	if err != nil {
		t.Fatalf("decoder: %v", err)
	}
	rt, err := dec.Unmarshal(raw)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Verify settings survive round-trip
	var rtSum, rtHist pmetric.Metric
	rtRms := rt.ResourceMetrics()
	for i := 0; i < rtRms.Len(); i++ {
		sms := rtRms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				switch m.Name() {
				case "my.sum":
					rtSum = m
				case "my.hist":
					rtHist = m
				}
			}
		}
	}
	if rtSum == (pmetric.Metric{}) {
		t.Fatal("round-trip: Sum metric not found")
	}
	if !rtSum.Sum().IsMonotonic() {
		t.Error("round-trip: Sum IsMonotonic should be true")
	}
	if rtSum.Sum().AggregationTemporality() != pmetric.AggregationTemporalityCumulative {
		t.Errorf("round-trip: Sum AggregationTemporality = %v; want Cumulative", rtSum.Sum().AggregationTemporality())
	}
	if rtHist == (pmetric.Metric{}) {
		t.Fatal("round-trip: Histogram not found")
	}
	if rtHist.Histogram().AggregationTemporality() != pmetric.AggregationTemporalityDelta {
		t.Errorf("round-trip: Histogram AggregationTemporality = %v; want Delta", rtHist.Histogram().AggregationTemporality())
	}
}

// --- test 5: promotion ---

func TestGroupMetricsByLeaf_Promotion(t *testing.T) {
	t.Run("metric_name regex promotes to datapoint attr", func(t *testing.T) {
		md := namedMetrics("svc", []string{"http_requests"})
		plan := resolveTestPlan(t, []PartitionKeySource{
			{Source: keySourceMetricName, Regex: `^([a-z]+)_`, Promote: "namespace"},
		})

		batches := groupMetricsByLeaf(md, plan)
		if len(batches) != 1 {
			t.Fatalf("expected 1 batch, got %d", len(batches))
		}

		m := batches[0].batch.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
		dp := m.Gauge().DataPoints().At(0)
		v, ok := dp.Attributes().Get("namespace")
		if !ok {
			t.Fatal("expected 'namespace' attribute on dest datapoint")
		}
		if v.AsString() != "http" {
			t.Errorf("namespace = %q; want %q", v.AsString(), "http")
		}
	})

	t.Run("absent-only: existing attribute not overwritten", func(t *testing.T) {
		// Datapoint already has namespace=preset; promotion must not overwrite it.
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("service.name", "svc")
		sm := rm.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName("http_requests")
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.Attributes().PutStr("namespace", "preset")
		dp.SetIntValue(1)

		plan := resolveTestPlan(t, []PartitionKeySource{
			{Source: keySourceMetricName, Regex: `^([a-z]+)_`, Promote: "namespace"},
		})

		batches := groupMetricsByLeaf(md, plan)
		if len(batches) != 1 {
			t.Fatalf("expected 1 batch, got %d", len(batches))
		}

		destDP := batches[0].batch.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0)
		v, ok := destDP.Attributes().Get("namespace")
		if !ok {
			t.Fatal("expected 'namespace' attribute")
		}
		if v.AsString() != "preset" {
			t.Errorf("namespace overwritten: got %q want %q", v.AsString(), "preset")
		}
	})

	t.Run("empty-skip: no-match regex writes no attribute", func(t *testing.T) {
		// Metric name "123x" doesn't match `^([a-z]+)_` → resolved value "" → no promotion
		md := namedMetrics("svc", []string{"123x"})
		plan := resolveTestPlan(t, []PartitionKeySource{
			{Source: keySourceMetricName, Regex: `^([a-z]+)_`, Promote: "namespace"},
		})

		batches := groupMetricsByLeaf(md, plan)
		if len(batches) != 1 {
			t.Fatalf("expected 1 batch, got %d", len(batches))
		}

		dp := batches[0].batch.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0)
		if _, ok := dp.Attributes().Get("namespace"); ok {
			t.Error("expected no 'namespace' attribute for no-match regex")
		}
	})

	t.Run("resource-source promotion writes to dest resource attrs", func(t *testing.T) {
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("service.name", "my-svc")
		sm := rm.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName("cpu")
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.Attributes().PutStr("instance", "inst-1")
		dp.SetIntValue(1)

		plan := resolveTestPlan(t, []PartitionKeySource{
			{Source: keySourceDatapoint, Name: "instance"},
			{Source: keySourceResource, Name: "service.name", Promote: "svc"},
		})

		batches := groupMetricsByLeaf(md, plan)
		if len(batches) != 1 {
			t.Fatalf("expected 1 batch, got %d", len(batches))
		}

		destRM := batches[0].batch.ResourceMetrics().At(0)
		v, ok := destRM.Resource().Attributes().Get("svc")
		if !ok {
			t.Fatal("expected 'svc' attribute on dest resource")
		}
		if v.AsString() != "my-svc" {
			t.Errorf("svc = %q; want %q", v.AsString(), "my-svc")
		}
	})
}

// --- test 6: input not mutated ---

func TestGroupMetricsByLeaf_InputNotMutated(t *testing.T) {
	instanceIDs := []string{"i1", "i2", "i3"}
	md := gaugeMetricsWithInstanceIDs("svc", "cpu.usage", instanceIDs)

	// Record original state before grouping.
	origDPCount := md.DataPointCount()
	origAttr := "i1" // first datapoint's instance

	plan := resolveTestPlan(t, []PartitionKeySource{
		{Source: keySourceDatapoint, Name: "instance", Promote: "instance_copy"},
	})

	_ = groupMetricsByLeaf(md, plan)

	// Datapoint count must be unchanged
	if md.DataPointCount() != origDPCount {
		t.Errorf("input DataPointCount changed: got %d want %d", md.DataPointCount(), origDPCount)
	}

	// Original first datapoint must not have the promoted attribute
	dp := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0)
	if _, ok := dp.Attributes().Get("instance_copy"); ok {
		t.Error("input datapoint has promoted attribute 'instance_copy' — input was mutated")
	}

	// Original instance must still be there untouched
	v, ok := dp.Attributes().Get("instance")
	if !ok {
		t.Fatal("original instance attribute missing from input")
	}
	if v.AsString() != origAttr {
		t.Errorf("original instance changed: got %q want %q", v.AsString(), origAttr)
	}
}

// --- test 7: all metric types exercised (ExponentialHistogram + Summary) ---

func TestGroupMetricsByLeaf_AllMetricTypes(t *testing.T) {
	// Build a batch with one datapoint each of all 5 metric types, keyed by instance.
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc")
	sm := rm.ScopeMetrics().AppendEmpty()

	// Gauge
	mGauge := sm.Metrics().AppendEmpty()
	mGauge.SetName("g")
	dpG := mGauge.SetEmptyGauge().DataPoints().AppendEmpty()
	dpG.Attributes().PutStr("instance", "i1")
	dpG.SetDoubleValue(1.0)

	// Sum
	mSum := sm.Metrics().AppendEmpty()
	mSum.SetName("s")
	sSum := mSum.SetEmptySum()
	sSum.SetIsMonotonic(true)
	sSum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dpS := sSum.DataPoints().AppendEmpty()
	dpS.Attributes().PutStr("instance", "i1")
	dpS.SetIntValue(2)

	// Histogram
	mHist := sm.Metrics().AppendEmpty()
	mHist.SetName("h")
	sHist := mHist.SetEmptyHistogram()
	sHist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dpH := sHist.DataPoints().AppendEmpty()
	dpH.Attributes().PutStr("instance", "i1")
	dpH.SetCount(3)

	// ExponentialHistogram
	mExpHist := sm.Metrics().AppendEmpty()
	mExpHist.SetName("eh")
	sExpHist := mExpHist.SetEmptyExponentialHistogram()
	sExpHist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dpEH := sExpHist.DataPoints().AppendEmpty()
	dpEH.Attributes().PutStr("instance", "i1")
	dpEH.SetCount(4)

	// Summary
	mSummary := sm.Metrics().AppendEmpty()
	mSummary.SetName("sum")
	dpSummary := mSummary.SetEmptySummary().DataPoints().AppendEmpty()
	dpSummary.Attributes().PutStr("instance", "i1")
	dpSummary.SetCount(5)

	plan := resolveTestPlan(t, []PartitionKeySource{
		{Source: keySourceDatapoint, Name: "instance"},
	})

	batches := groupMetricsByLeaf(md, plan)

	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	batch := batches[0].batch

	if batch.DataPointCount() != 5 {
		t.Errorf("expected 5 total datapoints across all types, got %d", batch.DataPointCount())
	}

	// Find each metric by name and verify type + settings
	found := map[string]pmetric.Metric{}
	rms := batch.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				found[m.Name()] = m
			}
		}
	}

	if m, ok := found["g"]; !ok {
		t.Error("Gauge metric 'g' missing")
	} else if m.Type() != pmetric.MetricTypeGauge {
		t.Errorf("'g' type: got %v want Gauge", m.Type())
	}

	if m, ok := found["s"]; !ok {
		t.Error("Sum metric 's' missing")
	} else {
		if m.Type() != pmetric.MetricTypeSum {
			t.Errorf("'s' type: got %v want Sum", m.Type())
		}
		if !m.Sum().IsMonotonic() {
			t.Error("'s': IsMonotonic should be true")
		}
		if m.Sum().AggregationTemporality() != pmetric.AggregationTemporalityCumulative {
			t.Errorf("'s': AggregationTemporality = %v; want Cumulative", m.Sum().AggregationTemporality())
		}
	}

	if m, ok := found["h"]; !ok {
		t.Error("Histogram metric 'h' missing")
	} else {
		if m.Type() != pmetric.MetricTypeHistogram {
			t.Errorf("'h' type: got %v want Histogram", m.Type())
		}
		if m.Histogram().AggregationTemporality() != pmetric.AggregationTemporalityDelta {
			t.Errorf("'h': AggregationTemporality = %v; want Delta", m.Histogram().AggregationTemporality())
		}
	}

	if m, ok := found["eh"]; !ok {
		t.Error("ExponentialHistogram metric 'eh' missing")
	} else {
		if m.Type() != pmetric.MetricTypeExponentialHistogram {
			t.Errorf("'eh' type: got %v want ExponentialHistogram", m.Type())
		}
		if m.ExponentialHistogram().AggregationTemporality() != pmetric.AggregationTemporalityDelta {
			t.Errorf("'eh': AggregationTemporality = %v; want Delta", m.ExponentialHistogram().AggregationTemporality())
		}
	}

	if m, ok := found["sum"]; !ok {
		t.Error("Summary metric 'sum' missing")
	} else if m.Type() != pmetric.MetricTypeSummary {
		t.Errorf("'sum' type: got %v want Summary", m.Type())
	}
}

// TestGroupMetricsByLeaf_ZeroDatapointMetricPreserved guards that a metric with
// no datapoints keeps its shell on the leaf path — the resource fast path
// preserves it via CopyTo, so the leaf path must not silently drop it.
func TestGroupMetricsByLeaf_ZeroDatapointMetricPreserved(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc")
	sm := rm.ScopeMetrics().AppendEmpty()

	// A monotonic cumulative Sum with ZERO datapoints (a legal pdata shape).
	mEmpty := sm.Metrics().AppendEmpty()
	mEmpty.SetName("empty.sum")
	es := mEmpty.SetEmptySum()
	es.SetIsMonotonic(true)
	es.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	// A gauge with one datapoint, so the batch is otherwise non-empty.
	mGauge := sm.Metrics().AppendEmpty()
	mGauge.SetName("g")
	dp := mGauge.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("instance", "i1")
	dp.SetIntValue(1)

	plan := resolveTestPlan(t, []PartitionKeySource{
		{Source: keySourceDatapoint, Name: "instance"},
	})

	batches := groupMetricsByLeaf(md, plan)

	// Locate the empty Sum across every emitted batch.
	var found pmetric.Metric
	dpTotal := 0
	for _, b := range batches {
		rms := b.batch.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					m := ms.At(k)
					dpTotal += totalDatapointsInMetric(m)
					if m.Name() == "empty.sum" {
						found = m
					}
				}
			}
		}
	}

	if found == (pmetric.Metric{}) {
		t.Fatal("empty.sum metric was dropped by the leaf path")
	}
	if found.Type() != pmetric.MetricTypeSum {
		t.Fatalf("empty.sum type: got %v want Sum", found.Type())
	}
	if !found.Sum().IsMonotonic() || found.Sum().AggregationTemporality() != pmetric.AggregationTemporalityCumulative {
		t.Error("empty.sum shell settings (monotonic/cumulative) not preserved")
	}
	if n := found.Sum().DataPoints().Len(); n != 0 {
		t.Errorf("empty.sum should have 0 datapoints, got %d", n)
	}
	if dpTotal != 1 {
		t.Errorf("total datapoints across batches: got %d want 1 (the gauge)", dpTotal)
	}
}
