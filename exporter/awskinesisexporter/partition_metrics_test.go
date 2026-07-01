package awskinesisexporter

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// --- builders ---

// gaugeMetricsWithInstanceIDs builds one ResourceMetrics with one Gauge metric
// named metricName. Each instanceID gets one datapoint with an int value equal
// to its position in the slice (0-indexed).
func gaugeMetricsWithInstanceIDs(svcName, metricName string, instanceIDs []string) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", svcName)
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName(metricName)
	g := m.SetEmptyGauge()
	for i, iid := range instanceIDs {
		dp := g.DataPoints().AppendEmpty()
		dp.Attributes().PutStr("instance", iid)
		dp.SetIntValue(int64(i))
	}
	return md
}

// namedMetrics builds one ResourceMetrics with N Gauge metrics, each with a
// single datapoint. Resource carries service.name=svcName.
func namedMetrics(svcName string, metricNames []string) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", svcName)
	sm := rm.ScopeMetrics().AppendEmpty()
	for _, name := range metricNames {
		m := sm.Metrics().AppendEmpty()
		m.SetName(name)
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetIntValue(1)
	}
	return md
}

// resolveTestPlan is a convenience wrapper for tests: build a tag_hash Config
// with the given Keys, call resolveKeyPlan, and return the plan.
func resolveTestPlan(t *testing.T, keys []PartitionKeySource) keyPlan {
	t.Helper()
	cfg := baseValidCfg()
	cfg.PartitionKey = PartitionKeyConfig{
		Strategy: partitionStrategyTagHash,
		Keys:     keys,
		Hash:     hashXXHash,
	}
	plan, err := cfg.resolveKeyPlan()
	if err != nil {
		t.Fatalf("resolveKeyPlan: %v", err)
	}
	return plan
}

// totalDataPoints sums datapoints across all metrics in all ResourceMetrics.
func totalDataPoints(md pmetric.Metrics) int {
	return md.DataPointCount()
}

// --- test 1: datapoint source grouping ---

func TestGroupMetricsByLeaf_DatapointSource(t *testing.T) {
	// i1 appears twice, i2 once, i3 once → 3 batches
	instanceIDs := []string{"i1", "i1", "i2", "i3"}
	md := gaugeMetricsWithInstanceIDs("svc", "cpu.usage", instanceIDs)

	plan := resolveTestPlan(t, []PartitionKeySource{
		{Source: keySourceDatapoint, Name: "instance"},
	})

	batches := groupMetricsByLeaf(md, plan)

	if len(batches) != 3 {
		t.Fatalf("expected 3 batches (i1,i2,i3), got %d", len(batches))
	}

	// Collect by instance
	batchByInstance := map[string]pmetric.Metrics{}
	for _, b := range batches {
		// Each batch has one ResourceMetrics, one ScopeMetrics, one Metric.
		rm := b.batch.ResourceMetrics().At(0)
		m := rm.ScopeMetrics().At(0).Metrics().At(0)
		dps := m.Gauge().DataPoints()
		// All datapoints in this batch should share the same instance
		if dps.Len() == 0 {
			t.Fatalf("batch key=%q has no datapoints", b.key)
		}
		iid, _ := dps.At(0).Attributes().Get("instance")
		batchByInstance[iid.AsString()] = b.batch
	}

	// i1 should have 2 datapoints
	if b, ok := batchByInstance["i1"]; !ok {
		t.Fatal("no batch for i1")
	} else {
		m := b.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
		if m.Gauge().DataPoints().Len() != 2 {
			t.Errorf("i1: expected 2 datapoints, got %d", m.Gauge().DataPoints().Len())
		}
	}

	// i2 and i3 should each have 1 datapoint
	for _, inst := range []string{"i2", "i3"} {
		if b, ok := batchByInstance[inst]; !ok {
			t.Fatalf("no batch for %s", inst)
		} else {
			m := b.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
			if m.Gauge().DataPoints().Len() != 1 {
				t.Errorf("%s: expected 1 datapoint, got %d", inst, m.Gauge().DataPoints().Len())
			}
		}
	}

	// Total datapoints across batches == input
	total := 0
	for _, b := range batches {
		total += totalDataPoints(b.batch)
	}
	if total != len(instanceIDs) {
		t.Errorf("total datapoints across batches: got %d want %d", total, len(instanceIDs))
	}

	// Keys stable: same instance → same joined key across two groupings
	batches2 := groupMetricsByLeaf(md, plan)
	keyByInstance := map[string]string{}
	for _, b := range batches {
		rm := b.batch.ResourceMetrics().At(0)
		m := rm.ScopeMetrics().At(0).Metrics().At(0)
		iid, _ := m.Gauge().DataPoints().At(0).Attributes().Get("instance")
		keyByInstance[iid.AsString()] = b.key
	}
	for _, b := range batches2 {
		rm := b.batch.ResourceMetrics().At(0)
		m := rm.ScopeMetrics().At(0).Metrics().At(0)
		iid, _ := m.Gauge().DataPoints().At(0).Attributes().Get("instance")
		if keyByInstance[iid.AsString()] != b.key {
			t.Errorf("key for instance %s changed between runs: %q vs %q",
				iid.AsString(), keyByInstance[iid.AsString()], b.key)
		}
	}
}

// --- test 2: metric_name + regex grouping ---

func TestGroupMetricsByLeaf_MetricNameRegex(t *testing.T) {
	// foo_a, foo_b → bucket "foo"; bar_c → bucket "bar"; 123x → no match → bucket ""
	md := namedMetrics("svc", []string{"foo_a", "foo_b", "bar_c", "123x"})

	plan := resolveTestPlan(t, []PartitionKeySource{
		{Source: keySourceMetricName, Regex: `^([a-z]+)_`},
	})

	batches := groupMetricsByLeaf(md, plan)

	// 3 buckets: "foo", "bar", ""
	if len(batches) != 3 {
		t.Fatalf("expected 3 batches, got %d: keys=%v", len(batches), batchKeys(batches))
	}

	byKey := map[string]pmetric.Metrics{}
	for _, b := range batches {
		byKey[b.key] = b.batch
	}

	// "foo" bucket: contains two SEPARATE metrics (foo_a and foo_b)
	fooBatch, ok := byKey["foo"]
	if !ok {
		t.Fatal("no batch for key 'foo'")
	}
	fooMetricCount := countMetrics(fooBatch)
	if fooMetricCount != 2 {
		t.Errorf("foo bucket: expected 2 metrics (foo_a and foo_b), got %d", fooMetricCount)
	}

	// "bar" bucket: 1 metric
	barBatch, ok := byKey["bar"]
	if !ok {
		t.Fatal("no batch for key 'bar'")
	}
	if countMetrics(barBatch) != 1 {
		t.Errorf("bar bucket: expected 1 metric, got %d", countMetrics(barBatch))
	}

	// "" bucket: 1 metric (123x didn't match)
	emptyBatch, ok := byKey[""]
	if !ok {
		t.Fatal("no batch for key ''")
	}
	if countMetrics(emptyBatch) != 1 {
		t.Errorf("empty-key bucket: expected 1 metric, got %d", countMetrics(emptyBatch))
	}

	// Whole-match fallback: pattern with no capture group → whole match
	planNoCapture := resolveTestPlan(t, []PartitionKeySource{
		{Source: keySourceMetricName, Regex: `^[a-z]+`},
	})
	batches2 := groupMetricsByLeaf(md, planNoCapture)
	byKey2 := map[string]pmetric.Metrics{}
	for _, b := range batches2 {
		byKey2[b.key] = b.batch
	}
	// foo_a, foo_b → whole match "foo"; bar_c → "bar"; 123x → ""
	if _, ok := byKey2["foo"]; !ok {
		t.Error("no-capture-group: expected 'foo' bucket")
	}
	if countMetrics(byKey2["foo"]) != 2 {
		t.Errorf("no-capture-group foo bucket: expected 2 metrics, got %d", countMetrics(byKey2["foo"]))
	}
}

// --- test 3: mixed ordered plan with 3 segments ---

func TestGroupMetricsByLeaf_MixedOrderedPlan(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "my-svc")
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("http_requests")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("instance", "inst-42")
	dp.SetIntValue(1)

	plan := resolveTestPlan(t, []PartitionKeySource{
		{Source: keySourceResource, Name: "service.name"},
		{Source: keySourceDatapoint, Name: "instance"},
		{Source: keySourceMetricName, Regex: `^([a-z]+)_`},
	})

	batches := groupMetricsByLeaf(md, plan)

	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}

	key := batches[0].key
	parts := splitKey(key)
	if len(parts) != 3 {
		t.Fatalf("expected 3 key segments, got %d: %q", len(parts), key)
	}
	if parts[0] != "my-svc" {
		t.Errorf("segment[0] (service.name): got %q want %q", parts[0], "my-svc")
	}
	if parts[1] != "inst-42" {
		t.Errorf("segment[1] (instance): got %q want %q", parts[1], "inst-42")
	}
	if parts[2] != "http" {
		t.Errorf("segment[2] (metric_name regex): got %q want %q", parts[2], "http")
	}
}

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

// --- helpers used by tests above ---

func batchKeys[T any](batches []taggedBatch[T]) []string {
	keys := make([]string, len(batches))
	for i, b := range batches {
		keys[i] = b.key
	}
	return keys
}

func countMetrics(md pmetric.Metrics) int {
	n := 0
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			n += sms.At(j).Metrics().Len()
		}
	}
	return n
}

// splitKey splits a joined key by tagSep for inspection.
func splitKey(key string) []string {
	// tagSep is "\x1f" — split manually so tests don't depend on strings package
	var parts []string
	start := 0
	for i := 0; i < len(key); i++ {
		if key[i] == '\x1f' {
			parts = append(parts, key[start:i])
			start = i + 1
		}
	}
	parts = append(parts, key[start:])
	return parts
}

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

// Silence the unused-function linter for totalDatapointsInMetric in case
// some tests use it via inline assertions.
var _ = totalDatapointsInMetric
