package awskinesisexporter

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"
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

// --- batch/key helpers used by grouping tests ---

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
