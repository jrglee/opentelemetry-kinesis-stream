package awskinesisexporter

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// ---- trace builders ----

// makeTraces builds a ptrace.Traces with one resource carrying resAttrs.
// spans is a list of (spanName, leafAttrs) pairs — each pair becomes one span
// under a single ScopeSpans envelope on that resource.
func makeTraces(resAttrs map[string]string, spans []spanSpec) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	for k, v := range resAttrs {
		rs.Resource().Attributes().PutStr(k, v)
	}
	ss := rs.ScopeSpans().AppendEmpty()
	for _, sp := range spans {
		dst := ss.Spans().AppendEmpty()
		dst.SetName(sp.name)
		for k, v := range sp.attrs {
			dst.Attributes().PutStr(k, v)
		}
	}
	return td
}

type spanSpec struct {
	name  string
	attrs map[string]string
}

// makeMultiScopeTraces builds a ptrace.Traces with one resource and two scopes,
// each holding one span, to exercise scope identity preservation.
func makeMultiScopeTraces(resAttrs map[string]string, scope0Span, scope1Span spanSpec) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	for k, v := range resAttrs {
		rs.Resource().Attributes().PutStr(k, v)
	}
	ss0 := rs.ScopeSpans().AppendEmpty()
	ss0.Scope().SetName("scope-0")
	ss0.SetSchemaUrl("https://schema/0")
	dst0 := ss0.Spans().AppendEmpty()
	dst0.SetName(scope0Span.name)
	for k, v := range scope0Span.attrs {
		dst0.Attributes().PutStr(k, v)
	}
	ss1 := rs.ScopeSpans().AppendEmpty()
	ss1.Scope().SetName("scope-1")
	ss1.SetSchemaUrl("https://schema/1")
	dst1 := ss1.Spans().AppendEmpty()
	dst1.SetName(scope1Span.name)
	for k, v := range scope1Span.attrs {
		dst1.Attributes().PutStr(k, v)
	}
	return td
}

// ---- log builders ----

type logSpec struct {
	body  string
	attrs map[string]string
}

func makeLogs(resAttrs map[string]string, records []logSpec) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	for k, v := range resAttrs {
		rl.Resource().Attributes().PutStr(k, v)
	}
	sl := rl.ScopeLogs().AppendEmpty()
	for _, lr := range records {
		dst := sl.LogRecords().AppendEmpty()
		dst.Body().SetStr(lr.body)
		for k, v := range lr.attrs {
			dst.Attributes().PutStr(k, v)
		}
	}
	return ld
}

func makeMultiScopeLogs(resAttrs map[string]string, scope0Log, scope1Log logSpec) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	for k, v := range resAttrs {
		rl.Resource().Attributes().PutStr(k, v)
	}
	sl0 := rl.ScopeLogs().AppendEmpty()
	sl0.Scope().SetName("scope-0")
	sl0.SetSchemaUrl("https://schema/0")
	r0 := sl0.LogRecords().AppendEmpty()
	r0.Body().SetStr(scope0Log.body)
	for k, v := range scope0Log.attrs {
		r0.Attributes().PutStr(k, v)
	}
	sl1 := rl.ScopeLogs().AppendEmpty()
	sl1.Scope().SetName("scope-1")
	sl1.SetSchemaUrl("https://schema/1")
	r1 := sl1.LogRecords().AppendEmpty()
	r1.Body().SetStr(scope1Log.body)
	for k, v := range scope1Log.attrs {
		r1.Attributes().PutStr(k, v)
	}
	return ld
}

// ---- plan helpers ----

func leafPlan(name string) keyPlan {
	return keyPlan{{source: keySourceDatapoint, name: name}}
}

func resourceAndLeafPlan(resName, leafName string) keyPlan {
	return keyPlan{
		{source: keySourceResource, name: resName},
		{source: keySourceDatapoint, name: leafName},
	}
}

func leafPromotePlan(name, promote string) keyPlan {
	return keyPlan{{source: keySourceDatapoint, name: name, promote: promote}}
}

func resourcePromotePlan(name, promote string) keyPlan {
	return keyPlan{{source: keySourceResource, name: name, promote: promote}}
}

func metricNamePromotePlan(promote string) keyPlan {
	return keyPlan{{source: keySourceMetricName, promote: promote}}
}

// ---- TestGroupTracesByLeaf ----

// TestTracesByLeafDatapointSource: one resource, N spans with mixed instance
// span attributes → N batches, each holding only its instance's spans.
func TestTracesByLeafDatapointSource(t *testing.T) {
	td := makeTraces(
		map[string]string{"service.name": "svc"},
		[]spanSpec{
			{name: "s1", attrs: map[string]string{"instance": "i1"}},
			{name: "s2", attrs: map[string]string{"instance": "i2"}},
			{name: "s3", attrs: map[string]string{"instance": "i1"}},
			{name: "s4", attrs: map[string]string{"instance": "i3"}},
		},
	)
	plan := leafPlan("instance")
	batches := groupTracesByLeaf(td, plan)

	if len(batches) != 3 {
		t.Fatalf("expected 3 batches (i1, i2, i3), got %d", len(batches))
	}

	// First-seen order: i1, i2, i3.
	wantKeys := []string{"i1", "i2", "i3"}
	for i, want := range wantKeys {
		if batches[i].key != want {
			t.Errorf("batch[%d].key = %q, want %q", i, batches[i].key, want)
		}
	}

	// Total spans across batches must equal input.
	total := 0
	for _, b := range batches {
		total += b.batch.SpanCount()
	}
	if total != 4 {
		t.Fatalf("total spans: got %d want 4", total)
	}

	// i1 has 2 spans; i2 and i3 each have 1.
	counts := map[string]int{"i1": 2, "i2": 1, "i3": 1}
	for _, b := range batches {
		key := b.key
		got := b.batch.SpanCount()
		if got != counts[key] {
			t.Errorf("batch key=%q: span count %d want %d", key, got, counts[key])
		}
	}
}

// TestTracesByLeafResourceSourceViaLeafPath: plan with resource + datapoint
// sources → key is joined with separator; scope identity preserved.
func TestTracesByLeafResourceSourceViaLeafPath(t *testing.T) {
	td := makeMultiScopeTraces(
		map[string]string{"service.name": "web"},
		spanSpec{name: "scope0-span", attrs: map[string]string{"instance": "i1"}},
		spanSpec{name: "scope1-span", attrs: map[string]string{"instance": "i2"}},
	)
	plan := resourceAndLeafPlan("service.name", "instance")
	batches := groupTracesByLeaf(td, plan)

	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}

	wantKeys := []string{"web\x1fi1", "web\x1fi2"}
	for i, want := range wantKeys {
		if batches[i].key != want {
			t.Errorf("batch[%d].key = %q, want %q", i, batches[i].key, want)
		}
	}

	// Verify resource + scope identity carried into dest.
	for _, b := range batches {
		rs := b.batch.ResourceSpans()
		if rs.Len() != 1 {
			t.Fatalf("expected 1 ResourceSpans, got %d", rs.Len())
		}
		svc, ok := rs.At(0).Resource().Attributes().Get("service.name")
		if !ok || svc.AsString() != "web" {
			t.Errorf("resource service.name missing or wrong in batch %q", b.key)
		}
	}

	// Each batch holds one span from its matching scope.
	for _, b := range batches {
		if b.batch.SpanCount() != 1 {
			t.Errorf("batch %q: expected 1 span, got %d", b.key, b.batch.SpanCount())
		}
	}
}

// TestTracesByLeafPromotion covers:
// a) datapoint-source promote → dest span attribute present
// b) absent-only (pre-existing attr kept)
// c) empty-skip (missing source attr → "" → not written)
// d) resource-source promote → dest resource attribute present
func TestTracesByLeafPromotion(t *testing.T) {
	t.Run("datapoint_promote_written", func(t *testing.T) {
		td := makeTraces(
			map[string]string{"service.name": "svc"},
			[]spanSpec{
				{name: "span", attrs: map[string]string{"instance": "i1"}},
			},
		)
		plan := leafPromotePlan("instance", "promoted.instance")
		batches := groupTracesByLeaf(td, plan)
		if len(batches) != 1 {
			t.Fatalf("expected 1 batch, got %d", len(batches))
		}
		span := batches[0].batch.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		v, ok := span.Attributes().Get("promoted.instance")
		if !ok {
			t.Fatal("promoted.instance attribute missing from span")
		}
		if v.AsString() != "i1" {
			t.Errorf("promoted.instance = %q, want %q", v.AsString(), "i1")
		}
	})

	t.Run("absent_only_keeps_existing", func(t *testing.T) {
		td := makeTraces(
			map[string]string{"service.name": "svc"},
			[]spanSpec{
				{name: "span", attrs: map[string]string{"instance": "i1", "promoted.instance": "existing"}},
			},
		)
		plan := leafPromotePlan("instance", "promoted.instance")
		batches := groupTracesByLeaf(td, plan)
		span := batches[0].batch.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		v, ok := span.Attributes().Get("promoted.instance")
		if !ok {
			t.Fatal("promoted.instance attribute missing")
		}
		if v.AsString() != "existing" {
			t.Errorf("absent-only violated: got %q, want %q (existing)", v.AsString(), "existing")
		}
	})

	t.Run("empty_skip_missing_source", func(t *testing.T) {
		td := makeTraces(
			map[string]string{"service.name": "svc"},
			[]spanSpec{
				{name: "span", attrs: map[string]string{}}, // no instance
			},
		)
		plan := leafPromotePlan("instance", "promoted.instance")
		batches := groupTracesByLeaf(td, plan)
		span := batches[0].batch.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		if _, ok := span.Attributes().Get("promoted.instance"); ok {
			t.Error("promoted.instance should NOT be written when source is missing")
		}
	})

	t.Run("resource_source_promote_to_resource", func(t *testing.T) {
		td := makeTraces(
			map[string]string{"service.name": "svc"},
			[]spanSpec{
				{name: "span", attrs: map[string]string{"instance": "i1"}},
			},
		)
		plan := keyPlan{
			{source: keySourceResource, name: "service.name", promote: "promoted.svc"},
		}
		batches := groupTracesByLeaf(td, plan)
		resAttrs := batches[0].batch.ResourceSpans().At(0).Resource().Attributes()
		v, ok := resAttrs.Get("promoted.svc")
		if !ok {
			t.Fatal("promoted.svc resource attribute missing")
		}
		if v.AsString() != "svc" {
			t.Errorf("promoted.svc = %q, want %q", v.AsString(), "svc")
		}
	})
}

// TestTracesByLeafInputNotMutated verifies that the source ptrace.Traces is
// unchanged after grouping.
func TestTracesByLeafInputNotMutated(t *testing.T) {
	td := makeTraces(
		map[string]string{"service.name": "svc"},
		[]spanSpec{
			{name: "span1", attrs: map[string]string{"instance": "i1"}},
			{name: "span2", attrs: map[string]string{"instance": "i2"}},
		},
	)
	// Snapshot span count before.
	before := td.SpanCount()
	// Snapshot first span's attribute count.
	beforeAttrs := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().Len()

	plan := leafPromotePlan("instance", "promoted.instance")
	_ = groupTracesByLeaf(td, plan)

	if td.SpanCount() != before {
		t.Errorf("input SpanCount changed: %d → %d", before, td.SpanCount())
	}
	afterAttrs := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().Len()
	if afterAttrs != beforeAttrs {
		t.Errorf("input span[0].Attributes().Len() changed: %d → %d", beforeAttrs, afterAttrs)
	}
}

// TestTracesByLeafMetricNameSourceInert: a plan containing metric_name source
// resolves to "" and writes nothing / contributes an empty key segment.
func TestTracesByLeafMetricNameSourceInert(t *testing.T) {
	td := makeTraces(
		map[string]string{"service.name": "svc"},
		[]spanSpec{
			{name: "span", attrs: map[string]string{"instance": "i1"}},
		},
	)
	plan := metricNamePromotePlan("namespace")
	batches := groupTracesByLeaf(td, plan)

	// One batch with empty key (metric_name resolves to "" for traces).
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	if batches[0].key != "" {
		t.Errorf("key = %q, want empty (metric_name is inert for traces)", batches[0].key)
	}
	// Promote target must not be written (resolved to "").
	span := batches[0].batch.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	if _, ok := span.Attributes().Get("namespace"); ok {
		t.Error("namespace should NOT be promoted (source resolved to empty string)")
	}
}

// TestTracesByLeafRoundTrip marshals and unmarshals each batch, verifying the
// full span payload survives the proto encoding round trip.
func TestTracesByLeafRoundTrip(t *testing.T) {
	td := makeTraces(
		map[string]string{"service.name": "svc"},
		[]spanSpec{
			{name: "span-a", attrs: map[string]string{"instance": "i1"}},
			{name: "span-b", attrs: map[string]string{"instance": "i2"}},
		},
	)
	plan := leafPlan("instance")
	batches := groupTracesByLeaf(td, plan)
	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}

	enc, err := encoding.NewTracesEncoder(encoding.EncodingOTLPProto)
	if err != nil {
		t.Fatalf("NewTracesEncoder: %v", err)
	}
	dec, err := encoding.NewTracesDecoder(encoding.EncodingOTLPProto)
	if err != nil {
		t.Fatalf("NewTracesDecoder: %v", err)
	}

	wantNames := map[string]string{"i1": "span-a", "i2": "span-b"}
	for _, b := range batches {
		raw, err := enc.Marshal(b.batch)
		if err != nil {
			t.Fatalf("marshal batch %q: %v", b.key, err)
		}
		got, err := dec.Unmarshal(raw)
		if err != nil {
			t.Fatalf("unmarshal batch %q: %v", b.key, err)
		}
		if got.SpanCount() != 1 {
			t.Errorf("batch %q: span count %d want 1", b.key, got.SpanCount())
		}
		name := got.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name()
		if name != wantNames[b.key] {
			t.Errorf("batch %q: span name %q want %q", b.key, name, wantNames[b.key])
		}
	}
}

// ---- TestGroupLogsByLeaf ----

// TestLogsByLeafDatapointSource: one resource, N log records with mixed
// instance attributes → N batches, each holding only its instance's records.
func TestLogsByLeafDatapointSource(t *testing.T) {
	ld := makeLogs(
		map[string]string{"service.name": "svc"},
		[]logSpec{
			{body: "r1", attrs: map[string]string{"instance": "i1"}},
			{body: "r2", attrs: map[string]string{"instance": "i2"}},
			{body: "r3", attrs: map[string]string{"instance": "i1"}},
			{body: "r4", attrs: map[string]string{"instance": "i3"}},
		},
	)
	plan := leafPlan("instance")
	batches := groupLogsByLeaf(ld, plan)

	if len(batches) != 3 {
		t.Fatalf("expected 3 batches, got %d", len(batches))
	}

	// First-seen order: i1, i2, i3.
	wantKeys := []string{"i1", "i2", "i3"}
	for i, want := range wantKeys {
		if batches[i].key != want {
			t.Errorf("batch[%d].key = %q, want %q", i, batches[i].key, want)
		}
	}

	total := 0
	for _, b := range batches {
		total += b.batch.LogRecordCount()
	}
	if total != 4 {
		t.Fatalf("total log records: got %d want 4", total)
	}

	counts := map[string]int{"i1": 2, "i2": 1, "i3": 1}
	for _, b := range batches {
		got := b.batch.LogRecordCount()
		if got != counts[b.key] {
			t.Errorf("batch key=%q: log record count %d want %d", b.key, got, counts[b.key])
		}
	}
}

// TestLogsByLeafResourceSourceViaLeafPath: resource + datapoint plan on logs.
func TestLogsByLeafResourceSourceViaLeafPath(t *testing.T) {
	ld := makeMultiScopeLogs(
		map[string]string{"service.name": "web"},
		logSpec{body: "scope0-log", attrs: map[string]string{"instance": "i1"}},
		logSpec{body: "scope1-log", attrs: map[string]string{"instance": "i2"}},
	)
	plan := resourceAndLeafPlan("service.name", "instance")
	batches := groupLogsByLeaf(ld, plan)

	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}

	wantKeys := []string{"web\x1fi1", "web\x1fi2"}
	for i, want := range wantKeys {
		if batches[i].key != want {
			t.Errorf("batch[%d].key = %q, want %q", i, batches[i].key, want)
		}
	}

	for _, b := range batches {
		rl := b.batch.ResourceLogs()
		if rl.Len() != 1 {
			t.Fatalf("expected 1 ResourceLogs, got %d", rl.Len())
		}
		svc, ok := rl.At(0).Resource().Attributes().Get("service.name")
		if !ok || svc.AsString() != "web" {
			t.Errorf("resource service.name missing or wrong in batch %q", b.key)
		}
		if b.batch.LogRecordCount() != 1 {
			t.Errorf("batch %q: expected 1 log record, got %d", b.key, b.batch.LogRecordCount())
		}
	}
}

// TestLogsByLeafPromotion covers the same four sub-cases as traces.
func TestLogsByLeafPromotion(t *testing.T) {
	t.Run("datapoint_promote_written", func(t *testing.T) {
		ld := makeLogs(
			map[string]string{"service.name": "svc"},
			[]logSpec{
				{body: "r", attrs: map[string]string{"instance": "i1"}},
			},
		)
		plan := leafPromotePlan("instance", "promoted.instance")
		batches := groupLogsByLeaf(ld, plan)
		lr := batches[0].batch.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
		v, ok := lr.Attributes().Get("promoted.instance")
		if !ok {
			t.Fatal("promoted.instance attribute missing from log record")
		}
		if v.AsString() != "i1" {
			t.Errorf("promoted.instance = %q, want %q", v.AsString(), "i1")
		}
	})

	t.Run("absent_only_keeps_existing", func(t *testing.T) {
		ld := makeLogs(
			map[string]string{"service.name": "svc"},
			[]logSpec{
				{body: "r", attrs: map[string]string{"instance": "i1", "promoted.instance": "existing"}},
			},
		)
		plan := leafPromotePlan("instance", "promoted.instance")
		batches := groupLogsByLeaf(ld, plan)
		lr := batches[0].batch.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
		v, ok := lr.Attributes().Get("promoted.instance")
		if !ok {
			t.Fatal("promoted.instance attribute missing")
		}
		if v.AsString() != "existing" {
			t.Errorf("absent-only violated: got %q want %q", v.AsString(), "existing")
		}
	})

	t.Run("empty_skip_missing_source", func(t *testing.T) {
		ld := makeLogs(
			map[string]string{"service.name": "svc"},
			[]logSpec{
				{body: "r", attrs: map[string]string{}}, // no instance
			},
		)
		plan := leafPromotePlan("instance", "promoted.instance")
		batches := groupLogsByLeaf(ld, plan)
		lr := batches[0].batch.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
		if _, ok := lr.Attributes().Get("promoted.instance"); ok {
			t.Error("promoted.instance should NOT be written when source is missing")
		}
	})

	t.Run("resource_source_promote_to_resource", func(t *testing.T) {
		ld := makeLogs(
			map[string]string{"service.name": "svc"},
			[]logSpec{
				{body: "r", attrs: map[string]string{"instance": "i1"}},
			},
		)
		plan := resourcePromotePlan("service.name", "promoted.svc")
		batches := groupLogsByLeaf(ld, plan)
		resAttrs := batches[0].batch.ResourceLogs().At(0).Resource().Attributes()
		v, ok := resAttrs.Get("promoted.svc")
		if !ok {
			t.Fatal("promoted.svc resource attribute missing")
		}
		if v.AsString() != "svc" {
			t.Errorf("promoted.svc = %q, want %q", v.AsString(), "svc")
		}
	})
}

// TestLogsByLeafInputNotMutated verifies the source plog.Logs is unchanged.
func TestLogsByLeafInputNotMutated(t *testing.T) {
	ld := makeLogs(
		map[string]string{"service.name": "svc"},
		[]logSpec{
			{body: "r1", attrs: map[string]string{"instance": "i1"}},
			{body: "r2", attrs: map[string]string{"instance": "i2"}},
		},
	)
	before := ld.LogRecordCount()
	beforeAttrs := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes().Len()

	plan := leafPromotePlan("instance", "promoted.instance")
	_ = groupLogsByLeaf(ld, plan)

	if ld.LogRecordCount() != before {
		t.Errorf("input LogRecordCount changed: %d → %d", before, ld.LogRecordCount())
	}
	afterAttrs := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes().Len()
	if afterAttrs != beforeAttrs {
		t.Errorf("input log[0].Attributes().Len() changed: %d → %d", beforeAttrs, afterAttrs)
	}
}

// TestLogsByLeafMetricNameSourceInert: metric_name resolves to "" for logs.
func TestLogsByLeafMetricNameSourceInert(t *testing.T) {
	ld := makeLogs(
		map[string]string{"service.name": "svc"},
		[]logSpec{
			{body: "r", attrs: map[string]string{"instance": "i1"}},
		},
	)
	plan := metricNamePromotePlan("namespace")
	batches := groupLogsByLeaf(ld, plan)

	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	if batches[0].key != "" {
		t.Errorf("key = %q, want empty (metric_name is inert for logs)", batches[0].key)
	}
	lr := batches[0].batch.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	if _, ok := lr.Attributes().Get("namespace"); ok {
		t.Error("namespace should NOT be promoted (source resolved to empty string)")
	}
}

// TestLogsByLeafRoundTrip marshals and unmarshals each batch via OTLP proto.
func TestLogsByLeafRoundTrip(t *testing.T) {
	ld := makeLogs(
		map[string]string{"service.name": "svc"},
		[]logSpec{
			{body: "log-a", attrs: map[string]string{"instance": "i1"}},
			{body: "log-b", attrs: map[string]string{"instance": "i2"}},
		},
	)
	plan := leafPlan("instance")
	batches := groupLogsByLeaf(ld, plan)
	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}

	enc, err := encoding.NewLogsEncoder(encoding.EncodingOTLPProto)
	if err != nil {
		t.Fatalf("NewLogsEncoder: %v", err)
	}
	dec, err := encoding.NewLogsDecoder(encoding.EncodingOTLPProto)
	if err != nil {
		t.Fatalf("NewLogsDecoder: %v", err)
	}

	wantBodies := map[string]string{"i1": "log-a", "i2": "log-b"}
	for _, b := range batches {
		raw, err := enc.Marshal(b.batch)
		if err != nil {
			t.Fatalf("marshal batch %q: %v", b.key, err)
		}
		got, err := dec.Unmarshal(raw)
		if err != nil {
			t.Fatalf("unmarshal batch %q: %v", b.key, err)
		}
		if got.LogRecordCount() != 1 {
			t.Errorf("batch %q: log record count %d want 1", b.key, got.LogRecordCount())
		}
		body := got.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str()
		if body != wantBodies[b.key] {
			t.Errorf("batch %q: body %q want %q", b.key, body, wantBodies[b.key])
		}
	}
}

// TestTracesByLeafScopeIdentityPreserved checks schema URL and scope name are
// carried into the dest bucket when keys differ across scopes.
func TestTracesByLeafScopeIdentityPreserved(t *testing.T) {
	td := makeMultiScopeTraces(
		map[string]string{"service.name": "web"},
		spanSpec{name: "s0", attrs: map[string]string{"instance": "i1"}},
		spanSpec{name: "s1", attrs: map[string]string{"instance": "i2"}},
	)
	plan := leafPlan("instance")
	batches := groupTracesByLeaf(td, plan)

	for _, b := range batches {
		rs := b.batch.ResourceSpans().At(0)
		if rs.ScopeSpans().Len() != 1 {
			t.Errorf("batch %q: expected 1 ScopeSpans, got %d", b.key, rs.ScopeSpans().Len())
			continue
		}
		ss := rs.ScopeSpans().At(0)
		switch b.key {
		case "i1":
			if ss.Scope().Name() != "scope-0" {
				t.Errorf("batch i1: scope name %q want scope-0", ss.Scope().Name())
			}
			if ss.SchemaUrl() != "https://schema/0" {
				t.Errorf("batch i1: schema URL %q want https://schema/0", ss.SchemaUrl())
			}
		case "i2":
			if ss.Scope().Name() != "scope-1" {
				t.Errorf("batch i2: scope name %q want scope-1", ss.Scope().Name())
			}
			if ss.SchemaUrl() != "https://schema/1" {
				t.Errorf("batch i2: schema URL %q want https://schema/1", ss.SchemaUrl())
			}
		}
	}
}

// TestLogsByLeafScopeIdentityPreserved mirrors the trace scope identity test.
func TestLogsByLeafScopeIdentityPreserved(t *testing.T) {
	ld := makeMultiScopeLogs(
		map[string]string{"service.name": "web"},
		logSpec{body: "l0", attrs: map[string]string{"instance": "i1"}},
		logSpec{body: "l1", attrs: map[string]string{"instance": "i2"}},
	)
	plan := leafPlan("instance")
	batches := groupLogsByLeaf(ld, plan)

	for _, b := range batches {
		rl := b.batch.ResourceLogs().At(0)
		if rl.ScopeLogs().Len() != 1 {
			t.Errorf("batch %q: expected 1 ScopeLogs, got %d", b.key, rl.ScopeLogs().Len())
			continue
		}
		sl := rl.ScopeLogs().At(0)
		switch b.key {
		case "i1":
			if sl.Scope().Name() != "scope-0" {
				t.Errorf("batch i1: scope name %q want scope-0", sl.Scope().Name())
			}
			if sl.SchemaUrl() != "https://schema/0" {
				t.Errorf("batch i1: schema URL %q want https://schema/0", sl.SchemaUrl())
			}
		case "i2":
			if sl.Scope().Name() != "scope-1" {
				t.Errorf("batch i2: scope name %q want scope-1", sl.Scope().Name())
			}
			if sl.SchemaUrl() != "https://schema/1" {
				t.Errorf("batch i2: schema URL %q want https://schema/1", sl.SchemaUrl())
			}
		}
	}
}

// TestTracesByLeafStableKeysPerInstance verifies key stability for repeat calls
// (same instance → same key string each time).
func TestTracesByLeafStableKeysPerInstance(t *testing.T) {
	td := makeTraces(
		map[string]string{"service.name": "svc"},
		[]spanSpec{
			{name: "s", attrs: map[string]string{"instance": "i1"}},
		},
	)
	plan := leafPlan("instance")
	b1 := groupTracesByLeaf(td, plan)
	b2 := groupTracesByLeaf(td, plan)

	if b1[0].key != b2[0].key {
		t.Errorf("key unstable across calls: %q vs %q", b1[0].key, b2[0].key)
	}
}
