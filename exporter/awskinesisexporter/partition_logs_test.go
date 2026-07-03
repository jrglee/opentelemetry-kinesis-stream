package awskinesisexporter

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

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
