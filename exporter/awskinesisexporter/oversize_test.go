package awskinesisexporter

import (
	"context"
	"testing"
	"unicode/utf8"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// sumDropped extracts the kinesis.exporter.records_dropped counter total.
func sumDropped(t *testing.T, rm *metricdata.ResourceMetrics) int64 {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "kinesis.exporter.records_dropped" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("unexpected data type %T", m.Data)
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	return 0
}

// sumByReason returns the per-reason counter values for the named instrument.
func sumByReason(t *testing.T, rm *metricdata.ResourceMetrics, name string) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s: unexpected data type %T", name, m.Data)
			}
			for _, dp := range sum.DataPoints {
				reason, _ := dp.Attributes.Value("reason")
				out[reason.AsString()] += dp.Value
			}
		}
	}
	return out
}

// sumInt64Counter returns the total value of an unlabeled int64 counter.
func sumInt64Counter(t *testing.T, rm *metricdata.ResourceMetrics, name string) int64 {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s: unexpected data type %T", name, m.Data)
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	return 0
}

func TestOversizeSplitPreservesSpans(t *testing.T) {
	cfg := &Config{
		StreamName:    "test-stream",
		Region:        "us-east-1",
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecNone,
		MaxRecordSize: 120, // tiny: a batch of several spans must split
		PartitionKey:  PartitionKeyConfig{Strategy: partitionStrategyRandom, Hash: hashXXHash},
		Oversize:      OversizeConfig{Policies: []string{oversizeSplitHalf}, MaxAttempts: 16, MaxAttributeValueBytes: 4096},
	}
	capt := &capture{}
	exp := newTestExporterCfg(t, cfg, capt.injectSerialize())

	// One resource, 8 spans whose names make the marshaled batch exceed the
	// ceiling while any single span still fits, so all survive repeated halving.
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < 8; i++ {
		ss.Spans().AppendEmpty().SetName("span-name-padding-to-exceed-the-limit")
	}
	if err := exp.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("consume: %v", err)
	}
	recs := capt.all()
	if len(recs) <= 1 {
		t.Fatalf("expected split into >1 record, got %d", len(recs))
	}
	dec, _ := encoding.NewTracesDecoder(encoding.EncodingOTLPProto)
	total := 0
	for _, r := range recs {
		if len(r.Data) > cfg.MaxRecordSize {
			t.Fatalf("record exceeds max size: %d", len(r.Data))
		}
		d, _ := dec.Unmarshal(r.Data)
		total += d.SpanCount()
	}
	if total != 8 {
		t.Fatalf("spans preserved: got %d want 8", total)
	}
}

func TestOversizeSingleSpanDroppedCountsMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	tel, err := newExporterTelemetry(mp)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		StreamName:    "test-stream",
		Region:        "us-east-1",
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecNone,
		MaxRecordSize: 16, // smaller than any single span
		PartitionKey:  PartitionKeyConfig{Strategy: partitionStrategyRandom, Hash: hashXXHash},
		Oversize:      OversizeConfig{Policies: []string{oversizeSplitHalf}, MaxAttempts: 8, MaxAttributeValueBytes: 4096},
	}
	capt := &capture{}
	exp := newTestExporterCfg(t, cfg, capt.injectSerialize())
	exp.tel = tel

	if err := exp.ConsumeTraces(context.Background(), sampleTraces()); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if recs := capt.all(); len(recs) != 0 {
		t.Fatalf("expected no records sent, got %d", len(recs))
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	got := sumDropped(t, &rm)
	if got != 1 {
		t.Fatalf("dropped counter: got %d want 1", got)
	}
}

// TestOversizeTruncateAttributes proves the truncate_attribute_values policy
// rescues a payload whose bloat lives in a single span attribute, where
// split_half alone would drop the irreducible leaf.
func TestOversizeTruncateAttributes(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	tel, err := newExporterTelemetry(mp)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		StreamName:    "test-stream",
		Region:        "us-east-1",
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecNone,
		MaxRecordSize: 512,
		PartitionKey:  PartitionKeyConfig{Strategy: partitionStrategyRandom, Hash: hashXXHash},
		Oversize: OversizeConfig{
			Policies:               []string{oversizeTruncateAttrs, oversizeSplitHalf},
			MaxAttempts:            8,
			MaxAttributeValueBytes: 64,
		},
	}
	capt := &capture{}
	exp := newTestExporterCfg(t, cfg, capt.injectSerialize())
	exp.tel = tel

	// One resource, one span, one attribute whose value alone blows the ceiling.
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "svc")
	sp := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetName("span")
	huge := make([]byte, 4096)
	for i := range huge {
		huge[i] = 'x'
	}
	sp.Attributes().PutStr("payload", string(huge))

	if err := exp.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("consume: %v", err)
	}
	recs := capt.all()
	if len(recs) != 1 {
		t.Fatalf("records: got %d want 1", len(recs))
	}
	if len(recs[0].Data) > cfg.MaxRecordSize {
		t.Fatalf("record exceeds max size: %d", len(recs[0].Data))
	}

	dec, _ := encoding.NewTracesDecoder(encoding.EncodingOTLPProto)
	got, err := dec.Unmarshal(recs[0].Data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.SpanCount() != 1 {
		t.Fatalf("span count: got %d want 1", got.SpanCount())
	}
	v, ok := got.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().Get("payload")
	if !ok {
		t.Fatalf("payload attribute missing")
	}
	if len(v.Str()) != cfg.Oversize.MaxAttributeValueBytes {
		t.Fatalf("payload length: got %d want %d", len(v.Str()), cfg.Oversize.MaxAttributeValueBytes)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	if got := sumInt64Counter(t, &rm, "kinesis.exporter.attributes_truncated"); got != 1 {
		t.Fatalf("attributes_truncated: got %d want 1 (the one long attr)", got)
	}
	if got := sumDropped(t, &rm); got != 0 {
		t.Fatalf("records_dropped: got %d want 0", got)
	}
}

// TestOversizeChainSplitFallback proves the chain falls through from a
// truncate that finds nothing to clamp to a split that succeeds — and that the
// drop counter remains zero when split fits.
func TestOversizeChainSplitFallback(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	tel, err := newExporterTelemetry(mp)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		StreamName:    "test-stream",
		Region:        "us-east-1",
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecNone,
		MaxRecordSize: 120,
		PartitionKey:  PartitionKeyConfig{Strategy: partitionStrategyRandom, Hash: hashXXHash},
		Oversize: OversizeConfig{
			Policies:               []string{oversizeTruncateAttrs, oversizeSplitHalf},
			MaxAttempts:            16,
			MaxAttributeValueBytes: 4096, // larger than any attribute here — truncate is a no-op
		},
	}
	capt := &capture{}
	exp := newTestExporterCfg(t, cfg, capt.injectSerialize())
	exp.tel = tel

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < 8; i++ {
		ss.Spans().AppendEmpty().SetName("span-name-padding-to-exceed-the-limit")
	}
	if err := exp.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if len(capt.all()) <= 1 {
		t.Fatalf("expected split into >1 record, got %d", len(capt.all()))
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	if got := sumDropped(t, &rm); got != 0 {
		t.Fatalf("records_dropped: got %d want 0", got)
	}
	if got := sumInt64Counter(t, &rm, "kinesis.exporter.attributes_truncated"); got != 0 {
		t.Fatalf("attributes_truncated: got %d want 0 (no attribute exceeded the threshold)", got)
	}
}

// TestOversizeChainExhausted exercises the reject policy as the chain
// terminator: the irreducible single-span payload hits reject and is dropped
// with reason="reject_policy" rather than the default "irreducible".
func TestOversizeChainExhausted(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	tel, err := newExporterTelemetry(mp)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		StreamName:    "test-stream",
		Region:        "us-east-1",
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecNone,
		MaxRecordSize: 16,
		PartitionKey:  PartitionKeyConfig{Strategy: partitionStrategyRandom, Hash: hashXXHash},
		Oversize: OversizeConfig{
			Policies:               []string{oversizeReject},
			MaxAttempts:            8,
			MaxAttributeValueBytes: 4096,
		},
	}
	capt := &capture{}
	exp := newTestExporterCfg(t, cfg, capt.injectSerialize())
	exp.tel = tel

	if err := exp.ConsumeTraces(context.Background(), sampleTraces()); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if recs := capt.all(); len(recs) != 0 {
		t.Fatalf("records: got %d want 0", len(recs))
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	dropped := sumByReason(t, &rm, "kinesis.exporter.records_dropped")
	if dropped["reject_policy"] != 1 {
		t.Fatalf("records_dropped{reject_policy}: got %d want 1 (saw %v)", dropped["reject_policy"], dropped)
	}
}

// TestOversizeTruncateFallsThroughToSplit_StillCreditsAttributesClamped:
// when truncate clamps something but the result still doesn't fit, and
// split_half is what ultimately ships the records, the
// attributes_truncated counter must STILL reflect the attribute mutations
// that did happen. The data left the exporter in a mutated form regardless
// of which policy fit the payload, and that mutation must be observable.
func TestOversizeTruncateFallsThroughToSplit_StillCreditsAttributesClamped(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	tel, err := newExporterTelemetry(mp)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		StreamName:    "test-stream",
		Region:        "us-east-1",
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecNone,
		MaxRecordSize: 200, // small enough that 8 spans don't fit even after a tiny truncation
		PartitionKey:  PartitionKeyConfig{Strategy: partitionStrategyRandom, Hash: hashXXHash},
		Oversize: OversizeConfig{
			Policies:               []string{oversizeTruncateAttrs, oversizeSplitHalf},
			MaxAttempts:            16,
			MaxAttributeValueBytes: 8, // small threshold so the one long attr below DOES get clamped
		},
	}
	capt := &capture{}
	exp := newTestExporterCfg(t, cfg, capt.injectSerialize())
	exp.tel = tel

	// 8 spans, each with a padded name. ONE span has a short over-threshold
	// attribute so truncate has something to clamp (n=1) — but the savings are
	// negligible against the per-span name padding, so the batch still won't
	// fit. split_half then halves until each piece fits.
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < 8; i++ {
		sp := ss.Spans().AppendEmpty()
		sp.SetName("span-name-padding-to-exceed-the-limit")
	}
	// Add one long attribute on the first span so truncate clamps something.
	ss.Spans().At(0).Attributes().PutStr("k", "this-is-longer-than-eight-bytes")

	if err := exp.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if len(capt.all()) <= 1 {
		t.Fatalf("expected split into >1 record, got %d", len(capt.all()))
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	if got := sumInt64Counter(t, &rm, "kinesis.exporter.attributes_truncated"); got != 1 {
		t.Fatalf("attributes_truncated: got %d want 1 (one attribute was clamped even though split fit the batch)", got)
	}
}

// TestOversizeSplitMixedLeafReasons_PreservesLabels pins BUG-C: when
// split_half produces drops with different terminal reasons (irreducible vs
// max_attempts), each reason must land on the metric under its own label
// rather than collapsing to a single "chain_exhausted" bucket — the operator's
// tuning lever (MaxAttempts) is invisible otherwise.
func TestOversizeSplitMixedLeafReasons_PreservesLabels(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	tel, err := newExporterTelemetry(mp)
	if err != nil {
		t.Fatal(err)
	}
	// Two resources: R1 has one span (terminates on irreducible at depth 1),
	// R2 has eight spans that exhaust MaxAttempts before reducing to a leaf.
	// MaxRecordSize=16 keeps everything oversize. MaxAttempts=2 means each
	// sub-batch of R2 hits the bound at depth 2 with reason="max_attempts".
	cfg := &Config{
		StreamName:    "test-stream",
		Region:        "us-east-1",
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecNone,
		MaxRecordSize: 16,
		PartitionKey:  PartitionKeyConfig{Strategy: partitionStrategyRandom, Hash: hashXXHash},
		Oversize: OversizeConfig{
			Policies:               []string{oversizeSplitHalf},
			MaxAttempts:            2,
			MaxAttributeValueBytes: 4096,
		},
	}
	capt := &capture{}
	exp := newTestExporterCfg(t, cfg, capt.injectSerialize())
	exp.tel = tel

	td := ptrace.NewTraces()
	r1 := td.ResourceSpans().AppendEmpty()
	r1.ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("solo-span-name-padding")
	r2 := td.ResourceSpans().AppendEmpty()
	r2ss := r2.ScopeSpans().AppendEmpty()
	for i := 0; i < 8; i++ {
		r2ss.Spans().AppendEmpty().SetName("span-name-padding-to-exceed-the-limit")
	}
	if err := exp.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if len(capt.all()) != 0 {
		t.Fatalf("records sent: got %d want 0 (none should fit)", len(capt.all()))
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	dropped := sumByReason(t, &rm, "kinesis.exporter.records_dropped")
	if dropped["irreducible"] < 1 {
		t.Fatalf("records_dropped{irreducible}: got %d want >=1 (single-span leaf), all=%v", dropped["irreducible"], dropped)
	}
	if dropped["max_attempts"] < 1 {
		t.Fatalf("records_dropped{max_attempts}: got %d want >=1 (depth-bounded branch), all=%v", dropped["max_attempts"], dropped)
	}
	if dropped["chain_exhausted"] != 0 {
		t.Fatalf("records_dropped{chain_exhausted}: got %d want 0 (the chain ran one policy fully; mixed reasons must not collapse), all=%v", dropped["chain_exhausted"], dropped)
	}
}

// TestTruncateAttributePreservesUTF8 pins finding #4 from code review:
// clampStringAttrs must backstep to a codepoint boundary so the output is
// valid UTF-8 — encoding/json silently substitutes the replacement character
// on invalid sequences, and downstream consumers may reject mid-codepoint
// truncations outright.
func TestTruncateAttributePreservesUTF8(t *testing.T) {
	// "héllo" — the é is 2 bytes (0xC3 0xA9). With maxBytes=2 the naive cut
	// would land between the two bytes of é, producing invalid UTF-8.
	m := pcommon.NewMap()
	m.PutStr("k", "héllo")
	n := clampStringAttrs(m, 2)
	if n != 1 {
		t.Fatalf("clamps: got %d want 1", n)
	}
	v, _ := m.Get("k")
	got := v.Str()
	if !utf8.ValidString(got) {
		t.Fatalf("truncated value is not valid UTF-8: %q (bytes %x)", got, []byte(got))
	}
	if got != "h" {
		t.Fatalf("expected backstep to %q (drops the partial é), got %q", "h", got)
	}
}
