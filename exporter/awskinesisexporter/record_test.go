package awskinesisexporter

import (
	"context"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/smithy-go/middleware"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// capture collects every PutRecordsRequestEntry the SDK would have sent,
// short-circuiting at Finalize with a success result so no network I/O occurs.
type capture struct {
	mu      sync.Mutex
	records []types.PutRecordsRequestEntry
}

// injectSerialize hooks the Initialize step where the typed input is still
// available, recording its records, then short-circuits the rest of the stack
// with a success result. This avoids depending on the wire request shape.
func (c *capture) injectSerialize() func(*kinesis.Options) {
	return func(o *kinesis.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Initialize.Add(
				middleware.InitializeMiddlewareFunc("captureKinesis", func(_ context.Context, in middleware.InitializeInput, _ middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
					if pr, ok := in.Parameters.(*kinesis.PutRecordsInput); ok {
						c.mu.Lock()
						c.records = append(c.records, pr.Records...)
						c.mu.Unlock()
					}
					return middleware.InitializeOutput{
						Result: &kinesis.PutRecordsOutput{FailedRecordCount: aws.Int32(0)},
					}, middleware.Metadata{}, nil
				}),
				middleware.Before,
			)
		})
	}
}

func (c *capture) all() []types.PutRecordsRequestEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.records
}

// tracesWith builds traces with one span per (service, region) resource.
func tracesWith(tuples [][2]string) ptrace.Traces {
	td := ptrace.NewTraces()
	for _, t := range tuples {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", t[0])
		rs.Resource().Attributes().PutStr("region", t[1])
		sp := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		sp.SetName(t[0] + "-" + t[1])
	}
	return td
}

func metricsWith(tuples [][2]string) pmetric.Metrics {
	md := pmetric.NewMetrics()
	for _, t := range tuples {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("service.name", t[0])
		rm.Resource().Attributes().PutStr("region", t[1])
		m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetName(t[0] + "-" + t[1])
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetIntValue(1)
	}
	return md
}

func logsWith(tuples [][2]string) plog.Logs {
	ld := plog.NewLogs()
	for _, t := range tuples {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", t[0])
		rl.Resource().Attributes().PutStr("region", t[1])
		lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		lr.Body().SetStr(t[0] + "-" + t[1])
	}
	return ld
}

func tagHashCfg() *Config {
	return &Config{
		StreamName:    "test-stream",
		Region:        "us-east-1",
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecNone,
		MaxRecordSize: 1 << 20,
		PartitionKey:  PartitionKeyConfig{Strategy: partitionStrategyTagHash, Tags: []string{"service.name", "region"}, Hash: hashXXHash},
		Oversize:      OversizeConfig{Policies: []string{oversizeSplitHalf}, MaxAttempts: 8, MaxAttributeValueBytes: 4096},
	}
}

func tuples() [][2]string {
	var out [][2]string
	for _, svc := range []string{"A", "B", "C"} {
		for _, reg := range []string{"us-east", "us-west"} {
			out = append(out, [2]string{svc, reg})
		}
	}
	return out
}

func TestTracesTagGrouping(t *testing.T) {
	capt := &capture{}
	exp := newTestExporterCfg(t, tagHashCfg(), capt.injectSerialize())
	// Two spans per tuple, shuffled order, to prove grouping collapses them.
	tps := tuples()
	all := append(append([][2]string{}, tps...), tps...)
	if err := exp.ConsumeTraces(context.Background(), tracesWith(all)); err != nil {
		t.Fatalf("consume: %v", err)
	}
	recs := capt.all()
	if len(recs) != len(tps) {
		t.Fatalf("records: got %d want %d (one per tuple)", len(recs), len(tps))
	}
	// Each distinct tuple -> exactly one record with a stable key; spans preserved.
	keyByName := map[string]string{}
	totalSpans := 0
	dec, _ := encoding.NewTracesDecoder(encoding.EncodingOTLPProto)
	for _, r := range recs {
		td, err := dec.Unmarshal(r.Data)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		totalSpans += td.SpanCount()
		rs := td.ResourceSpans().At(0)
		svc, _ := rs.Resource().Attributes().Get("service.name")
		reg, _ := rs.Resource().Attributes().Get("region")
		name := svc.AsString() + "/" + reg.AsString()
		if prev, ok := keyByName[name]; ok && prev != aws.ToString(r.PartitionKey) {
			t.Fatalf("unstable key for %s: %s vs %s", name, prev, aws.ToString(r.PartitionKey))
		}
		keyByName[name] = aws.ToString(r.PartitionKey)
	}
	if totalSpans != len(all) {
		t.Fatalf("spans preserved: got %d want %d", totalSpans, len(all))
	}
	if len(keyByName) != len(tps) {
		t.Fatalf("distinct tuples: got %d want %d", len(keyByName), len(tps))
	}
}

func TestMetricsTagGrouping(t *testing.T) {
	capt := &capture{}
	exp := newTestExporterCfg(t, tagHashCfg(), capt.injectSerialize())
	tps := tuples()
	all := append(append([][2]string{}, tps...), tps...)
	if err := exp.ConsumeMetrics(context.Background(), metricsWith(all)); err != nil {
		t.Fatalf("consume: %v", err)
	}
	recs := capt.all()
	if len(recs) != len(tps) {
		t.Fatalf("records: got %d want %d", len(recs), len(tps))
	}
	dec, _ := encoding.NewMetricsDecoder(encoding.EncodingOTLPProto)
	totalDP := 0
	keys := map[string]string{}
	for _, r := range recs {
		md, err := dec.Unmarshal(r.Data)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		totalDP += md.DataPointCount()
		rm := md.ResourceMetrics().At(0)
		svc, _ := rm.Resource().Attributes().Get("service.name")
		reg, _ := rm.Resource().Attributes().Get("region")
		name := svc.AsString() + "/" + reg.AsString()
		if prev, ok := keys[name]; ok && prev != aws.ToString(r.PartitionKey) {
			t.Fatalf("unstable key for %s", name)
		}
		keys[name] = aws.ToString(r.PartitionKey)
	}
	if totalDP != len(all) {
		t.Fatalf("datapoints preserved: got %d want %d", totalDP, len(all))
	}
}

func TestLogsTagGrouping(t *testing.T) {
	capt := &capture{}
	exp := newTestExporterCfg(t, tagHashCfg(), capt.injectSerialize())
	tps := tuples()
	all := append(append([][2]string{}, tps...), tps...)
	if err := exp.ConsumeLogs(context.Background(), logsWith(all)); err != nil {
		t.Fatalf("consume: %v", err)
	}
	recs := capt.all()
	if len(recs) != len(tps) {
		t.Fatalf("records: got %d want %d", len(recs), len(tps))
	}
	dec, _ := encoding.NewLogsDecoder(encoding.EncodingOTLPProto)
	totalLR := 0
	keys := map[string]string{}
	for _, r := range recs {
		ld, err := dec.Unmarshal(r.Data)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		totalLR += ld.LogRecordCount()
		rl := ld.ResourceLogs().At(0)
		svc, _ := rl.Resource().Attributes().Get("service.name")
		reg, _ := rl.Resource().Attributes().Get("region")
		name := svc.AsString() + "/" + reg.AsString()
		if prev, ok := keys[name]; ok && prev != aws.ToString(r.PartitionKey) {
			t.Fatalf("unstable key for %s", name)
		}
		keys[name] = aws.ToString(r.PartitionKey)
	}
	if totalLR != len(all) {
		t.Fatalf("log records preserved: got %d want %d", totalLR, len(all))
	}
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

// TestPutRecordsPartialFailureRetriesTransientOnly verifies the partial-failure
// contract: a permanently-rejected record is dropped-and-counted exactly once
// (never re-sent), while throttled / InternalFailure records are retried in
// place — only that transient subset, so succeeded records are not duplicated.
func TestPutRecordsPartialFailureRetriesTransientOnly(t *testing.T) {
	defer withFastBackoff()()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	var mu sync.Mutex
	var callSizes []int

	inject := func(o *kinesis.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Initialize.Add(
				middleware.InitializeMiddlewareFunc("programmedKinesis", func(_ context.Context, in middleware.InitializeInput, _ middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
					pr := in.Parameters.(*kinesis.PutRecordsInput)
					n := len(pr.Records)
					mu.Lock()
					callSizes = append(callSizes, n)
					first := len(callSizes) == 1
					mu.Unlock()

					recs := make([]types.PutRecordsResultEntry, n)
					var failed int32
					for i := range recs {
						code := ""
						if first {
							switch i {
							case 1:
								code = "ProvisionedThroughputExceededException" // transient
							case 2:
								code = "InternalFailure" // transient
							case 3:
								code = "ValidationException" // permanent
							}
						}
						if code == "" {
							recs[i] = types.PutRecordsResultEntry{SequenceNumber: aws.String("ok")}
						} else {
							recs[i] = types.PutRecordsResultEntry{ErrorCode: aws.String(code), ErrorMessage: aws.String(code)}
							failed++
						}
					}
					return middleware.InitializeOutput{
						Result: &kinesis.PutRecordsOutput{FailedRecordCount: aws.Int32(failed), Records: recs},
					}, middleware.Metadata{}, nil
				}),
				middleware.Before,
			)
		})
	}

	exp := newTestExporterCfg(t, tagHashCfg(), inject)
	tel, err := newExporterTelemetry(mp)
	if err != nil {
		t.Fatal(err)
	}
	exp.tel = tel

	if err := exp.ConsumeTraces(context.Background(), tracesWith(tuples())); err != nil {
		t.Fatalf("consume: %v", err) // transient retried to success, permanent dropped → no error
	}

	mu.Lock()
	defer mu.Unlock()
	if len(callSizes) != 2 {
		t.Fatalf("PutRecords calls: got %d (%v) want 2 (initial + transient retry)", len(callSizes), callSizes)
	}
	if callSizes[0] != len(tuples()) {
		t.Fatalf("first call records: got %d want %d", callSizes[0], len(tuples()))
	}
	if callSizes[1] != 2 {
		t.Fatalf("retry call records: got %d want 2 (only the transient subset)", callSizes[1])
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	if got := sumDropped(t, &rm); got != 1 {
		t.Fatalf("dropped counter: got %d want 1 (the one permanent rejection, counted once)", got)
	}
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
// valid UTF-8 — strict downstream encoders (otel_arrow) reject mid-codepoint
// truncations, and encoding/json silently substitutes the replacement
// character.
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

func TestRandomDefaultKey(t *testing.T) {
	capt := &capture{}
	exp := newTestExporter(t, 1<<20, capt.injectSerialize())
	if err := exp.ConsumeTraces(context.Background(), sampleTraces()); err != nil {
		t.Fatalf("consume 1: %v", err)
	}
	if err := exp.ConsumeTraces(context.Background(), sampleTraces()); err != nil {
		t.Fatalf("consume 2: %v", err)
	}
	recs := capt.all()
	if len(recs) != 2 {
		t.Fatalf("records: got %d want 2 (one per call)", len(recs))
	}
	k1, k2 := aws.ToString(recs[0].PartitionKey), aws.ToString(recs[1].PartitionKey)
	if k1 == "" || k2 == "" {
		t.Fatalf("empty random key")
	}
	if k1 == k2 {
		t.Fatalf("random keys should differ across calls: %s == %s", k1, k2)
	}
}
