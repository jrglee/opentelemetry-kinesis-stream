package awskinesisexporter

import (
	"context"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/smithy-go/middleware"
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

func tagHashCfg() *Config {
	return &Config{
		StreamName:    "test-stream",
		Region:        "us-east-1",
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecNone,
		MaxRecordSize: 1 << 20,
		PartitionKey:  PartitionKeyConfig{Strategy: partitionStrategyTagHash, Tags: []string{"service.name", "region"}, Hash: hashXXHash},
		Oversize:      OversizeConfig{Policy: oversizeSplitHalf, MaxAttempts: 8},
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

func TestOversizeSplitPreservesSpans(t *testing.T) {
	cfg := &Config{
		StreamName:    "test-stream",
		Region:        "us-east-1",
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecNone,
		MaxRecordSize: 120, // tiny: a batch of several spans must split
		PartitionKey:  PartitionKeyConfig{Strategy: partitionStrategyRandom, Hash: hashXXHash},
		Oversize:      OversizeConfig{Policy: oversizeSplitHalf, MaxAttempts: 16},
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
	dropped, err := mp.Meter("awskinesisexporter").Int64Counter("kinesis.exporter.records_dropped")
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
		Oversize:      OversizeConfig{Policy: oversizeSplitHalf, MaxAttempts: 8},
	}
	capt := &capture{}
	exp := newTestExporterCfg(t, cfg, capt.injectSerialize())
	exp.recordsDropped = dropped

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
