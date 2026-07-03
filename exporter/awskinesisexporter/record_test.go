package awskinesisexporter

import (
	"context"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/smithy-go/middleware"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

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
