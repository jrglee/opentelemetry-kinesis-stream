package awskinesisreceiver

import (
	"context"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap/zaptest"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// testTelemetry builds a no-op-backed telemetry struct for tests that exercise
// the poller/coordinator without asserting on emitted metrics.
func testTelemetry(t *testing.T) *receiverTelemetry {
	t.Helper()
	tel, err := newReceiverTelemetry(noopmetric.NewMeterProvider())
	if err != nil {
		t.Fatalf("telemetry: %v", err)
	}
	return tel
}

func tracesRecord(t *testing.T, name string) types.Record {
	t.Helper()
	enc, _ := encoding.NewTracesEncoder(encoding.EncodingOTLPProto)
	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName(name)
	b, err := enc.Marshal(td)
	if err != nil {
		t.Fatal(err)
	}
	return types.Record{Data: b, SequenceNumber: aws.String("s#0001"), PartitionKey: aws.String("pk")}
}

func logsRecord(t *testing.T, body string) types.Record {
	t.Helper()
	enc, _ := encoding.NewLogsEncoder(encoding.EncodingOTLPProto)
	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Body().SetStr(body)
	b, err := enc.Marshal(ld)
	if err != nil {
		t.Fatal(err)
	}
	return types.Record{Data: b, SequenceNumber: aws.String("s#0003"), PartitionKey: aws.String("pk")}
}

func metricsRecord(t *testing.T, name string) types.Record {
	t.Helper()
	enc, _ := encoding.NewMetricsEncoder(encoding.EncodingOTLPProto)
	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName(name)
	m.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(7)
	b, err := enc.Marshal(md)
	if err != nil {
		t.Fatal(err)
	}
	return types.Record{Data: b, SequenceNumber: aws.String("s#0002"), PartitionKey: aws.String("pk")}
}

type traceCollector struct {
	mu  sync.Mutex
	got []ptrace.Traces
}

func (c *traceCollector) consume(_ context.Context, td ptrace.Traces) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.got = append(c.got, td)
	return nil
}

type logCollector struct {
	mu  sync.Mutex
	got []plog.Logs
}

func (c *logCollector) consume(_ context.Context, ld plog.Logs) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.got = append(c.got, ld)
	return nil
}

type metricCollector struct {
	mu  sync.Mutex
	got []pmetric.Metrics
}

func (c *metricCollector) consume(_ context.Context, md pmetric.Metrics) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.got = append(c.got, md)
	return nil
}

func newPoller(t *testing.T, s sink, deadLetter bool) *shardPoller {
	t.Helper()
	comp, _ := encoding.NewCompressor(encoding.CodecNone)
	return &shardPoller{
		cfg: &Config{
			Encoding:    encoding.EncodingOTLPProto,
			Compression: encoding.CodecNone,
			DeadLetter:  DeadLetterConfig{Enabled: deadLetter},
		},
		comp:   comp,
		sink:   s,
		logger: zaptest.NewLogger(t),
		tel:    testTelemetry(t),
	}
}

func TestTracesSinkDelivers(t *testing.T) {
	col := &traceCollector{}
	next, _ := consumer.NewTraces(col.consume)
	p := newPoller(t, tracesSink{decoder: mustTracesDecoder(t), consumer: next}, false)

	if got := p.handleRecord(context.Background(), tracesRecord(t, "ok-span")); got != recordOK {
		t.Fatalf("result = %v, want recordOK", got)
	}
	if len(col.got) != 1 || col.got[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name() != "ok-span" {
		t.Fatalf("span not delivered: %+v", col.got)
	}
}

func TestMetricsSinkDelivers(t *testing.T) {
	col := &metricCollector{}
	next, _ := consumer.NewMetrics(col.consume)
	p := newPoller(t, metricsSink{decoder: mustMetricsDecoder(t), consumer: next}, false)

	if got := p.handleRecord(context.Background(), metricsRecord(t, "ok-metric")); got != recordOK {
		t.Fatalf("result = %v, want recordOK", got)
	}
	if len(col.got) != 1 || col.got[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Name() != "ok-metric" {
		t.Fatalf("metric not delivered: %+v", col.got)
	}
}

func TestLogsSinkDelivers(t *testing.T) {
	col := &logCollector{}
	next, _ := consumer.NewLogs(col.consume)
	p := newPoller(t, logsSink{decoder: mustLogsDecoder(t), consumer: next}, false)

	if got := p.handleRecord(context.Background(), logsRecord(t, "ok-log")); got != recordOK {
		t.Fatalf("result = %v, want recordOK", got)
	}
	if len(col.got) != 1 || col.got[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().AsString() != "ok-log" {
		t.Fatalf("log not delivered: %+v", col.got)
	}
}

func TestDeadLetterTracesOnDecodeFailure(t *testing.T) {
	col := &traceCollector{}
	next, _ := consumer.NewTraces(col.consume)
	p := newPoller(t, tracesSink{decoder: mustTracesDecoder(t), consumer: next}, true)

	corrupt := types.Record{Data: []byte("not-otlp-proto"), SequenceNumber: aws.String("s#9"), PartitionKey: aws.String("pk")}
	if got := p.handleRecord(context.Background(), corrupt); got != recordSkip {
		t.Fatalf("result = %v, want recordSkip", got)
	}
	if len(col.got) != 1 {
		t.Fatalf("expected one dead-letter span, got %d", len(col.got))
	}
	span := col.got[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	if span.Name() != deadLetterName {
		t.Fatalf("span name = %q, want %q", span.Name(), deadLetterName)
	}
	if fc, ok := span.Attributes().Get("kinesis.failure_class"); !ok || fc.AsString() != "decode" {
		t.Fatalf("failure_class attr missing/wrong: %v", fc)
	}
}

func TestDeadLetterDisabledDropsSilently(t *testing.T) {
	col := &traceCollector{}
	next, _ := consumer.NewTraces(col.consume)
	p := newPoller(t, tracesSink{decoder: mustTracesDecoder(t), consumer: next}, false)

	corrupt := types.Record{Data: []byte("not-otlp"), SequenceNumber: aws.String("s#9"), PartitionKey: aws.String("pk")}
	if got := p.handleRecord(context.Background(), corrupt); got != recordSkip {
		t.Fatalf("result = %v, want recordSkip", got)
	}
	if len(col.got) != 0 {
		t.Fatalf("expected no emission when dead-letter disabled, got %d", len(col.got))
	}
}

func mustTracesDecoder(t *testing.T) encoding.TracesDecoder {
	t.Helper()
	d, err := encoding.NewTracesDecoder(encoding.EncodingOTLPProto)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func mustMetricsDecoder(t *testing.T) encoding.MetricsDecoder {
	t.Helper()
	d, err := encoding.NewMetricsDecoder(encoding.EncodingOTLPProto)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func mustLogsDecoder(t *testing.T) encoding.LogsDecoder {
	t.Helper()
	d, err := encoding.NewLogsDecoder(encoding.EncodingOTLPProto)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
