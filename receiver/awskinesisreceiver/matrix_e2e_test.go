package awskinesisreceiver

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// TestEncodingCodecMatrix is the in-process correctness sweep across every
// (encoding × codec) combination. It drives the real coordinator and shard
// pollers — only the AWS HTTP layer is faked, via the same Smithy middleware
// pattern that reshard_test.go uses. For each combo it pre-marshals N traces
// at the producer side using the configured encoding+codec, stuffs the bytes
// into a single closed shard, lets the coordinator drain it, and asserts the
// full count was delivered in order with no duplicates and matching span
// names. This covers the whole receive path (decompress → decode → sink)
// where the per-marshaler unit tests only cover the encode/decode seam.
func TestEncodingCodecMatrix(t *testing.T) {
	encodings := []encoding.Encoding{
		encoding.EncodingOTLPProto,
		encoding.EncodingOTLPJSON,
		encoding.EncodingOTelArrow,
	}
	codecs := []encoding.Codec{
		encoding.CodecNone,
		encoding.CodecGzip,
		encoding.CodecZstd,
		encoding.CodecSnappy,
		encoding.CodecSnappyFramed,
		encoding.CodecZlib,
		encoding.CodecDeflate,
	}

	const N = 25

	for _, e := range encodings {
		for _, c := range codecs {
			e, c := e, c
			t.Run(fmt.Sprintf("traces/%s/%s", e, c), func(t *testing.T) {
				t.Parallel()
				runTracesMatrixCase(t, e, c, N)
			})
			t.Run(fmt.Sprintf("metrics/%s/%s", e, c), func(t *testing.T) {
				t.Parallel()
				runMetricsMatrixCase(t, e, c, N)
			})
			t.Run(fmt.Sprintf("logs/%s/%s", e, c), func(t *testing.T) {
				t.Parallel()
				runLogsMatrixCase(t, e, c, N)
			})
		}
	}
}

func runTracesMatrixCase(t *testing.T, e encoding.Encoding, c encoding.Codec, n int) {
	t.Helper()

	enc, err := encoding.NewTracesEncoder(e)
	if err != nil {
		t.Fatalf("NewTracesEncoder: %v", err)
	}
	dec, err := encoding.NewTracesDecoder(e)
	if err != nil {
		t.Fatalf("NewTracesDecoder: %v", err)
	}
	comp, err := encoding.NewCompressor(c)
	if err != nil {
		t.Fatalf("NewCompressor: %v", err)
	}
	records := make([][]byte, n)
	for i := 0; i < n; i++ {
		td := ptrace.NewTraces()
		s := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		s.SetName(fmt.Sprintf("matrix-%d", i))
		raw, err := enc.Marshal(td)
		if err != nil {
			t.Fatalf("Marshal[%d]: %v", i, err)
		}
		zipped, err := comp.Compress(raw)
		if err != nil {
			t.Fatalf("Compress[%d]: %v", i, err)
		}
		records[i] = zipped
	}

	fs := &fakeStream{shards: []*fakeShard{{id: "shard-matrix", records: records}}}

	rec := &recorder{}
	consumeFn, err := consumer.NewTraces(rec.consume)
	if err != nil {
		t.Fatalf("consumer.NewTraces: %v", err)
	}

	cfg := fastCoordCfg("matrix", e, c)
	coord := newTestCoordinator(t, cfg, fs, tracesSink{decoder: dec, consumer: consumeFn}, "matrix")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := coord.start(ctx); err != nil {
		t.Fatalf("coordinator start: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && rec.len() < n {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	coord.wait()

	names := rec.snapshot()
	if len(names) != n {
		t.Fatalf("delivered %d spans, want %d", len(names), n)
	}
	counts := make(map[string]int, n)
	for _, name := range names {
		counts[name]++
	}
	for i := 0; i < n; i++ {
		want := fmt.Sprintf("matrix-%d", i)
		if counts[want] != 1 {
			t.Fatalf("span %q delivered %d times, want 1", want, counts[want])
		}
	}
	// Order: shard delivers strictly in sequence.
	for i, name := range names {
		if want := fmt.Sprintf("matrix-%d", i); !strings.HasSuffix(name, want) {
			t.Fatalf("position %d delivered %q, want %q", i, name, want)
		}
	}
}

func runLogsMatrixCase(t *testing.T, e encoding.Encoding, c encoding.Codec, n int) {
	t.Helper()

	enc, err := encoding.NewLogsEncoder(e)
	if err != nil {
		t.Fatalf("NewLogsEncoder: %v", err)
	}
	dec, err := encoding.NewLogsDecoder(e)
	if err != nil {
		t.Fatalf("NewLogsDecoder: %v", err)
	}
	comp, err := encoding.NewCompressor(c)
	if err != nil {
		t.Fatalf("NewCompressor: %v", err)
	}

	records := make([][]byte, n)
	for i := 0; i < n; i++ {
		ld := plog.NewLogs()
		lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		lr.Body().SetStr(fmt.Sprintf("l-%d", i))
		raw, err := enc.Marshal(ld)
		if err != nil {
			t.Fatalf("Marshal[%d]: %v", i, err)
		}
		zipped, err := comp.Compress(raw)
		if err != nil {
			t.Fatalf("Compress[%d]: %v", i, err)
		}
		records[i] = zipped
	}

	fs := &fakeStream{shards: []*fakeShard{{id: "shard-matrix-l", records: records}}}

	lc := &logCollector{}
	consumeFn, err := consumer.NewLogs(lc.consume)
	if err != nil {
		t.Fatalf("consumer.NewLogs: %v", err)
	}

	cfg := fastCoordCfg("matrix-logs", e, c)
	coord := newTestCoordinator(t, cfg, fs, logsSink{decoder: dec, consumer: consumeFn}, "matrix-l")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := coord.start(ctx); err != nil {
		t.Fatalf("coordinator start: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		lc.mu.Lock()
		got := len(lc.got)
		lc.mu.Unlock()
		if got >= n {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	coord.wait()

	lc.mu.Lock()
	defer lc.mu.Unlock()
	if len(lc.got) != n {
		t.Fatalf("delivered %d log batches, want %d", len(lc.got), n)
	}
	counts := make(map[string]int, n)
	for _, ld := range lc.got {
		body := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().AsString()
		counts[body]++
	}
	for i := 0; i < n; i++ {
		want := fmt.Sprintf("l-%d", i)
		if counts[want] != 1 {
			t.Fatalf("log %q delivered %d times, want 1", want, counts[want])
		}
	}
}

func runMetricsMatrixCase(t *testing.T, e encoding.Encoding, c encoding.Codec, n int) {
	t.Helper()

	enc, err := encoding.NewMetricsEncoder(e)
	if err != nil {
		t.Fatalf("NewMetricsEncoder: %v", err)
	}
	dec, err := encoding.NewMetricsDecoder(e)
	if err != nil {
		t.Fatalf("NewMetricsDecoder: %v", err)
	}
	comp, err := encoding.NewCompressor(c)
	if err != nil {
		t.Fatalf("NewCompressor: %v", err)
	}

	records := make([][]byte, n)
	for i := 0; i < n; i++ {
		md := pmetric.NewMetrics()
		m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetName(fmt.Sprintf("m-%d", i))
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetIntValue(int64(i))
		raw, err := enc.Marshal(md)
		if err != nil {
			t.Fatalf("Marshal[%d]: %v", i, err)
		}
		zipped, err := comp.Compress(raw)
		if err != nil {
			t.Fatalf("Compress[%d]: %v", i, err)
		}
		records[i] = zipped
	}

	fs := &fakeStream{shards: []*fakeShard{{id: "shard-matrix-m", records: records}}}

	mc := &metricCollector{}
	consumeFn, err := consumer.NewMetrics(mc.consume)
	if err != nil {
		t.Fatalf("consumer.NewMetrics: %v", err)
	}

	cfg := fastCoordCfg("matrix-metrics", e, c)
	coord := newTestCoordinator(t, cfg, fs, metricsSink{decoder: dec, consumer: consumeFn}, "matrix-m")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := coord.start(ctx); err != nil {
		t.Fatalf("coordinator start: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mc.mu.Lock()
		got := len(mc.got)
		mc.mu.Unlock()
		if got >= n {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	coord.wait()

	mc.mu.Lock()
	defer mc.mu.Unlock()
	if len(mc.got) != n {
		t.Fatalf("delivered %d metrics batches, want %d", len(mc.got), n)
	}
	counts := make(map[string]int, n)
	for _, md := range mc.got {
		name := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Name()
		counts[name]++
	}
	for i := 0; i < n; i++ {
		want := fmt.Sprintf("m-%d", i)
		if counts[want] != 1 {
			t.Fatalf("metric %q delivered %d times, want 1", want, counts[want])
		}
	}
}
