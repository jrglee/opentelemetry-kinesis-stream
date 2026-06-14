package encoding

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestRoundTrip(t *testing.T) {
	encodings := []Encoding{EncodingOTLPProto, EncodingOTLPJSON}
	codecs := []Codec{CodecNone, CodecGzip, CodecZstd, CodecSnappy, CodecSnappyFramed, CodecZlib, CodecDeflate}

	for _, e := range encodings {
		for _, c := range codecs {
			name := string(e) + "/" + string(c)
			t.Run(name, func(t *testing.T) {
				enc, err := NewTracesEncoder(e)
				if err != nil {
					t.Fatalf("NewTracesEncoder: %v", err)
				}
				dec, err := NewTracesDecoder(e)
				if err != nil {
					t.Fatalf("NewTracesDecoder: %v", err)
				}
				comp, err := NewCompressor(c)
				if err != nil {
					t.Fatalf("NewCompressor: %v", err)
				}

				in := sampleTraces()
				raw, err := enc.Marshal(in)
				if err != nil {
					t.Fatalf("Marshal: %v", err)
				}
				zipped, err := comp.Compress(raw)
				if err != nil {
					t.Fatalf("Compress: %v", err)
				}
				unzipped, err := comp.Decompress(zipped)
				if err != nil {
					t.Fatalf("Decompress: %v", err)
				}
				out, err := dec.Unmarshal(unzipped)
				if err != nil {
					t.Fatalf("Unmarshal: %v", err)
				}

				if out.SpanCount() != in.SpanCount() {
					t.Fatalf("span count mismatch: got %d want %d", out.SpanCount(), in.SpanCount())
				}
				if got := out.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name(); got != "test-span" {
					t.Fatalf("span name mismatch: got %q want %q", got, "test-span")
				}
			})
		}
	}
}

func TestMetricsRoundTrip(t *testing.T) {
	encodings := []Encoding{EncodingOTLPProto, EncodingOTLPJSON}
	codecs := []Codec{CodecNone, CodecGzip, CodecZstd, CodecSnappy, CodecSnappyFramed, CodecZlib, CodecDeflate}
	for _, e := range encodings {
		for _, c := range codecs {
			name := string(e) + "/" + string(c)
			t.Run(name, func(t *testing.T) {
				enc, err := NewMetricsEncoder(e)
				if err != nil {
					t.Fatalf("NewMetricsEncoder: %v", err)
				}
				dec, err := NewMetricsDecoder(e)
				if err != nil {
					t.Fatalf("NewMetricsDecoder: %v", err)
				}
				comp, err := NewCompressor(c)
				if err != nil {
					t.Fatalf("NewCompressor: %v", err)
				}

				in := sampleMetrics()
				raw, err := enc.Marshal(in)
				if err != nil {
					t.Fatalf("Marshal: %v", err)
				}
				zipped, err := comp.Compress(raw)
				if err != nil {
					t.Fatalf("Compress: %v", err)
				}
				unzipped, err := comp.Decompress(zipped)
				if err != nil {
					t.Fatalf("Decompress: %v", err)
				}
				out, err := dec.Unmarshal(unzipped)
				if err != nil {
					t.Fatalf("Unmarshal: %v", err)
				}

				if out.DataPointCount() != in.DataPointCount() {
					t.Fatalf("datapoint count mismatch: got %d want %d", out.DataPointCount(), in.DataPointCount())
				}
				gotName := out.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Name()
				if gotName != "test-metric" {
					t.Fatalf("metric name mismatch: got %q want %q", gotName, "test-metric")
				}
			})
		}
	}
}

func TestLogsRoundTrip(t *testing.T) {
	encodings := []Encoding{EncodingOTLPProto, EncodingOTLPJSON}
	codecs := []Codec{CodecNone, CodecGzip, CodecZstd, CodecSnappy, CodecSnappyFramed, CodecZlib, CodecDeflate}
	for _, e := range encodings {
		for _, c := range codecs {
			name := string(e) + "/" + string(c)
			t.Run(name, func(t *testing.T) {
				enc, err := NewLogsEncoder(e)
				if err != nil {
					t.Fatalf("NewLogsEncoder: %v", err)
				}
				dec, err := NewLogsDecoder(e)
				if err != nil {
					t.Fatalf("NewLogsDecoder: %v", err)
				}
				comp, err := NewCompressor(c)
				if err != nil {
					t.Fatalf("NewCompressor: %v", err)
				}

				in := sampleLogs()
				raw, err := enc.Marshal(in)
				if err != nil {
					t.Fatalf("Marshal: %v", err)
				}
				zipped, err := comp.Compress(raw)
				if err != nil {
					t.Fatalf("Compress: %v", err)
				}
				unzipped, err := comp.Decompress(zipped)
				if err != nil {
					t.Fatalf("Decompress: %v", err)
				}
				out, err := dec.Unmarshal(unzipped)
				if err != nil {
					t.Fatalf("Unmarshal: %v", err)
				}

				if out.LogRecordCount() != in.LogRecordCount() {
					t.Fatalf("log record count mismatch: got %d want %d", out.LogRecordCount(), in.LogRecordCount())
				}
				gotBody := out.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().AsString()
				if gotBody != "test-log" {
					t.Fatalf("log body mismatch: got %q want %q", gotBody, "test-log")
				}
			})
		}
	}
}

func TestUnknownEncoding(t *testing.T) {
	if _, err := NewTracesEncoder("bogus"); err == nil {
		t.Fatal("expected error for unknown encoding")
	}
	if _, err := NewMetricsEncoder("bogus"); err == nil {
		t.Fatal("expected error for unknown encoding")
	}
	if _, err := NewLogsEncoder("bogus"); err == nil {
		t.Fatal("expected error for unknown encoding")
	}
	if _, err := NewCompressor("bogus"); err == nil {
		t.Fatal("expected error for unknown codec")
	}
}

func sampleTraces() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "test-service")
	ss := rs.ScopeSpans().AppendEmpty()
	span := ss.Spans().AppendEmpty()
	span.SetName("test-span")
	span.SetTraceID([16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10})
	span.SetSpanID([8]byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18})
	return td
}

func sampleLogs() plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "test-service")
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Body().SetStr("test-log")
	lr.Attributes().PutStr("k", "v")
	return ld
}

func sampleMetrics() pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-service")
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("test-metric")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetIntValue(42)
	dp.Attributes().PutStr("host", "h1")
	return md
}
