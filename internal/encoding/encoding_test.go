package encoding

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestRoundTrip(t *testing.T) {
	encodings := []Encoding{EncodingOTLPProto}
	codecs := []Codec{CodecNone, CodecGzip, CodecZstd, CodecSnappy}

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

func TestUnknownEncoding(t *testing.T) {
	if _, err := NewTracesEncoder("bogus"); err == nil {
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
