//go:build perf

package perf

import (
	"bytes"
	"testing"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// TestDatasetsDeterministic guards the reproducibility invariant: two calls to
// GenerateMetrics / GenerateTraces with the same arguments must produce
// byte-identical encoded payloads. If this ever fails, the perf numbers have
// stopped being comparable across runs and machines.
func TestDatasetsDeterministic(t *testing.T) {
	enc, err := encoding.NewMetricsEncoder(encoding.EncodingOTLPProto)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range MetricsProfiles {
		for _, size := range []int{1, 100, 1000} {
			a := GenerateMetrics(p, size)
			b := GenerateMetrics(p, size)
			ba, err := enc.Marshal(a)
			if err != nil {
				t.Fatalf("%s/%d marshal A: %v", p, size, err)
			}
			bb, err := enc.Marshal(b)
			if err != nil {
				t.Fatalf("%s/%d marshal B: %v", p, size, err)
			}
			if !bytes.Equal(ba, bb) {
				t.Fatalf("%s/%d: non-deterministic — payload bytes differ between calls", p, size)
			}
		}
	}

	tenc, err := encoding.NewTracesEncoder(encoding.EncodingOTLPProto)
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{1, 100} {
		a := GenerateTraces(size)
		b := GenerateTraces(size)
		ba, err := tenc.Marshal(a)
		if err != nil {
			t.Fatalf("traces/%d marshal A: %v", size, err)
		}
		bb, err := tenc.Marshal(b)
		if err != nil {
			t.Fatalf("traces/%d marshal B: %v", size, err)
		}
		if !bytes.Equal(ba, bb) {
			t.Fatalf("traces/%d: non-deterministic — payload bytes differ between calls", size)
		}
	}
}
