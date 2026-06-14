//go:build perf

package perf

import (
	"fmt"
	"testing"
	"time"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// warmupIterations is the number of throw-away (encode, compress) cycles we
// run before each benchmark resets its timer. Priming both Arrow's internal
// pools and the codec writer state keeps first-call allocation noise out of
// the steady-state numbers. Kept small so warm-up wall time stays bounded at
// the very large batch sizes (n=1M Arrow encode is ~3s per call).
const warmupIterations = 5

// benchEncodings is the per-profile sweep of encodings. Codecs are swept
// inside each benchmark.
var benchEncodings = []encoding.Encoding{
	encoding.EncodingOTLPProto,
	encoding.EncodingOTLPJSON,
	encoding.EncodingOTelArrow,
}

// benchCodecs is the full codec set the encoding layer exposes. Each codec
// has a different CPU/ratio profile; the report makes the trade-off visible
// rather than guessing which one to recommend.
var benchCodecs = []encoding.Codec{
	encoding.CodecNone,
	encoding.CodecGzip,
	encoding.CodecZstd,
	encoding.CodecSnappy,
	encoding.CodecSnappyFramed,
	encoding.CodecZlib,
	encoding.CodecDeflate,
}

// BenchmarkEncodeMetrics measures encode-plus-compress wall time, on-wire
// size, and compression ratio across (profile × encoding × codec × batch
// size). The reported `compressed_bytes` and `compression_ratio` metrics are
// deterministic (input bytes are seeded), so they read the same on any
// architecture; ns/op varies with hardware but the *ordering* of encodings
// on a single host is the load-bearing comparison.
func BenchmarkEncodeMetrics(b *testing.B) {
	for _, profile := range MetricsProfiles {
		for _, size := range BatchSizes {
			md := GenerateMetrics(profile, size)
			for _, e := range benchEncodings {
				enc, err := encoding.NewMetricsEncoder(e)
				if err != nil {
					b.Fatalf("NewMetricsEncoder(%s): %v", e, err)
				}
				for _, c := range benchCodecs {
					comp, err := encoding.NewCompressor(c)
					if err != nil {
						b.Fatalf("NewCompressor(%s): %v", c, err)
					}
					name := fmt.Sprintf("%s/%s/%s/n=%d", profile, e, c, size)
					b.Run(name, func(b *testing.B) {
						// Warm-up: prime buffer pools (Arrow producer state,
						// zstd window, etc.). Discarded before the timer runs.
						// Upstream Arrow can panic on extreme cardinality;
						// safeMarshal converts the panic to an error so the
						// case is skipped and the matrix continues.
						var rawLen, compressedLen int
						for i := 0; i < warmupIterations; i++ {
							raw, err := safeMarshal(enc.Marshal, md)
							if err != nil {
								b.Skipf("encode unsupported on this profile/size: %v", err)
								return
							}
							zipped, err := comp.Compress(raw)
							if err != nil {
								b.Skipf("compress failed: %v", err)
								return
							}
							rawLen, compressedLen = len(raw), len(zipped)
						}

						samples := make([]time.Duration, 0, b.N)
						b.ResetTimer()
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							t0 := time.Now()
							raw, err := safeMarshal(enc.Marshal, md)
							if err != nil {
								b.Fatalf("Marshal: %v", err)
							}
							if _, err := comp.Compress(raw); err != nil {
								b.Fatalf("Compress: %v", err)
							}
							samples = append(samples, time.Since(t0))
						}
						b.StopTimer()
						// ReportMetric must be called AFTER the timing loop —
						// the testing framework clears the extras map between
						// calibration runs, so metrics set before the loop are
						// silently dropped from the final report.
						b.ReportMetric(float64(compressedLen), "compressed_bytes")
						if compressedLen > 0 {
							b.ReportMetric(float64(rawLen)/float64(compressedLen), "compression_ratio")
						}
						// bytes_per_record normalizes the on-wire size by the
						// number of datapoints (or spans) the record carries.
						// This is the shard-bandwidth-relevant number: doubling
						// a record from 100 to 1000 datapoints does not double
						// its bytes (Arrow's schema overhead amortizes,
						// dictionaries pack tighter), so bytes/datapoint is the
						// curve that decides "should I batch more?".
						if size > 0 {
							b.ReportMetric(float64(compressedLen)/float64(size), "compressed_bytes_per_record")
							b.ReportMetric(float64(rawLen)/float64(size), "raw_bytes_per_record")
						}
						reportLatencyStats(b, samples)
					})
				}
			}
		}
	}
}

// BenchmarkEncodeTraces mirrors BenchmarkEncodeMetrics for the typical-trace
// profile so OTLP-proto's traces baseline is also represented in the report.
func BenchmarkEncodeTraces(b *testing.B) {
	for _, size := range BatchSizes {
		td := GenerateTraces(size)
		for _, e := range benchEncodings {
			enc, err := encoding.NewTracesEncoder(e)
			if err != nil {
				b.Fatalf("NewTracesEncoder(%s): %v", e, err)
			}
			for _, c := range benchCodecs {
				comp, err := encoding.NewCompressor(c)
				if err != nil {
					b.Fatalf("NewCompressor(%s): %v", c, err)
				}
				name := fmt.Sprintf("%s/%s/%s/n=%d", TracesTypical, e, c, size)
				b.Run(name, func(b *testing.B) {
					var rawLen, compressedLen int
					for i := 0; i < warmupIterations; i++ {
						raw, err := safeMarshal(enc.Marshal, td)
						if err != nil {
							b.Skipf("encode unsupported on this profile/size: %v", err)
							return
						}
						zipped, err := comp.Compress(raw)
						if err != nil {
							b.Skipf("compress failed: %v", err)
							return
						}
						rawLen, compressedLen = len(raw), len(zipped)
					}

					samples := make([]time.Duration, 0, b.N)
					b.ResetTimer()
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						t0 := time.Now()
						raw, err := safeMarshal(enc.Marshal, td)
						if err != nil {
							b.Fatalf("Marshal: %v", err)
						}
						if _, err := comp.Compress(raw); err != nil {
							b.Fatalf("Compress: %v", err)
						}
						samples = append(samples, time.Since(t0))
					}
					b.StopTimer()
					b.ReportMetric(float64(compressedLen), "compressed_bytes")
					if compressedLen > 0 {
						b.ReportMetric(float64(rawLen)/float64(compressedLen), "compression_ratio")
					}
					if size > 0 {
						b.ReportMetric(float64(compressedLen)/float64(size), "compressed_bytes_per_record")
						b.ReportMetric(float64(rawLen)/float64(size), "raw_bytes_per_record")
					}
					reportLatencyStats(b, samples)
				})
			}
		}
	}
}
