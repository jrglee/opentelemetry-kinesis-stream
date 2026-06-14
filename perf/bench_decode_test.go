//go:build perf

package perf

import (
	"fmt"
	"testing"
	"time"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// BenchmarkDecodeMetrics measures the receiver-side decompress + decode cost.
// Inputs are pre-encoded at construction time so the timer captures only the
// receiver path — the symmetric measurement to BenchmarkEncodeMetrics.
func BenchmarkDecodeMetrics(b *testing.B) {
	for _, profile := range MetricsProfiles {
		for _, size := range BatchSizes {
			md := GenerateMetrics(profile, size)
			for _, e := range benchEncodings {
				enc, err := encoding.NewMetricsEncoder(e)
				if err != nil {
					b.Fatalf("NewMetricsEncoder(%s): %v", e, err)
				}
				dec, err := encoding.NewMetricsDecoder(e)
				if err != nil {
					b.Fatalf("NewMetricsDecoder(%s): %v", e, err)
				}
				for _, c := range benchCodecs {
					comp, err := encoding.NewCompressor(c)
					if err != nil {
						b.Fatalf("NewCompressor(%s): %v", c, err)
					}
					raw, err := enc.Marshal(md)
					if err != nil {
						name := fmt.Sprintf("%s/%s/%s/n=%d", profile, e, c, size)
						b.Run(name, func(b *testing.B) {
							b.Skipf("seed encode unsupported: %v", err)
						})
						continue
					}
					payload, err := comp.Compress(raw)
					if err != nil {
						b.Fatalf("seed Compress: %v", err)
					}

					name := fmt.Sprintf("%s/%s/%s/n=%d", profile, e, c, size)
					b.Run(name, func(b *testing.B) {
						for i := 0; i < warmupIterations; i++ {
							unzipped, err := comp.Decompress(payload)
							if err != nil {
								b.Skipf("warmup Decompress: %v", err)
								return
							}
							if _, err := dec.Unmarshal(unzipped); err != nil {
								b.Skipf("warmup Unmarshal: %v", err)
								return
							}
						}
						samples := make([]time.Duration, 0, b.N)
						b.ResetTimer()
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							t0 := time.Now()
							unzipped, err := comp.Decompress(payload)
							if err != nil {
								b.Fatalf("Decompress: %v", err)
							}
							if _, err := dec.Unmarshal(unzipped); err != nil {
								b.Fatalf("Unmarshal: %v", err)
							}
							samples = append(samples, time.Since(t0))
						}
						b.StopTimer()
						b.ReportMetric(float64(len(payload)), "compressed_bytes")
						if size > 0 {
							b.ReportMetric(float64(len(payload))/float64(size), "compressed_bytes_per_record")
							b.ReportMetric(float64(len(raw))/float64(size), "raw_bytes_per_record")
						}
						reportLatencyStats(b, samples)
					})
				}
			}
		}
	}
}

// BenchmarkDecodeTraces mirrors BenchmarkDecodeMetrics for the typical-trace
// profile.
func BenchmarkDecodeTraces(b *testing.B) {
	for _, size := range BatchSizes {
		td := GenerateTraces(size)
		for _, e := range benchEncodings {
			enc, err := encoding.NewTracesEncoder(e)
			if err != nil {
				b.Fatalf("NewTracesEncoder(%s): %v", e, err)
			}
			dec, err := encoding.NewTracesDecoder(e)
			if err != nil {
				b.Fatalf("NewTracesDecoder(%s): %v", e, err)
			}
			for _, c := range benchCodecs {
				comp, err := encoding.NewCompressor(c)
				if err != nil {
					b.Fatalf("NewCompressor(%s): %v", c, err)
				}
				raw, err := enc.Marshal(td)
				if err != nil {
					name := fmt.Sprintf("%s/%s/%s/n=%d", TracesTypical, e, c, size)
					b.Run(name, func(b *testing.B) {
						b.Skipf("seed encode unsupported: %v", err)
					})
					continue
				}
				payload, err := comp.Compress(raw)
				if err != nil {
					b.Fatalf("seed Compress: %v", err)
				}

				name := fmt.Sprintf("%s/%s/%s/n=%d", TracesTypical, e, c, size)
				b.Run(name, func(b *testing.B) {
					for i := 0; i < warmupIterations; i++ {
						unzipped, err := comp.Decompress(payload)
						if err != nil {
							b.Skipf("warmup Decompress: %v", err)
							return
						}
						if _, err := dec.Unmarshal(unzipped); err != nil {
							b.Skipf("warmup Unmarshal: %v", err)
							return
						}
					}

					samples := make([]time.Duration, 0, b.N)
					b.ResetTimer()
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						t0 := time.Now()
						unzipped, err := comp.Decompress(payload)
						if err != nil {
							b.Fatalf("Decompress: %v", err)
						}
						if _, err := dec.Unmarshal(unzipped); err != nil {
							b.Fatalf("Unmarshal: %v", err)
						}
						samples = append(samples, time.Since(t0))
					}
					b.StopTimer()
					b.ReportMetric(float64(len(payload)), "compressed_bytes")
					if size > 0 {
						b.ReportMetric(float64(len(payload))/float64(size), "compressed_bytes_per_record")
						b.ReportMetric(float64(len(raw))/float64(size), "raw_bytes_per_record")
					}
					reportLatencyStats(b, samples)
				})
			}
		}
	}
}
