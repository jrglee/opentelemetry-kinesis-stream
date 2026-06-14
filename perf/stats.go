//go:build perf

package perf

import (
	"sort"
	"testing"
	"time"
)

// percentile returns the value at fraction f (0..1) of a sorted slice of
// durations. Caller must pre-sort. Uses nearest-rank — simple and stable, no
// interpolation between samples.
func percentile(sorted []time.Duration, f float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(f * float64(len(sorted)-1))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// reportLatencyStats sorts samples in place and reports min/p50/p90/max as
// extra benchmark metrics (all in nanoseconds). The standard `ns/op` Go
// reports is the mean; these four widen the view to per-call variance
// without baking the variance into the score itself. Sample count is the
// per-case `b.N` chosen by the testing framework — for very slow cases
// this can be 1, in which case all four numbers collapse to the single
// observation.
func reportLatencyStats(b *testing.B, samples []time.Duration) {
	b.Helper()
	if len(samples) == 0 {
		return
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	b.ReportMetric(float64(samples[0].Nanoseconds()), "min_ns")
	b.ReportMetric(float64(percentile(samples, 0.50).Nanoseconds()), "p50_ns")
	b.ReportMetric(float64(percentile(samples, 0.90).Nanoseconds()), "p90_ns")
	b.ReportMetric(float64(samples[len(samples)-1].Nanoseconds()), "max_ns")
	b.ReportMetric(float64(len(samples)), "samples")
}
