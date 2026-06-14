// Package perf is the reproducible benchmarking harness for the encoding ×
// codec matrix. Datasets are generated deterministically from a fixed seed so
// that compressed-size and compression-ratio numbers are bit-for-bit stable
// across runs, machine architectures, and CI environments. Wall-clock numbers
// (ns/op) vary with hardware, but the ordering between encodings on a single
// host is the load-bearing comparison.
//
// Caveat on the per-iteration percentiles (`min_ns`, `p50_ns`, `p90_ns`,
// `max_ns`): they come from wrapping each call in `time.Now()` / `time.Since`,
// which itself costs ~40–80 ns. Below ~1 µs/op the timer overhead is a
// material fraction of the measurement; treat sub-µs percentile numbers as
// indicative, not precise. The standard `ns/op` (mean across many iterations
// inside one `time.Now()`) is reliable at any scale.
//
//go:build perf

package perf

import (
	"fmt"
	"math/rand/v2"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// PerfSeed is the single source of randomness for every dataset generator.
// Fixed value → byte-identical inputs on every architecture. Do not derive any
// per-profile seeds from a clock; if a future profile needs an independent
// stream, derive it from PerfSeed and the profile name.
const PerfSeed uint64 = 0xC0FFEEBABE

// Profile names one of the synthetic telemetry shapes the harness exercises.
// Each profile is a pure function of (profile, batchSize, seed) → pdata.
type Profile string

const (
	// MetricsHighCardinality models per-request or per-pod attributes —
	// many unique attribute combinations, few datapoints per series. This is
	// the shape where Arrow's dictionary encoding *should struggle* under
	// fresh-producer-per-record because there's little intra-record repetition
	// to amortize the schema overhead against.
	MetricsHighCardinality Profile = "metrics-high-cardinality"

	// MetricsHighFrequency models infrastructure metrics — a small
	// attribute fan-out with many datapoints per series. Dictionary
	// encoding within a single record has plenty of repeated label values
	// to compress; this is where Arrow *should shine* on this transport.
	MetricsHighFrequency Profile = "metrics-high-frequency"

	// MetricsBalanced is the middle of the road — moderate cardinality,
	// moderate frequency. Sanity check that the conclusions from the two
	// extremes don't depend on a pathological choice of dataset.
	MetricsBalanced Profile = "metrics-balanced"

	// TracesTypical is a single representative trace shape so the harness
	// reports an apples-to-apples encode/decode comparison for the signal
	// the OTLP-proto baseline was tuned for.
	TracesTypical Profile = "traces-typical"
)

// MetricsProfiles is the iteration order used by the benchmark tables.
var MetricsProfiles = []Profile{MetricsHighCardinality, MetricsHighFrequency, MetricsBalanced}

// BatchSizes are the per-record datapoint (or span) counts the benchmarks
// sweep. The small end models single-event records; the large end (100k) is
// already past any reasonable Kinesis per-record byte ceiling but is
// retained so the harness shows where each encoding's encode/decode cost
// scales linearly and where (e.g. JSON allocation) it does not. 1M was
// tried and removed: Arrow at 1M takes ~3s per call and bloats CI total
// runtime past any reasonable budget without changing the conclusions.
var BatchSizes = []int{1, 10, 100, 1000, 10000, 100000}

// Vocabularies are picked from by index so attribute values are deterministic
// (no map iteration, no time.Now). Keeping them small and finite makes the
// resulting strings highly compressible by both dictionary-style (Arrow) and
// LZ-style (zstd) codecs — which is what real observability workloads look
// like.
var (
	hosts     = []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"}
	regions   = []string{"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-2"}
	services  = []string{"checkout", "search", "catalog", "payments", "auth", "shipping"}
	envs      = []string{"prod", "stage", "dev"}
	versions  = []string{"v1.0.0", "v1.1.3", "v2.0.0", "v2.1.0"}
	endpoints = []string{"/v1/cart", "/v1/items", "/v1/users", "/v1/search", "/healthz", "/metrics"}
)

// GenerateMetrics builds a pmetric.Metrics with batchSize datapoints arranged
// according to the named profile. The same (profile, batchSize) always returns
// pdata that marshals to the same bytes — no clocks, no global RNG, no map
// iteration order in the output shape.
func GenerateMetrics(profile Profile, batchSize int) pmetric.Metrics {
	if batchSize <= 0 {
		panic(fmt.Sprintf("perf: invalid batchSize %d", batchSize))
	}
	r := rand.New(rand.NewPCG(PerfSeed, uint64(hash32(string(profile)))))

	switch profile {
	case MetricsHighCardinality:
		return generateHighCardinality(r, batchSize)
	case MetricsHighFrequency:
		return generateHighFrequency(r, batchSize)
	case MetricsBalanced:
		return generateBalanced(r, batchSize)
	default:
		panic(fmt.Sprintf("perf: unknown metrics profile %q", profile))
	}
}

// GenerateTraces builds a ptrace.Traces with batchSize spans for the typical
// trace shape. Same determinism guarantees as GenerateMetrics.
func GenerateTraces(batchSize int) ptrace.Traces {
	if batchSize <= 0 {
		panic(fmt.Sprintf("perf: invalid batchSize %d", batchSize))
	}
	r := rand.New(rand.NewPCG(PerfSeed, uint64(hash32(string(TracesTypical)))))

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	a := rs.Resource().Attributes()
	a.PutStr("service.name", pick(r, services))
	a.PutStr("deployment.environment", pick(r, envs))
	a.PutStr("service.version", pick(r, versions))

	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("perf.tracer")
	spans := ss.Spans()
	spans.EnsureCapacity(batchSize)

	// Counter-derived timestamps — never time.Now. tickNanos is arbitrary;
	// keeping it constant per dataset keeps the bytes deterministic.
	const tickNanos uint64 = 1_000_000
	for i := 0; i < batchSize; i++ {
		s := spans.AppendEmpty()
		s.SetName(fmt.Sprintf("%s %s", pick(r, []string{"GET", "POST", "PUT"}), pick(r, endpoints)))
		var tid [16]byte
		var sid [8]byte
		fillBytes(r, tid[:])
		fillBytes(r, sid[:])
		s.SetTraceID(tid)
		s.SetSpanID(sid)
		start := uint64(i) * tickNanos
		end := start + 500_000
		s.SetStartTimestamp(pcommon.Timestamp(start))
		s.SetEndTimestamp(pcommon.Timestamp(end))
		s.Attributes().PutStr("http.route", pick(r, endpoints))
		s.Attributes().PutInt("http.status_code", int64(200+(i%5)*100))
		s.Attributes().PutStr("net.peer.name", pick(r, hosts))
	}
	return td
}

// generateHighCardinality: every datapoint carries a unique attribute set —
// modeled by giving each datapoint a synthetic request_id, user_id, and trace
// fragment alongside the small vocabularies. The result is many distinct
// label-value combinations per record, which dictionary encoding cannot
// amortize as effectively as it can in low-cardinality streams.
func generateHighCardinality(r *rand.Rand, batchSize int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", pick(r, services))
	rm.Resource().Attributes().PutStr("deployment.environment", pick(r, envs))

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("perf.high-cardinality")

	m := sm.Metrics().AppendEmpty()
	m.SetName("http.server.request.duration")
	m.SetUnit("ms")
	dps := m.SetEmptyHistogram().DataPoints()
	dps.EnsureCapacity(batchSize)

	const tickNanos uint64 = 1_000_000
	for i := 0; i < batchSize; i++ {
		dp := dps.AppendEmpty()
		dp.SetStartTimestamp(pcommon.Timestamp(uint64(i) * tickNanos))
		dp.SetTimestamp(pcommon.Timestamp(uint64(i)*tickNanos + 500_000))
		dp.SetCount(uint64(1 + (i % 17)))
		dp.SetSum(float64(i) * 1.5)
		// High-cardinality attrs: unique per datapoint.
		dp.Attributes().PutStr("request.id", fmt.Sprintf("req-%010d-%04x", i, r.Uint32()&0xFFFF))
		dp.Attributes().PutStr("user.id", fmt.Sprintf("u-%08x", r.Uint32()))
		dp.Attributes().PutStr("http.route", pick(r, endpoints))
		dp.Attributes().PutStr("http.method", pick(r, []string{"GET", "POST", "PUT", "DELETE"}))
		dp.Attributes().PutInt("http.status_code", int64(200+(i%5)*100))
		dp.Attributes().PutStr("host.name", pick(r, hosts))
	}
	return md
}

// generateHighFrequency: small attribute fan-out (one of N hosts × one of M
// regions, etc.) but many datapoints — the same labels recur, so dictionary
// encoding has a lot to compress within a single record.
func generateHighFrequency(r *rand.Rand, batchSize int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", pick(r, services))

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("perf.high-frequency")

	m := sm.Metrics().AppendEmpty()
	m.SetName("system.cpu.utilization")
	m.SetUnit("1")
	dps := m.SetEmptyGauge().DataPoints()
	dps.EnsureCapacity(batchSize)

	const tickNanos uint64 = 100_000
	for i := 0; i < batchSize; i++ {
		dp := dps.AppendEmpty()
		dp.SetTimestamp(pcommon.Timestamp(uint64(i) * tickNanos))
		dp.SetDoubleValue(float64(r.IntN(1000)) / 1000.0)
		dp.Attributes().PutStr("host.name", hosts[i%len(hosts)])
		dp.Attributes().PutStr("cloud.region", regions[i%len(regions)])
		dp.Attributes().PutStr("state", []string{"user", "system", "idle"}[i%3])
	}
	return md
}

// generateBalanced sits between the two extremes: a moderate vocabulary of
// per-datapoint attributes with some recurrence.
func generateBalanced(r *rand.Rand, batchSize int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", pick(r, services))
	rm.Resource().Attributes().PutStr("deployment.environment", pick(r, envs))
	rm.Resource().Attributes().PutStr("service.version", pick(r, versions))

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("perf.balanced")

	m := sm.Metrics().AppendEmpty()
	m.SetName("app.events.count")
	dps := m.SetEmptySum().DataPoints()
	dps.EnsureCapacity(batchSize)

	const tickNanos uint64 = 500_000
	for i := 0; i < batchSize; i++ {
		dp := dps.AppendEmpty()
		dp.SetTimestamp(pcommon.Timestamp(uint64(i) * tickNanos))
		dp.SetIntValue(int64(1 + r.IntN(1000)))
		dp.Attributes().PutStr("host.name", hosts[i%len(hosts)])
		dp.Attributes().PutStr("endpoint", endpoints[i%len(endpoints)])
		dp.Attributes().PutStr("event.kind", []string{"created", "updated", "deleted", "queried"}[i%4])
		dp.Attributes().PutStr("tenant", fmt.Sprintf("t-%03d", i%50))
	}
	return md
}

func pick(r *rand.Rand, v []string) string { return v[r.IntN(len(v))] }

func fillBytes(r *rand.Rand, out []byte) {
	for i := range out {
		out[i] = byte(r.UintN(256))
	}
}

// hash32 is FNV-1a 32-bit; used only to derive a per-profile PCG seed from
// PerfSeed and the profile name. Stable across architectures.
func hash32(s string) uint32 {
	const (
		offset uint32 = 2166136261
		prime  uint32 = 16777619
	)
	h := offset
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime
	}
	return h
}
