package awskinesisexporter

import (
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// tagSep separates joined tag values in a partition-key seed. 0x1f (unit
// separator) cannot appear in attribute keys and is vanishingly unlikely in
// values, so it avoids "a"+"bc" colliding with "ab"+"c".
const tagSep = "\x1f"

// tagKey reads the ordered tags from a resource's attributes and joins them.
// A missing attribute contributes an empty segment so position is preserved.
func tagKey(attrs pcommon.Map, tags []string) string {
	parts := make([]string, len(tags))
	for i, t := range tags {
		if v, ok := attrs.Get(t); ok {
			parts[i] = v.AsString()
		}
	}
	return strings.Join(parts, tagSep)
}

// tracesCodec adapts ptrace.Traces to the generic record pipeline.
func tracesCodec(enc encoding.TracesEncoder) signalCodec[ptrace.Traces] {
	return signalCodec[ptrace.Traces]{
		groupByTags: groupTracesByTags,
		splitHalf:   splitTracesHalf,
		marshal:     enc.Marshal,
		itemCount:   func(td ptrace.Traces) int { return td.SpanCount() },
	}
}

func groupTracesByTags(td ptrace.Traces, tags []string) []taggedBatch[ptrace.Traces] {
	if len(tags) == 0 {
		return []taggedBatch[ptrace.Traces]{{key: "", batch: td}}
	}
	byKey := map[string]ptrace.Traces{}
	var order []string
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		key := tagKey(rs.Resource().Attributes(), tags)
		dst, ok := byKey[key]
		if !ok {
			dst = ptrace.NewTraces()
			byKey[key] = dst
			order = append(order, key)
		}
		rs.CopyTo(dst.ResourceSpans().AppendEmpty())
	}
	out := make([]taggedBatch[ptrace.Traces], 0, len(order))
	for _, k := range order {
		out = append(out, taggedBatch[ptrace.Traces]{key: k, batch: byKey[k]})
	}
	return out
}

// splitTracesHalf splits resources in half when there are several; otherwise
// it splits the single resource's spans (across all its scopes) in half. ok is
// false only for a single resource holding a single span. The input is never
// mutated — every move is a CopyTo into freshly allocated Traces.
func splitTracesHalf(td ptrace.Traces) (ptrace.Traces, ptrace.Traces, bool) {
	rss := td.ResourceSpans()
	if rss.Len() > 1 {
		mid := rss.Len() / 2
		a, b := ptrace.NewTraces(), ptrace.NewTraces()
		for i := 0; i < rss.Len(); i++ {
			if i < mid {
				rss.At(i).CopyTo(a.ResourceSpans().AppendEmpty())
			} else {
				rss.At(i).CopyTo(b.ResourceSpans().AppendEmpty())
			}
		}
		return a, b, true
	}
	if rss.Len() == 0 || td.SpanCount() <= 1 {
		return ptrace.Traces{}, ptrace.Traces{}, false
	}
	// Single resource, multiple spans: split the flattened span list in half,
	// preserving resource and scope identity on both sides.
	rs := rss.At(0)
	mid := td.SpanCount() / 2
	a, b := ptrace.NewTraces(), ptrace.NewTraces()
	ra, rb := a.ResourceSpans().AppendEmpty(), b.ResourceSpans().AppendEmpty()
	rs.Resource().CopyTo(ra.Resource())
	rs.Resource().CopyTo(rb.Resource())
	ra.SetSchemaUrl(rs.SchemaUrl())
	rb.SetSchemaUrl(rs.SchemaUrl())
	seen := 0
	sss := rs.ScopeSpans()
	for i := 0; i < sss.Len(); i++ {
		ss := sss.At(i)
		var da, db ptrace.ScopeSpans
		spans := ss.Spans()
		for j := 0; j < spans.Len(); j++ {
			if seen < mid {
				if da == (ptrace.ScopeSpans{}) {
					da = ra.ScopeSpans().AppendEmpty()
					ss.Scope().CopyTo(da.Scope())
					da.SetSchemaUrl(ss.SchemaUrl())
				}
				spans.At(j).CopyTo(da.Spans().AppendEmpty())
			} else {
				if db == (ptrace.ScopeSpans{}) {
					db = rb.ScopeSpans().AppendEmpty()
					ss.Scope().CopyTo(db.Scope())
					db.SetSchemaUrl(ss.SchemaUrl())
				}
				spans.At(j).CopyTo(db.Spans().AppendEmpty())
			}
			seen++
		}
	}
	return a, b, true
}

// metricsCodec adapts pmetric.Metrics to the generic record pipeline.
func metricsCodec(enc encoding.MetricsEncoder) signalCodec[pmetric.Metrics] {
	return signalCodec[pmetric.Metrics]{
		groupByTags: groupMetricsByTags,
		splitHalf:   splitMetricsHalf,
		marshal:     enc.Marshal,
		itemCount:   func(md pmetric.Metrics) int { return md.DataPointCount() },
	}
}

func groupMetricsByTags(md pmetric.Metrics, tags []string) []taggedBatch[pmetric.Metrics] {
	if len(tags) == 0 {
		return []taggedBatch[pmetric.Metrics]{{key: "", batch: md}}
	}
	byKey := map[string]pmetric.Metrics{}
	var order []string
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		key := tagKey(rm.Resource().Attributes(), tags)
		dst, ok := byKey[key]
		if !ok {
			dst = pmetric.NewMetrics()
			byKey[key] = dst
			order = append(order, key)
		}
		rm.CopyTo(dst.ResourceMetrics().AppendEmpty())
	}
	out := make([]taggedBatch[pmetric.Metrics], 0, len(order))
	for _, k := range order {
		out = append(out, taggedBatch[pmetric.Metrics]{key: k, batch: byKey[k]})
	}
	return out
}

// splitMetricsHalf mirrors splitTracesHalf: split resources when there are
// several, else split the single resource's metrics (across all scopes) in
// half. A metric is the indivisible leaf here. ok is false for one resource
// holding one metric. The input is never mutated.
func splitMetricsHalf(md pmetric.Metrics) (pmetric.Metrics, pmetric.Metrics, bool) {
	rms := md.ResourceMetrics()
	if rms.Len() > 1 {
		mid := rms.Len() / 2
		a, b := pmetric.NewMetrics(), pmetric.NewMetrics()
		for i := 0; i < rms.Len(); i++ {
			if i < mid {
				rms.At(i).CopyTo(a.ResourceMetrics().AppendEmpty())
			} else {
				rms.At(i).CopyTo(b.ResourceMetrics().AppendEmpty())
			}
		}
		return a, b, true
	}
	if rms.Len() == 0 {
		return pmetric.Metrics{}, pmetric.Metrics{}, false
	}
	rm := rms.At(0)
	total := metricCount(rm)
	if total <= 1 {
		return pmetric.Metrics{}, pmetric.Metrics{}, false
	}
	mid := total / 2
	a, b := pmetric.NewMetrics(), pmetric.NewMetrics()
	ra, rb := a.ResourceMetrics().AppendEmpty(), b.ResourceMetrics().AppendEmpty()
	rm.Resource().CopyTo(ra.Resource())
	rm.Resource().CopyTo(rb.Resource())
	ra.SetSchemaUrl(rm.SchemaUrl())
	rb.SetSchemaUrl(rm.SchemaUrl())
	seen := 0
	sms := rm.ScopeMetrics()
	for i := 0; i < sms.Len(); i++ {
		sm := sms.At(i)
		var da, db pmetric.ScopeMetrics
		ms := sm.Metrics()
		for j := 0; j < ms.Len(); j++ {
			if seen < mid {
				if da == (pmetric.ScopeMetrics{}) {
					da = ra.ScopeMetrics().AppendEmpty()
					sm.Scope().CopyTo(da.Scope())
					da.SetSchemaUrl(sm.SchemaUrl())
				}
				ms.At(j).CopyTo(da.Metrics().AppendEmpty())
			} else {
				if db == (pmetric.ScopeMetrics{}) {
					db = rb.ScopeMetrics().AppendEmpty()
					sm.Scope().CopyTo(db.Scope())
					db.SetSchemaUrl(sm.SchemaUrl())
				}
				ms.At(j).CopyTo(db.Metrics().AppendEmpty())
			}
			seen++
		}
	}
	return a, b, true
}

// metricCount counts metrics (the split leaf) under a resource.
func metricCount(rm pmetric.ResourceMetrics) int {
	n := 0
	sms := rm.ScopeMetrics()
	for i := 0; i < sms.Len(); i++ {
		n += sms.At(i).Metrics().Len()
	}
	return n
}
