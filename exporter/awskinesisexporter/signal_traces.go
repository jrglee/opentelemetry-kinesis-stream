package awskinesisexporter

// Traces signal adapter for the generic record pipeline.
//
// pdata's ptrace type family is structurally independent of pmetric and plog —
// there is no common interface for ResourceSpans / ResourceMetrics /
// ResourceLogs — so every operation that produces or consumes trace data
// requires its own concrete implementation. The signalCodec[ptrace.Traces]
// returned by tracesCodec is what bridges this file into the signal-agnostic
// pipeline in record.go.

import (
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// tracesCodec adapts ptrace.Traces to the generic record pipeline.
func tracesCodec(enc encoding.TracesEncoder) signalCodec[ptrace.Traces] {
	return signalCodec[ptrace.Traces]{
		groupByTags:        groupTracesByTags,
		splitHalf:          splitTracesHalf,
		truncateAttributes: truncateTracesAttributes,
		marshal:            enc.Marshal,
		itemCount:          func(td ptrace.Traces) int { return td.SpanCount() },
	}
}

// truncateTracesAttributes returns a clone of td with every string attribute
// (resource, scope, span, event, link) clamped to maxBytes. The caller's td is
// not mutated — the clone is what gets re-marshaled.
func truncateTracesAttributes(td ptrace.Traces, maxBytes int) (ptrace.Traces, int) {
	out := ptrace.NewTraces()
	td.CopyTo(out)
	changed := 0
	rss := out.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		changed += clampStringAttrs(rs.Resource().Attributes(), maxBytes)
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			ss := sss.At(j)
			changed += clampStringAttrs(ss.Scope().Attributes(), maxBytes)
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				sp := spans.At(k)
				changed += clampStringAttrs(sp.Attributes(), maxBytes)
				events := sp.Events()
				for e := 0; e < events.Len(); e++ {
					changed += clampStringAttrs(events.At(e).Attributes(), maxBytes)
				}
				links := sp.Links()
				for l := 0; l < links.Len(); l++ {
					changed += clampStringAttrs(links.At(l).Attributes(), maxBytes)
				}
			}
		}
	}
	return out, changed
}

func groupTracesByTags(td ptrace.Traces, plan keyPlan) []taggedBatch[ptrace.Traces] {
	if len(plan) == 0 {
		return []taggedBatch[ptrace.Traces]{{key: "", batch: td}}
	}
	if !plan.resourceOnly() {
		return groupTracesByLeaf(td, plan)
	}
	return groupTracesByResource(td, plan)
}

func groupTracesByResource(td ptrace.Traces, plan keyPlan) []taggedBatch[ptrace.Traces] {
	byKey := map[string]ptrace.Traces{}
	var order []string
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		parts := resolveParts(plan, rs.Resource().Attributes(), "", emptyAttrs)
		key := joinParts(parts)
		dst, ok := byKey[key]
		if !ok {
			dst = ptrace.NewTraces()
			byKey[key] = dst
			order = append(order, key)
		}
		appended := dst.ResourceSpans().AppendEmpty()
		rs.CopyTo(appended)
		if plan.hasPromotion() {
			applyPromotions(plan, parts, appended.Resource().Attributes(), emptyAttrs)
		}
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
