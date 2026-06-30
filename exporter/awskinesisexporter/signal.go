package awskinesisexporter

import (
	"unicode/utf8"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// tagSep separates joined tag values in a partition-key seed. 0x1f (unit
// separator) cannot appear in attribute keys and is vanishingly unlikely in
// values, so it avoids "a"+"bc" colliding with "ab"+"c".
const tagSep = "\x1f"

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

// clampStringAttrs truncates every string-valued attribute in m whose UTF-8
// byte length exceeds maxBytes, in place. Returns the number of values changed.
// Non-string kinds (bool, int, double, bytes, slice, map) are never touched —
// numeric values do not bloat records, and mutating structured kinds would
// rewrite semantics rather than trim them. The truncation backsteps to a
// codepoint boundary so the output remains valid UTF-8; otlp_json silently
// substitutes the replacement character on invalid sequences, which is worse
// than emitting a few fewer bytes than maxBytes.
func clampStringAttrs(m pcommon.Map, maxBytes int) int {
	changed := 0
	m.Range(func(_ string, v pcommon.Value) bool {
		if v.Type() != pcommon.ValueTypeStr {
			return true
		}
		s := v.Str()
		if len(s) <= maxBytes {
			return true
		}
		v.SetStr(s[:utf8SafeCut(s, maxBytes)])
		changed++
		return true
	})
	return changed
}

// utf8SafeCut returns the largest n <= maxBytes such that s[:n] ends on a
// UTF-8 codepoint boundary. A mid-codepoint cut produces invalid UTF-8 that
// strict encoders reject; we'd rather drop a few bytes than ship garbage.
// Caller has already established len(s) > maxBytes.
func utf8SafeCut(s string, maxBytes int) int {
	// Scan backward at most 3 bytes — UTF-8 codepoints are 1-4 bytes, so the
	// cut is at most 3 bytes before maxBytes.
	for n := maxBytes; n > maxBytes-4 && n > 0; n-- {
		if utf8.RuneStart(s[n]) {
			return n
		}
	}
	return maxBytes
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

// metricsCodec adapts pmetric.Metrics to the generic record pipeline.
func metricsCodec(enc encoding.MetricsEncoder) signalCodec[pmetric.Metrics] {
	return signalCodec[pmetric.Metrics]{
		groupByTags:        groupMetricsByTags,
		splitHalf:          splitMetricsHalf,
		truncateAttributes: truncateMetricsAttributes,
		marshal:            enc.Marshal,
		itemCount:          func(md pmetric.Metrics) int { return md.DataPointCount() },
	}
}

// truncateMetricsAttributes returns a clone of md with every string attribute
// (resource, scope, and every datapoint across gauge/sum/histogram/exp-histogram/
// summary) clamped to maxBytes. Exemplar attributes are also walked because they
// are arbitrary user-supplied dimensions that can drive the same bloat.
func truncateMetricsAttributes(md pmetric.Metrics, maxBytes int) (pmetric.Metrics, int) {
	out := pmetric.NewMetrics()
	md.CopyTo(out)
	changed := 0
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		changed += clampStringAttrs(rm.Resource().Attributes(), maxBytes)
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			changed += clampStringAttrs(sm.Scope().Attributes(), maxBytes)
			ms := sm.Metrics()
			for k := 0; k < ms.Len(); k++ {
				changed += clampMetricDataPoints(ms.At(k), maxBytes)
			}
		}
	}
	return out, changed
}

// clampMetricDataPoints walks all datapoint kinds in m and clamps their
// attribute strings. Each metric kind exposes datapoints under a different
// type-discriminated field, so the switch is unavoidable.
func clampMetricDataPoints(m pmetric.Metric, maxBytes int) int {
	changed := 0
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			changed += clampStringAttrs(dp.Attributes(), maxBytes)
			changed += clampNumberExemplars(dp.Exemplars(), maxBytes)
		}
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			changed += clampStringAttrs(dp.Attributes(), maxBytes)
			changed += clampNumberExemplars(dp.Exemplars(), maxBytes)
		}
	case pmetric.MetricTypeHistogram:
		dps := m.Histogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			changed += clampStringAttrs(dp.Attributes(), maxBytes)
			changed += clampNumberExemplars(dp.Exemplars(), maxBytes)
		}
	case pmetric.MetricTypeExponentialHistogram:
		dps := m.ExponentialHistogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			changed += clampStringAttrs(dp.Attributes(), maxBytes)
			changed += clampNumberExemplars(dp.Exemplars(), maxBytes)
		}
	case pmetric.MetricTypeSummary:
		dps := m.Summary().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			changed += clampStringAttrs(dps.At(i).Attributes(), maxBytes)
		}
	}
	return changed
}

func clampNumberExemplars(exs pmetric.ExemplarSlice, maxBytes int) int {
	changed := 0
	for i := 0; i < exs.Len(); i++ {
		changed += clampStringAttrs(exs.At(i).FilteredAttributes(), maxBytes)
	}
	return changed
}

func groupMetricsByTags(md pmetric.Metrics, plan keyPlan) []taggedBatch[pmetric.Metrics] {
	if len(plan) == 0 {
		return []taggedBatch[pmetric.Metrics]{{key: "", batch: md}}
	}
	if !plan.resourceOnly() {
		return groupMetricsByLeaf(md, plan)
	}
	return groupMetricsByResource(md, plan)
}

func groupMetricsByResource(md pmetric.Metrics, plan keyPlan) []taggedBatch[pmetric.Metrics] {
	byKey := map[string]pmetric.Metrics{}
	var order []string
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		parts := resolveParts(plan, rm.Resource().Attributes(), "", emptyAttrs)
		key := joinParts(parts)
		dst, ok := byKey[key]
		if !ok {
			dst = pmetric.NewMetrics()
			byKey[key] = dst
			order = append(order, key)
		}
		appended := dst.ResourceMetrics().AppendEmpty()
		rm.CopyTo(appended)
		if plan.hasPromotion() {
			applyPromotions(plan, parts, appended.Resource().Attributes(), emptyAttrs)
		}
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

// logsCodec adapts plog.Logs to the generic record pipeline.
func logsCodec(enc encoding.LogsEncoder) signalCodec[plog.Logs] {
	return signalCodec[plog.Logs]{
		groupByTags:        groupLogsByTags,
		splitHalf:          splitLogsHalf,
		truncateAttributes: truncateLogsAttributes,
		marshal:            enc.Marshal,
		itemCount:          func(ld plog.Logs) int { return ld.LogRecordCount() },
	}
}

// truncateLogsAttributes returns a clone of ld with every string-valued
// attribute (resource, scope, log record) clamped to maxBytes. The log
// record's Body is deliberately left untouched: the policy is named
// truncate_attribute_values, the attributes_truncated counter is labelled
// the same way, and operators would not expect a metric increment under
// that name to mean their log message text was mutated. A future opt-in
// policy can clamp bodies if that recovery path is needed.
func truncateLogsAttributes(ld plog.Logs, maxBytes int) (plog.Logs, int) {
	out := plog.NewLogs()
	ld.CopyTo(out)
	changed := 0
	rls := out.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		changed += clampStringAttrs(rl.Resource().Attributes(), maxBytes)
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			sl := sls.At(j)
			changed += clampStringAttrs(sl.Scope().Attributes(), maxBytes)
			lrs := sl.LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				changed += clampStringAttrs(lrs.At(k).Attributes(), maxBytes)
			}
		}
	}
	return out, changed
}

func groupLogsByTags(ld plog.Logs, plan keyPlan) []taggedBatch[plog.Logs] {
	if len(plan) == 0 {
		return []taggedBatch[plog.Logs]{{key: "", batch: ld}}
	}
	if !plan.resourceOnly() {
		return groupLogsByLeaf(ld, plan)
	}
	return groupLogsByResource(ld, plan)
}

func groupLogsByResource(ld plog.Logs, plan keyPlan) []taggedBatch[plog.Logs] {
	byKey := map[string]plog.Logs{}
	var order []string
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		parts := resolveParts(plan, rl.Resource().Attributes(), "", emptyAttrs)
		key := joinParts(parts)
		dst, ok := byKey[key]
		if !ok {
			dst = plog.NewLogs()
			byKey[key] = dst
			order = append(order, key)
		}
		appended := dst.ResourceLogs().AppendEmpty()
		rl.CopyTo(appended)
		if plan.hasPromotion() {
			applyPromotions(plan, parts, appended.Resource().Attributes(), emptyAttrs)
		}
	}
	out := make([]taggedBatch[plog.Logs], 0, len(order))
	for _, k := range order {
		out = append(out, taggedBatch[plog.Logs]{key: k, batch: byKey[k]})
	}
	return out
}

// splitLogsHalf mirrors splitTracesHalf: split resources when there are
// several, else split the single resource's log records (across all scopes)
// in half. A LogRecord is the indivisible leaf. ok is false for one resource
// holding one log record. The input is never mutated.
func splitLogsHalf(ld plog.Logs) (plog.Logs, plog.Logs, bool) {
	rls := ld.ResourceLogs()
	if rls.Len() > 1 {
		mid := rls.Len() / 2
		a, b := plog.NewLogs(), plog.NewLogs()
		for i := 0; i < rls.Len(); i++ {
			if i < mid {
				rls.At(i).CopyTo(a.ResourceLogs().AppendEmpty())
			} else {
				rls.At(i).CopyTo(b.ResourceLogs().AppendEmpty())
			}
		}
		return a, b, true
	}
	if rls.Len() == 0 || ld.LogRecordCount() <= 1 {
		return plog.Logs{}, plog.Logs{}, false
	}
	rl := rls.At(0)
	mid := ld.LogRecordCount() / 2
	a, b := plog.NewLogs(), plog.NewLogs()
	ra, rb := a.ResourceLogs().AppendEmpty(), b.ResourceLogs().AppendEmpty()
	rl.Resource().CopyTo(ra.Resource())
	rl.Resource().CopyTo(rb.Resource())
	ra.SetSchemaUrl(rl.SchemaUrl())
	rb.SetSchemaUrl(rl.SchemaUrl())
	seen := 0
	sls := rl.ScopeLogs()
	for i := 0; i < sls.Len(); i++ {
		sl := sls.At(i)
		var da, db plog.ScopeLogs
		lrs := sl.LogRecords()
		for j := 0; j < lrs.Len(); j++ {
			if seen < mid {
				if da == (plog.ScopeLogs{}) {
					da = ra.ScopeLogs().AppendEmpty()
					sl.Scope().CopyTo(da.Scope())
					da.SetSchemaUrl(sl.SchemaUrl())
				}
				lrs.At(j).CopyTo(da.LogRecords().AppendEmpty())
			} else {
				if db == (plog.ScopeLogs{}) {
					db = rb.ScopeLogs().AppendEmpty()
					sl.Scope().CopyTo(db.Scope())
					db.SetSchemaUrl(sl.SchemaUrl())
				}
				lrs.At(j).CopyTo(db.LogRecords().AppendEmpty())
			}
			seen++
		}
	}
	return a, b, true
}
