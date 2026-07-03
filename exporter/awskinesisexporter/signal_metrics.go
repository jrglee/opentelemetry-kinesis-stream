package awskinesisexporter

// Metrics signal adapter for the generic record pipeline.
//
// pdata's pmetric type family is structurally independent of ptrace and plog.
// Metrics carry an extra level of hierarchy below the scope (the Metric shell)
// and expose datapoints through a type-discriminated switch rather than a
// uniform slice, which forces the parallel implementations here. The
// signalCodec[pmetric.Metrics] returned by metricsCodec bridges this file into
// the signal-agnostic pipeline in record.go.

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

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
	if total == 0 {
		return pmetric.Metrics{}, pmetric.Metrics{}, false
	}
	if total == 1 {
		// A single metric: the only remaining axis is its datapoints. Split those
		// in half so an oversize single-metric record (the common shape when
		// partitioning by a datapoint attribute) stays recoverable instead of
		// being dropped as irreducible.
		return splitSingleMetricDatapoints(rm)
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

// splitSingleMetricDatapoints splits the datapoints of a resource holding
// exactly one metric into two halves, preserving resource/scope/metric identity
// and the metric shell on both sides. ok is false only when the metric has a
// single (indivisible) datapoint. The input is never mutated.
func splitSingleMetricDatapoints(rm pmetric.ResourceMetrics) (pmetric.Metrics, pmetric.Metrics, bool) {
	var srcSM pmetric.ScopeMetrics
	var srcM pmetric.Metric
	sms := rm.ScopeMetrics()
	for i := 0; i < sms.Len(); i++ {
		if sms.At(i).Metrics().Len() > 0 {
			srcSM = sms.At(i)
			srcM = srcSM.Metrics().At(0)
			break
		}
	}

	n := 0
	eachMetricDataPoint(srcM, func(pcommon.Map, func(pmetric.Metric) pcommon.Map) { n++ })
	if n <= 1 {
		return pmetric.Metrics{}, pmetric.Metrics{}, false
	}

	mid := n / 2
	a, b := pmetric.NewMetrics(), pmetric.NewMetrics()
	ma := buildMetricShell(a, rm, srcSM, srcM)
	mb := buildMetricShell(b, rm, srcSM, srcM)
	seen := 0
	eachMetricDataPoint(srcM, func(_ pcommon.Map, appendInto func(dst pmetric.Metric) pcommon.Map) {
		if seen < mid {
			appendInto(ma)
		} else {
			appendInto(mb)
		}
		seen++
	})
	return a, b, true
}

// buildMetricShell appends a ResourceMetrics/ScopeMetrics/Metric skeleton into
// dst — copying resource, scope, schema URLs, and the metric shell (no
// datapoints) — and returns the empty dest Metric for datapoints to append into.
func buildMetricShell(dst pmetric.Metrics, srcRM pmetric.ResourceMetrics, srcSM pmetric.ScopeMetrics, srcM pmetric.Metric) pmetric.Metric {
	drm := dst.ResourceMetrics().AppendEmpty()
	srcRM.Resource().CopyTo(drm.Resource())
	drm.SetSchemaUrl(srcRM.SchemaUrl())
	dsm := drm.ScopeMetrics().AppendEmpty()
	srcSM.Scope().CopyTo(dsm.Scope())
	dsm.SetSchemaUrl(srcSM.SchemaUrl())
	dm := dsm.Metrics().AppendEmpty()
	copyMetricShell(srcM, dm)
	return dm
}
