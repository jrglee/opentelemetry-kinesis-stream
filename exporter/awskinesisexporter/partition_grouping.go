package awskinesisexporter

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// metricBucket coalesces datapoints under preserved (i,j,k) origin identity,
// keyed by the datapoint-level partition key. Distinct origin metrics (e.g.
// foo_a and foo_b) stay separate dest metrics even when they share a key;
// datapoints from the same origin metric recombine into one dest metric.
type metricBucket struct {
	out    pmetric.Metrics
	rm     map[int]pmetric.ResourceMetrics
	sm     map[[2]int]pmetric.ScopeMetrics
	metric map[[3]int]pmetric.Metric
}

func newMetricBucket() *metricBucket {
	return &metricBucket{
		out:    pmetric.NewMetrics(),
		rm:     make(map[int]pmetric.ResourceMetrics),
		sm:     make(map[[2]int]pmetric.ScopeMetrics),
		metric: make(map[[3]int]pmetric.Metric),
	}
}

// ensureMetric find-or-creates the dest ResourceMetrics(i)/ScopeMetrics(i,j)/
// Metric(i,j,k) inside the bucket, copying resource, scope, schema URLs, and
// the metric shell (no datapoints). Returns the dest Metric.
func (b *metricBucket) ensureMetric(
	i, j, k int,
	srcRM pmetric.ResourceMetrics,
	srcSM pmetric.ScopeMetrics,
	srcM pmetric.Metric,
) pmetric.Metric {
	if _, ok := b.rm[i]; !ok {
		dstRM := b.out.ResourceMetrics().AppendEmpty()
		srcRM.Resource().CopyTo(dstRM.Resource())
		dstRM.SetSchemaUrl(srcRM.SchemaUrl())
		b.rm[i] = dstRM
	}

	smKey := [2]int{i, j}
	if _, ok := b.sm[smKey]; !ok {
		dstSM := b.rm[i].ScopeMetrics().AppendEmpty()
		srcSM.Scope().CopyTo(dstSM.Scope())
		dstSM.SetSchemaUrl(srcSM.SchemaUrl())
		b.sm[smKey] = dstSM
	}

	mKey := [3]int{i, j, k}
	if _, ok := b.metric[mKey]; !ok {
		dstM := b.sm[smKey].Metrics().AppendEmpty()
		copyMetricShell(srcM, dstM)
		b.metric[mKey] = dstM
	}

	return b.metric[mKey]
}

// copyMetricShell copies Name/Description/Unit/Metadata and the typed-empty
// body with settings: Sum → AggregationTemporality + IsMonotonic; Histogram /
// ExponentialHistogram → AggregationTemporality; Gauge / Summary → none.
func copyMetricShell(src, dst pmetric.Metric) {
	dst.SetName(src.Name())
	dst.SetDescription(src.Description())
	dst.SetUnit(src.Unit())
	src.Metadata().CopyTo(dst.Metadata())

	switch src.Type() {
	case pmetric.MetricTypeGauge:
		dst.SetEmptyGauge()
	case pmetric.MetricTypeSum:
		dstSum := dst.SetEmptySum()
		dstSum.SetAggregationTemporality(src.Sum().AggregationTemporality())
		dstSum.SetIsMonotonic(src.Sum().IsMonotonic())
	case pmetric.MetricTypeHistogram:
		dstHist := dst.SetEmptyHistogram()
		dstHist.SetAggregationTemporality(src.Histogram().AggregationTemporality())
	case pmetric.MetricTypeExponentialHistogram:
		dstExpHist := dst.SetEmptyExponentialHistogram()
		dstExpHist.SetAggregationTemporality(src.ExponentialHistogram().AggregationTemporality())
	case pmetric.MetricTypeSummary:
		dst.SetEmptySummary()
	}
}

// eachMetricDataPoint iterates every datapoint in m, calling fn with:
//   - dpAttrs: the datapoint's own Attributes map (read-only; from src)
//   - appendInto: a closure that appends THAT datapoint into dst's matching
//     typed DataPoints slice and returns the appended datapoint's Attributes
//     (so the caller can promote onto it).
func eachMetricDataPoint(
	m pmetric.Metric,
	fn func(dpAttrs pcommon.Map, appendInto func(dst pmetric.Metric) pcommon.Map),
) {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			fn(dp.Attributes(), func(dst pmetric.Metric) pcommon.Map {
				newDP := dst.Gauge().DataPoints().AppendEmpty()
				dp.CopyTo(newDP)
				return newDP.Attributes()
			})
		}
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			fn(dp.Attributes(), func(dst pmetric.Metric) pcommon.Map {
				newDP := dst.Sum().DataPoints().AppendEmpty()
				dp.CopyTo(newDP)
				return newDP.Attributes()
			})
		}
	case pmetric.MetricTypeHistogram:
		dps := m.Histogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			fn(dp.Attributes(), func(dst pmetric.Metric) pcommon.Map {
				newDP := dst.Histogram().DataPoints().AppendEmpty()
				dp.CopyTo(newDP)
				return newDP.Attributes()
			})
		}
	case pmetric.MetricTypeExponentialHistogram:
		dps := m.ExponentialHistogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			fn(dp.Attributes(), func(dst pmetric.Metric) pcommon.Map {
				newDP := dst.ExponentialHistogram().DataPoints().AppendEmpty()
				dp.CopyTo(newDP)
				return newDP.Attributes()
			})
		}
	case pmetric.MetricTypeSummary:
		dps := m.Summary().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			fn(dp.Attributes(), func(dst pmetric.Metric) pcommon.Map {
				newDP := dst.Summary().DataPoints().AppendEmpty()
				dp.CopyTo(newDP)
				return newDP.Attributes()
			})
		}
	}
}

// groupMetricsByLeaf descends to each datapoint, computes a per-datapoint key
// via resolveParts(plan, resourceAttrs, metricName, dpAttrs) joined with
// joinParts, reassembles datapoints into per-key batches preserving
// resource/scope/metric identity, and applies promotions. Emits taggedBatches
// in first-seen key order. The input md is never mutated.
func groupMetricsByLeaf(md pmetric.Metrics, plan keyPlan) []taggedBatch[pmetric.Metrics] {
	byKey := map[string]*metricBucket{}
	var order []string
	promoting := plan.hasPromotion()

	// bucketFor find-or-creates the bucket for a key, tracking first-seen order.
	bucketFor := func(key string) *metricBucket {
		b, ok := byKey[key]
		if !ok {
			b = newMetricBucket()
			byKey[key] = b
			order = append(order, key)
		}
		return b
	}

	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		srcRM := rms.At(i)
		resAttrs := srcRM.Resource().Attributes()

		sms := srcRM.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			srcSM := sms.At(j)
			ms := srcSM.Metrics()
			for k := 0; k < ms.Len(); k++ {
				srcM := ms.At(k)

				seen := false
				eachMetricDataPoint(srcM, func(dpAttrs pcommon.Map, appendInto func(dst pmetric.Metric) pcommon.Map) {
					seen = true
					parts := resolveParts(plan, resAttrs, srcM.Name(), dpAttrs)
					bucket := bucketFor(joinParts(parts))
					destDPAttrs := appendInto(bucket.ensureMetric(i, j, k, srcRM, srcSM, srcM))

					if promoting {
						applyPromotions(plan, parts, bucket.rm[i].Resource().Attributes(), destDPAttrs)
					}
				})

				// A metric with no datapoints carries no leaf to key on, but its
				// shell (name/type/temporality/monotonicity) still matters — the
				// resource fast path preserves it via CopyTo, so the leaf path must
				// too. Route it by the leaf-less key (datapoint segments empty). No
				// promotion runs: there is no datapoint to enrich, and metric_name
				// promotion targets the leaf, which does not exist here.
				if !seen {
					parts := resolveParts(plan, resAttrs, srcM.Name(), emptyAttrs)
					bucketFor(joinParts(parts)).ensureMetric(i, j, k, srcRM, srcSM, srcM)
				}
			}
		}
	}

	out := make([]taggedBatch[pmetric.Metrics], 0, len(order))
	for _, k := range order {
		out = append(out, taggedBatch[pmetric.Metrics]{key: k, batch: byKey[k].out})
	}
	return out
}

// traceBucket coalesces spans under preserved (i,j) origin identity, keyed by
// the span-level partition key. Spans from the same origin scope recombine into
// one dest ScopeSpans even when they share a key.
type traceBucket struct {
	out ptrace.Traces
	rs  map[int]ptrace.ResourceSpans
	ss  map[[2]int]ptrace.ScopeSpans
}

func newTraceBucket() *traceBucket {
	return &traceBucket{
		out: ptrace.NewTraces(),
		rs:  make(map[int]ptrace.ResourceSpans),
		ss:  make(map[[2]int]ptrace.ScopeSpans),
	}
}

// ensureScope find-or-creates the dest ResourceSpans(i)/ScopeSpans(i,j), copying
// resource, scope, and schema URLs. Returns the dest ScopeSpans.
func (b *traceBucket) ensureScope(i, j int, srcRS ptrace.ResourceSpans, srcSS ptrace.ScopeSpans) ptrace.ScopeSpans {
	if _, ok := b.rs[i]; !ok {
		dstRS := b.out.ResourceSpans().AppendEmpty()
		srcRS.Resource().CopyTo(dstRS.Resource())
		dstRS.SetSchemaUrl(srcRS.SchemaUrl())
		b.rs[i] = dstRS
	}

	ssKey := [2]int{i, j}
	if _, ok := b.ss[ssKey]; !ok {
		dstSS := b.rs[i].ScopeSpans().AppendEmpty()
		srcSS.Scope().CopyTo(dstSS.Scope())
		dstSS.SetSchemaUrl(srcSS.SchemaUrl())
		b.ss[ssKey] = dstSS
	}

	return b.ss[ssKey]
}

// groupTracesByLeaf descends to each span, computes a per-span key via
// resolveParts(plan, resourceAttrs, "", span.Attributes()) joined with
// joinParts, reassembles spans into per-key batches preserving resource/scope
// identity, and applies promotions. Emits taggedBatches in first-seen key
// order. The input td is never mutated.
func groupTracesByLeaf(td ptrace.Traces, plan keyPlan) []taggedBatch[ptrace.Traces] {
	byKey := map[string]*traceBucket{}
	var order []string
	promoting := plan.hasPromotion()

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		srcRS := rss.At(i)
		resAttrs := srcRS.Resource().Attributes()

		sss := srcRS.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			srcSS := sss.At(j)
			spans := srcSS.Spans()
			for k := 0; k < spans.Len(); k++ {
				srcSpan := spans.At(k)

				parts := resolveParts(plan, resAttrs, "", srcSpan.Attributes())
				key := joinParts(parts)

				if _, ok := byKey[key]; !ok {
					byKey[key] = newTraceBucket()
					order = append(order, key)
				}
				bucket := byKey[key]
				dstSS := bucket.ensureScope(i, j, srcRS, srcSS)
				dstSpan := dstSS.Spans().AppendEmpty()
				srcSpan.CopyTo(dstSpan)

				if promoting {
					destResAttrs := bucket.rs[i].Resource().Attributes()
					applyPromotions(plan, parts, destResAttrs, dstSpan.Attributes())
				}
			}
		}
	}

	out := make([]taggedBatch[ptrace.Traces], 0, len(order))
	for _, k := range order {
		out = append(out, taggedBatch[ptrace.Traces]{key: k, batch: byKey[k].out})
	}
	return out
}

// logBucket coalesces log records under preserved (i,j) origin identity, keyed
// by the log-record-level partition key.
type logBucket struct {
	out plog.Logs
	rl  map[int]plog.ResourceLogs
	sl  map[[2]int]plog.ScopeLogs
}

func newLogBucket() *logBucket {
	return &logBucket{
		out: plog.NewLogs(),
		rl:  make(map[int]plog.ResourceLogs),
		sl:  make(map[[2]int]plog.ScopeLogs),
	}
}

// ensureScope find-or-creates the dest ResourceLogs(i)/ScopeLogs(i,j), copying
// resource, scope, and schema URLs. Returns the dest ScopeLogs.
func (b *logBucket) ensureScope(i, j int, srcRL plog.ResourceLogs, srcSL plog.ScopeLogs) plog.ScopeLogs {
	if _, ok := b.rl[i]; !ok {
		dstRL := b.out.ResourceLogs().AppendEmpty()
		srcRL.Resource().CopyTo(dstRL.Resource())
		dstRL.SetSchemaUrl(srcRL.SchemaUrl())
		b.rl[i] = dstRL
	}

	slKey := [2]int{i, j}
	if _, ok := b.sl[slKey]; !ok {
		dstSL := b.rl[i].ScopeLogs().AppendEmpty()
		srcSL.Scope().CopyTo(dstSL.Scope())
		dstSL.SetSchemaUrl(srcSL.SchemaUrl())
		b.sl[slKey] = dstSL
	}

	return b.sl[slKey]
}

// groupLogsByLeaf descends to each log record, computes a per-record key via
// resolveParts(plan, resourceAttrs, "", lr.Attributes()) joined with joinParts,
// reassembles records into per-key batches preserving resource/scope identity,
// and applies promotions. Emits taggedBatches in first-seen key order. The
// input ld is never mutated.
func groupLogsByLeaf(ld plog.Logs, plan keyPlan) []taggedBatch[plog.Logs] {
	byKey := map[string]*logBucket{}
	var order []string
	promoting := plan.hasPromotion()

	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		srcRL := rls.At(i)
		resAttrs := srcRL.Resource().Attributes()

		sls := srcRL.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			srcSL := sls.At(j)
			lrs := srcSL.LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				srcLR := lrs.At(k)

				parts := resolveParts(plan, resAttrs, "", srcLR.Attributes())
				key := joinParts(parts)

				if _, ok := byKey[key]; !ok {
					byKey[key] = newLogBucket()
					order = append(order, key)
				}
				bucket := byKey[key]
				dstSL := bucket.ensureScope(i, j, srcRL, srcSL)
				dstLR := dstSL.LogRecords().AppendEmpty()
				srcLR.CopyTo(dstLR)

				if promoting {
					destResAttrs := bucket.rl[i].Resource().Attributes()
					applyPromotions(plan, parts, destResAttrs, dstLR.Attributes())
				}
			}
		}
	}

	out := make([]taggedBatch[plog.Logs], 0, len(order))
	for _, k := range order {
		out = append(out, taggedBatch[plog.Logs]{key: k, batch: byKey[k].out})
	}
	return out
}
