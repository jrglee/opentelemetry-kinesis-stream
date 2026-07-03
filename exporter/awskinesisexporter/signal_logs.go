package awskinesisexporter

// Logs signal adapter for the generic record pipeline.
//
// pdata's plog type family is structurally independent of ptrace and pmetric.
// The log record is the indivisible leaf; it has no sub-items analogous to
// datapoints, so splitLogsHalf bottoms out at a single log record rather than
// descending further. The signalCodec[plog.Logs] returned by logsCodec bridges
// this file into the signal-agnostic pipeline in record.go.

import (
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

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
