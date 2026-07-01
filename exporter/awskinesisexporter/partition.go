package awskinesisexporter

import (
	"fmt"
	"regexp"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// keySource is one compiled component of a tag_hash key plan.
type keySource struct {
	source  string
	name    string
	promote string
	re      *regexp.Regexp // nil when no regex was configured
}

// keyPlan is the compiled, ready-for-hot-path partition-key plan.
// It is built once per exporter start via Config.resolveKeyPlan and reused
// across every record.
type keyPlan []keySource

// resourceOnly reports whether every component reads from resource attributes.
// When true the caller can skip resolving datapoint/metric-name fields and use
// a cheaper resource-level fast path.
func (p keyPlan) resourceOnly() bool {
	for _, ks := range p {
		if ks.source != keySourceResource {
			return false
		}
	}
	return true
}

// hasPromotion reports whether any component will write a derived attribute
// back onto the outgoing record. When false the caller can skip applyPromotions.
func (p keyPlan) hasPromotion() bool {
	for _, ks := range p {
		if ks.promote != "" {
			return true
		}
	}
	return false
}

// resolveKeyPlan compiles the partition-key configuration into a ready-to-use
// keyPlan. It returns nil, nil for any strategy other than tag_hash. An error
// is returned (with the offending index) only when a regex fails to compile —
// defensive even though Validate already rejects bad patterns.
func (c *Config) resolveKeyPlan() (keyPlan, error) {
	if c.PartitionKey.Strategy != partitionStrategyTagHash {
		return nil, nil
	}

	// Shorthand: Tags expands to all-resource sources with no regex/promote.
	if len(c.PartitionKey.Tags) > 0 {
		plan := make(keyPlan, len(c.PartitionKey.Tags))
		for i, tag := range c.PartitionKey.Tags {
			plan[i] = keySource{source: keySourceResource, name: tag}
		}
		return plan, nil
	}

	// Full form: Keys list with heterogeneous sources, optional regex and promote.
	plan := make(keyPlan, len(c.PartitionKey.Keys))
	for i, ks := range c.PartitionKey.Keys {
		src := ks.Source
		if src == "" {
			src = keySourceResource
		}
		entry := keySource{
			source:  src,
			name:    ks.Name,
			promote: ks.Promote,
		}
		if ks.Regex != "" {
			re, err := regexp.Compile(ks.Regex)
			if err != nil {
				return nil, fmt.Errorf("partition_key.keys[%d]: invalid regex: %w", i, err)
			}
			entry.re = re
		}
		plan[i] = entry
	}
	return plan, nil
}

// resolveParts resolves each plan component to its final string value (after
// optional regex reduction). The returned slice has one element per plan entry;
// callers join it with tagSep to form the partition key and pass the same
// slice to applyPromotions.
func resolveParts(plan keyPlan, res pcommon.Map, metricName string, leaf pcommon.Map) []string {
	parts := make([]string, len(plan))
	for i, ks := range plan {
		var v string
		switch ks.source {
		case keySourceResource:
			if attr, ok := res.Get(ks.name); ok {
				v = attr.AsString()
			}
		case keySourceDatapoint:
			if attr, ok := leaf.Get(ks.name); ok {
				v = attr.AsString()
			}
		case keySourceMetricName:
			v = metricName
		}
		if ks.re != nil {
			v = firstCapture(ks.re, v)
		}
		parts[i] = v
	}
	return parts
}

// firstCapture returns the first capture group of the first match of re against
// s. When the pattern has no capture groups the whole match is returned. A
// non-match returns "".
func firstCapture(re *regexp.Regexp, s string) string {
	m := re.FindStringSubmatch(s)
	if m == nil {
		return ""
	}
	if len(m) > 1 {
		return m[1]
	}
	return m[0]
}

// applyPromotions writes each resolved value back onto the outgoing record
// under its configured promote key, but only when:
//   - the component has a non-empty promote name,
//   - the resolved value is non-empty, and
//   - the target attribute is not already present.
//
// Resource-source components target dstRes; datapoint- and metric_name-source
// components target dstLeaf.
func applyPromotions(plan keyPlan, parts []string, dstRes, dstLeaf pcommon.Map) {
	for i, ks := range plan {
		if ks.promote == "" || parts[i] == "" {
			continue
		}
		switch ks.source {
		case keySourceResource:
			putIfAbsent(dstRes, ks.promote, parts[i])
		case keySourceDatapoint, keySourceMetricName:
			putIfAbsent(dstLeaf, ks.promote, parts[i])
		}
	}
}

// putIfAbsent writes value v under key k in m only when k is not already
// present.
func putIfAbsent(m pcommon.Map, k, v string) {
	if _, ok := m.Get(k); !ok {
		m.PutStr(k, v)
	}
}

// emptyAttrs is a shared empty map used as the leaf argument on paths that have
// no record leaf (the resource-only fast path and the zero-datapoint metric
// branch). It is READ-ONLY: every use only reads it via resolveParts' Get, and
// applyPromotions is never reached with emptyAttrs as its write target (those
// paths are resource-only, so promotion writes to the resource map, never the
// leaf). It must never be written to — doing so would corrupt shared state
// across concurrent Consume calls.
var emptyAttrs = pcommon.NewMap()

// joinParts joins resolved parts with the standard tag separator. Extracted so
// every grouping path shares one join.
func joinParts(parts []string) string {
	return strings.Join(parts, tagSep)
}
