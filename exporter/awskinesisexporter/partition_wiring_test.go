package awskinesisexporter

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// TestKeysBasedPlanWiredThroughConsumeMetrics exercises the full
// resolveKeyPlan → newExporter → emit → groupByTags → leaf path, proving
// that a Keys-based (non-Tags) tag_hash config fans out datapoints by
// instance, applies a regex-derived promoted attribute (namespace), and
// produces stable per-instance partition keys.
func TestKeysBasedPlanWiredThroughConsumeMetrics(t *testing.T) {
	cfg := &Config{
		StreamName:    "test-stream",
		Region:        "us-east-1",
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecNone,
		MaxRecordSize: 1 << 20,
		PartitionKey: PartitionKeyConfig{
			Strategy: partitionStrategyTagHash,
			Hash:     hashXXHash,
			Keys: []PartitionKeySource{
				{Source: keySourceDatapoint, Name: "instance"},
				{Source: keySourceMetricName, Regex: "^([a-z]+)_", Promote: "namespace"},
			},
		},
		Oversize: OversizeConfig{Policies: []string{oversizeSplitHalf}, MaxAttempts: 8, MaxAttributeValueBytes: 4096},
	}

	capt := &capture{}
	exp := newTestExporterCfg(t, cfg, capt.injectSerialize())

	// One ResourceMetrics, one metric named "http_requests", three datapoints:
	// two for instance i1 and one for instance i2.
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("http_requests")
	g := m.SetEmptyGauge()

	i1a := g.DataPoints().AppendEmpty()
	i1a.Attributes().PutStr("instance", "i1")
	i1a.SetIntValue(10)

	i1b := g.DataPoints().AppendEmpty()
	i1b.Attributes().PutStr("instance", "i1")
	i1b.SetIntValue(11)

	i2 := g.DataPoints().AppendEmpty()
	i2.Attributes().PutStr("instance", "i2")
	i2.SetIntValue(20)

	if err := exp.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}

	recs := capt.all()
	// Expect exactly two records: one per distinct instance.
	if len(recs) != 2 {
		t.Fatalf("records: got %d want 2 (one per instance)", len(recs))
	}

	dec, err := encoding.NewMetricsDecoder(encoding.EncodingOTLPProto)
	if err != nil {
		t.Fatalf("decoder: %v", err)
	}

	totalDP := 0
	keyByInstance := map[string]string{} // instance → partition key
	for _, r := range recs {
		decoded, err := dec.Unmarshal(r.Data)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}

		// Walk every datapoint in the decoded batch.
		rms := decoded.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					metric := ms.At(k)
					dps := metric.Gauge().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						totalDP++

						instID, ok := dp.Attributes().Get("instance")
						if !ok {
							t.Fatalf("datapoint missing instance")
						}
						inst := instID.AsString()

						// All datapoints in one record must share the same instance.
						pkey := aws.ToString(r.PartitionKey)
						if prev, seen := keyByInstance[inst]; seen {
							if prev != pkey {
								t.Fatalf("unstable partition key for instance %s: %s vs %s", inst, prev, pkey)
							}
						} else {
							keyByInstance[inst] = pkey
						}

						// Promoted attribute: namespace="http" (from "^([a-z]+)_" on "http_requests").
						ns, ok := dp.Attributes().Get("namespace")
						if !ok {
							t.Fatalf("promoted attribute 'namespace' missing on datapoint for instance %s", inst)
						}
						if ns.AsString() != "http" {
							t.Fatalf("namespace: got %q want %q", ns.AsString(), "http")
						}
					}
				}
			}
		}
	}

	// All three datapoints must be present across the two records.
	if totalDP != 3 {
		t.Fatalf("total datapoints: got %d want 3", totalDP)
	}

	// The two instances must have produced distinct partition keys.
	if len(keyByInstance) != 2 {
		t.Fatalf("distinct instance keys: got %d want 2", len(keyByInstance))
	}
	if keyByInstance["i1"] == keyByInstance["i2"] {
		t.Fatalf("i1 and i2 have the same partition key %s — expected distinct keys", keyByInstance["i1"])
	}
}

// TestInvalidRegexFailsNewExporter confirms that a bad regex in Keys causes
// resolveKeyPlan to return an error, which surfaces early at exporter construction.
func TestInvalidRegexFailsNewExporter(t *testing.T) {
	cfg := &Config{
		StreamName:    "test-stream",
		Region:        "us-east-1",
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecNone,
		MaxRecordSize: 1 << 20,
		PartitionKey: PartitionKeyConfig{
			Strategy: partitionStrategyTagHash,
			Hash:     hashXXHash,
			Keys: []PartitionKeySource{
				{Source: keySourceMetricName, Regex: "[invalid("},
			},
		},
		Oversize: OversizeConfig{Policies: []string{oversizeSplitHalf}, MaxAttempts: 8, MaxAttributeValueBytes: 4096},
	}

	_, err := cfg.resolveKeyPlan()
	if err == nil {
		t.Fatal("expected error for invalid regex, got nil")
	}
}
