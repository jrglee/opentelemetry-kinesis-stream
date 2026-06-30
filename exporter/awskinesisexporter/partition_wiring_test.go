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
// device.id, applies a regex-derived promoted attribute (subsystem), and
// produces stable per-device partition keys.
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
				{Source: keySourceDatapoint, Name: "device.id"},
				{Source: keySourceMetricName, Regex: "^([a-z]+)_", Promote: "subsystem"},
			},
		},
		Oversize: OversizeConfig{Policies: []string{oversizeSplitHalf}, MaxAttempts: 8, MaxAttributeValueBytes: 4096},
	}

	capt := &capture{}
	exp := newTestExporterCfg(t, cfg, capt.injectSerialize())

	// One ResourceMetrics, one metric named "charging_state", three datapoints:
	// two for device d1 and one for device d2.
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("charging_state")
	g := m.SetEmptyGauge()

	d1a := g.DataPoints().AppendEmpty()
	d1a.Attributes().PutStr("device.id", "d1")
	d1a.SetIntValue(10)

	d1b := g.DataPoints().AppendEmpty()
	d1b.Attributes().PutStr("device.id", "d1")
	d1b.SetIntValue(11)

	d2 := g.DataPoints().AppendEmpty()
	d2.Attributes().PutStr("device.id", "d2")
	d2.SetIntValue(20)

	if err := exp.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}

	recs := capt.all()
	// Expect exactly two records: one per distinct device.id.
	if len(recs) != 2 {
		t.Fatalf("records: got %d want 2 (one per device)", len(recs))
	}

	dec, err := encoding.NewMetricsDecoder(encoding.EncodingOTLPProto)
	if err != nil {
		t.Fatalf("decoder: %v", err)
	}

	totalDP := 0
	keyByDevice := map[string]string{} // device.id → partition key
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

						devID, ok := dp.Attributes().Get("device.id")
						if !ok {
							t.Fatalf("datapoint missing device.id")
						}
						dev := devID.AsString()

						// All datapoints in one record must share the same device.id.
						pkey := aws.ToString(r.PartitionKey)
						if prev, seen := keyByDevice[dev]; seen {
							if prev != pkey {
								t.Fatalf("unstable partition key for device %s: %s vs %s", dev, prev, pkey)
							}
						} else {
							keyByDevice[dev] = pkey
						}

						// Promoted attribute: subsystem="charging" (from "^([a-z]+)_" on "charging_state").
						subsys, ok := dp.Attributes().Get("subsystem")
						if !ok {
							t.Fatalf("promoted attribute 'subsystem' missing on datapoint for device %s", dev)
						}
						if subsys.AsString() != "charging" {
							t.Fatalf("subsystem: got %q want %q", subsys.AsString(), "charging")
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

	// The two devices must have produced distinct partition keys.
	if len(keyByDevice) != 2 {
		t.Fatalf("distinct device keys: got %d want 2", len(keyByDevice))
	}
	if keyByDevice["d1"] == keyByDevice["d2"] {
		t.Fatalf("d1 and d2 have the same partition key %s — expected distinct keys", keyByDevice["d1"])
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
