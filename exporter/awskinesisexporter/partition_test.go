package awskinesisexporter

import (
	"regexp"
	"strings"
	"testing"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// --- firstCapture ---

func TestFirstCapture(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		input   string
		want    string
	}{
		{
			name:    "no match returns empty",
			pattern: `^\d+$`,
			input:   "abc",
			want:    "",
		},
		{
			name:    "pattern with no capture group returns whole match",
			pattern: `\w+`,
			input:   "hello world",
			want:    "hello",
		},
		{
			name:    "one capture group returns the group",
			pattern: `^(GET|POST)`,
			input:   "GET /api",
			want:    "GET",
		},
		{
			name:    "multiple capture groups returns first group",
			pattern: `(\w+)-(\w+)`,
			input:   "foo-bar",
			want:    "foo",
		},
		{
			name:    "empty input no match returns empty",
			pattern: `\d+`,
			input:   "",
			want:    "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			re := regexp.MustCompile(tc.pattern)
			got := firstCapture(re, tc.input)
			if got != tc.want {
				t.Errorf("firstCapture(%q, %q) = %q; want %q", tc.pattern, tc.input, got, tc.want)
			}
		})
	}
}

// --- resolveParts ---

func newMap(pairs ...string) pcommon.Map {
	m := pcommon.NewMap()
	for i := 0; i+1 < len(pairs); i += 2 {
		m.PutStr(pairs[i], pairs[i+1])
	}
	return m
}

func TestResolveParts(t *testing.T) {
	tests := []struct {
		name       string
		plan       keyPlan
		res        pcommon.Map
		metricName string
		leaf       pcommon.Map
		want       []string
	}{
		{
			name: "resource attribute present",
			plan: keyPlan{{source: keySourceResource, name: "service.name"}},
			res:  newMap("service.name", "my-svc"),
			leaf: pcommon.NewMap(),
			want: []string{"my-svc"},
		},
		{
			name: "resource attribute missing gives empty",
			plan: keyPlan{{source: keySourceResource, name: "absent"}},
			res:  newMap("other", "val"),
			leaf: pcommon.NewMap(),
			want: []string{""},
		},
		{
			name: "datapoint attribute present",
			plan: keyPlan{{source: keySourceDatapoint, name: "http.method"}},
			res:  pcommon.NewMap(),
			leaf: newMap("http.method", "POST"),
			want: []string{"POST"},
		},
		{
			name: "datapoint attribute missing gives empty",
			plan: keyPlan{{source: keySourceDatapoint, name: "absent"}},
			res:  pcommon.NewMap(),
			leaf: pcommon.NewMap(),
			want: []string{""},
		},
		{
			name:       "metric_name returns name",
			plan:       keyPlan{{source: keySourceMetricName}},
			metricName: "http.server.duration",
			res:        pcommon.NewMap(),
			leaf:       pcommon.NewMap(),
			want:       []string{"http.server.duration"},
		},
		{
			name: "non-string attr coerced via AsString (int)",
			plan: keyPlan{{source: keySourceResource, name: "port"}},
			res: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutInt("port", 8080)
				return m
			}(),
			leaf: pcommon.NewMap(),
			want: []string{"8080"},
		},
		{
			name: "regex applied to resolved value",
			plan: keyPlan{{
				source: keySourceResource,
				name:   "host",
				re:     regexp.MustCompile(`^([^.]+)`),
			}},
			res:  newMap("host", "foo.example.com"),
			leaf: pcommon.NewMap(),
			want: []string{"foo"},
		},
		{
			name: "regex no match yields empty",
			plan: keyPlan{{
				source: keySourceResource,
				name:   "host",
				re:     regexp.MustCompile(`^\d+`),
			}},
			res:  newMap("host", "foo.example.com"),
			leaf: pcommon.NewMap(),
			want: []string{""},
		},
		{
			name: "mixed plan ordering resource datapoint metric_name",
			plan: keyPlan{
				{source: keySourceResource, name: "service.name"},
				{source: keySourceDatapoint, name: "http.method"},
				{source: keySourceMetricName},
			},
			res:        newMap("service.name", "svc-a"),
			metricName: "requests",
			leaf:       newMap("http.method", "GET"),
			want:       []string{"svc-a", "GET", "requests"},
		},
		{
			name: "separator prevents prefix collision a+bc vs ab+c",
			plan: keyPlan{
				{source: keySourceResource, name: "p1"},
				{source: keySourceResource, name: "p2"},
			},
			// "a" + "bc" joined = "a\x1fbc"; "ab" + "c" joined = "ab\x1fc" — different
			res:  newMap("p1", "a", "p2", "bc"),
			leaf: pcommon.NewMap(),
			want: []string{"a", "bc"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveParts(tc.plan, tc.res, tc.metricName, tc.leaf)
			if len(got) != len(tc.want) {
				t.Fatalf("resolveParts len=%d; want %d", len(got), len(tc.want))
			}
			for i, v := range got {
				if v != tc.want[i] {
					t.Errorf("resolveParts[%d] = %q; want %q", i, v, tc.want[i])
				}
			}
		})
	}
}

// Explicit collision test: a+bc vs ab+c must differ when joined with tagSep.
func TestResolvePartsSeparatorNoCollision(t *testing.T) {
	plan1 := keyPlan{
		{source: keySourceResource, name: "p1"},
		{source: keySourceResource, name: "p2"},
	}
	res1 := newMap("p1", "a", "p2", "bc")
	res2 := newMap("p1", "ab", "p2", "c")
	key1 := joinParts(resolveParts(plan1, res1, "", pcommon.NewMap()))
	key2 := joinParts(resolveParts(plan1, res2, "", pcommon.NewMap()))
	if key1 == key2 {
		t.Errorf("expected different keys for (a,bc) vs (ab,c), got %q", key1)
	}
}

// --- resolveKeyPlan ---

func tagHashCfgWith(mutate func(pk *PartitionKeyConfig)) *Config {
	c := baseValidCfg()
	c.PartitionKey = PartitionKeyConfig{
		Strategy: partitionStrategyTagHash,
		Hash:     hashXXHash,
	}
	mutate(&c.PartitionKey)
	return c
}

func TestResolveKeyPlan(t *testing.T) {
	t.Run("random strategy returns nil plan", func(t *testing.T) {
		c := baseValidCfg()
		c.PartitionKey = PartitionKeyConfig{Strategy: partitionStrategyRandom}
		plan, err := c.resolveKeyPlan()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if plan != nil {
			t.Errorf("expected nil plan for random strategy, got %v", plan)
		}
	})

	t.Run("empty strategy treated as random returns nil", func(t *testing.T) {
		c := baseValidCfg()
		c.PartitionKey = PartitionKeyConfig{Strategy: ""}
		plan, err := c.resolveKeyPlan()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if plan != nil {
			t.Errorf("expected nil plan, got %v", plan)
		}
	})

	t.Run("tags shorthand expands to all-resource plan", func(t *testing.T) {
		c := tagHashCfgWith(func(pk *PartitionKeyConfig) {
			pk.Tags = []string{"service.name", "env"}
		})
		plan, err := c.resolveKeyPlan()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(plan) != 2 {
			t.Fatalf("expected plan length 2, got %d", len(plan))
		}
		for i, ks := range plan {
			if ks.source != keySourceResource {
				t.Errorf("plan[%d].source = %q; want %q", i, ks.source, keySourceResource)
			}
			if ks.re != nil {
				t.Errorf("plan[%d].re should be nil for tags shorthand", i)
			}
			if ks.promote != "" {
				t.Errorf("plan[%d].promote should be empty for tags shorthand", i)
			}
		}
		if plan[0].name != "service.name" {
			t.Errorf("plan[0].name = %q; want %q", plan[0].name, "service.name")
		}
		if plan[1].name != "env" {
			t.Errorf("plan[1].name = %q; want %q", plan[1].name, "env")
		}
	})

	t.Run("keys with mixed sources, regex, and promote", func(t *testing.T) {
		c := tagHashCfgWith(func(pk *PartitionKeyConfig) {
			pk.Keys = []PartitionKeySource{
				{Source: keySourceResource, Name: "service.name"},
				{Source: keySourceDatapoint, Name: "http.method", Regex: `^(GET|POST)`, Promote: "derived.method"},
				{Source: keySourceMetricName},
			}
		})
		plan, err := c.resolveKeyPlan()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(plan) != 3 {
			t.Fatalf("expected plan length 3, got %d", len(plan))
		}
		if plan[0].source != keySourceResource || plan[0].name != "service.name" || plan[0].re != nil || plan[0].promote != "" {
			t.Errorf("plan[0] mismatch: %+v", plan[0])
		}
		if plan[1].source != keySourceDatapoint || plan[1].name != "http.method" || plan[1].re == nil || plan[1].promote != "derived.method" {
			t.Errorf("plan[1] mismatch: %+v", plan[1])
		}
		if plan[2].source != keySourceMetricName || plan[2].name != "" || plan[2].re != nil {
			t.Errorf("plan[2] mismatch: %+v", plan[2])
		}
	})

	t.Run("empty source defaults to resource", func(t *testing.T) {
		c := tagHashCfgWith(func(pk *PartitionKeyConfig) {
			pk.Keys = []PartitionKeySource{
				{Source: "", Name: "service.name"},
			}
		})
		plan, err := c.resolveKeyPlan()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if plan[0].source != keySourceResource {
			t.Errorf("expected source %q, got %q", keySourceResource, plan[0].source)
		}
	})

	t.Run("invalid regex returns error mentioning index", func(t *testing.T) {
		c := &Config{
			StreamName:    "s",
			Region:        "us-east-1",
			Encoding:      encoding.EncodingOTLPProto,
			Compression:   encoding.CodecNone,
			MaxRecordSize: 1 << 20,
			PartitionKey: PartitionKeyConfig{
				Strategy: partitionStrategyTagHash,
				Keys: []PartitionKeySource{
					{Source: keySourceResource, Name: "x", Regex: `[invalid`},
				},
				Hash: hashXXHash,
			},
			Oversize: OversizeConfig{
				Policies:               []string{oversizeSplitHalf},
				MaxAttempts:            8,
				MaxAttributeValueBytes: 4096,
			},
			PutRecords: PutRecordsConfig{MaxRecords: 500, MaxBytes: 5 << 20},
		}
		_, err := c.resolveKeyPlan()
		if err == nil {
			t.Fatal("expected error for invalid regex, got nil")
		}
		if !strings.Contains(err.Error(), "0") {
			t.Errorf("error should mention index 0, got: %v", err)
		}
	})
}

// --- resourceOnly ---

func TestResourceOnly(t *testing.T) {
	tests := []struct {
		name string
		plan keyPlan
		want bool
	}{
		{
			name: "all resource sources returns true",
			plan: keyPlan{
				{source: keySourceResource, name: "a"},
				{source: keySourceResource, name: "b"},
			},
			want: true,
		},
		{
			name: "single resource returns true",
			plan: keyPlan{{source: keySourceResource, name: "a"}},
			want: true,
		},
		{
			name: "contains datapoint returns false",
			plan: keyPlan{
				{source: keySourceResource, name: "a"},
				{source: keySourceDatapoint, name: "b"},
			},
			want: false,
		},
		{
			name: "contains metric_name returns false",
			plan: keyPlan{
				{source: keySourceResource, name: "a"},
				{source: keySourceMetricName},
			},
			want: false,
		},
		{
			name: "empty plan returns true",
			plan: keyPlan{},
			want: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.plan.resourceOnly()
			if got != tc.want {
				t.Errorf("resourceOnly() = %v; want %v", got, tc.want)
			}
		})
	}
}

// --- hasPromotion ---

func TestHasPromotion(t *testing.T) {
	tests := []struct {
		name string
		plan keyPlan
		want bool
	}{
		{
			name: "no promotions returns false",
			plan: keyPlan{
				{source: keySourceResource, name: "a"},
				{source: keySourceDatapoint, name: "b"},
			},
			want: false,
		},
		{
			name: "one promotion returns true",
			plan: keyPlan{
				{source: keySourceResource, name: "a"},
				{source: keySourceDatapoint, name: "b", promote: "derived.b"},
			},
			want: true,
		},
		{
			name: "all promote returns true",
			plan: keyPlan{
				{source: keySourceResource, name: "a", promote: "p.a"},
			},
			want: true,
		},
		{
			name: "empty plan returns false",
			plan: keyPlan{},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.plan.hasPromotion()
			if got != tc.want {
				t.Errorf("hasPromotion() = %v; want %v", got, tc.want)
			}
		})
	}
}

// --- applyPromotions ---

func TestApplyPromotions(t *testing.T) {
	t.Run("writes resource-source promotion to dstRes when absent", func(t *testing.T) {
		plan := keyPlan{{source: keySourceResource, name: "svc", promote: "derived.svc"}}
		parts := []string{"my-svc"}
		dstRes := pcommon.NewMap()
		dstLeaf := pcommon.NewMap()
		applyPromotions(plan, parts, dstRes, dstLeaf)
		v, ok := dstRes.Get("derived.svc")
		if !ok {
			t.Fatal("expected derived.svc in dstRes")
		}
		if v.AsString() != "my-svc" {
			t.Errorf("derived.svc = %q; want %q", v.AsString(), "my-svc")
		}
		if _, ok := dstLeaf.Get("derived.svc"); ok {
			t.Error("derived.svc should not be in dstLeaf for resource source")
		}
	})

	t.Run("writes datapoint-source promotion to dstLeaf when absent", func(t *testing.T) {
		plan := keyPlan{{source: keySourceDatapoint, name: "method", promote: "derived.method"}}
		parts := []string{"GET"}
		dstRes := pcommon.NewMap()
		dstLeaf := pcommon.NewMap()
		applyPromotions(plan, parts, dstRes, dstLeaf)
		v, ok := dstLeaf.Get("derived.method")
		if !ok {
			t.Fatal("expected derived.method in dstLeaf")
		}
		if v.AsString() != "GET" {
			t.Errorf("derived.method = %q; want %q", v.AsString(), "GET")
		}
		if _, ok := dstRes.Get("derived.method"); ok {
			t.Error("derived.method should not be in dstRes for datapoint source")
		}
	})

	t.Run("metric_name promotes to dstLeaf", func(t *testing.T) {
		plan := keyPlan{{source: keySourceMetricName, promote: "metric.name.promoted"}}
		parts := []string{"http.duration"}
		dstRes := pcommon.NewMap()
		dstLeaf := pcommon.NewMap()
		applyPromotions(plan, parts, dstRes, dstLeaf)
		v, ok := dstLeaf.Get("metric.name.promoted")
		if !ok {
			t.Fatal("expected metric.name.promoted in dstLeaf")
		}
		if v.AsString() != "http.duration" {
			t.Errorf("metric.name.promoted = %q; want %q", v.AsString(), "http.duration")
		}
	})

	t.Run("does not overwrite existing key (absent-only)", func(t *testing.T) {
		plan := keyPlan{{source: keySourceResource, name: "svc", promote: "derived.svc"}}
		parts := []string{"new-svc"}
		dstRes := pcommon.NewMap()
		dstRes.PutStr("derived.svc", "existing")
		dstLeaf := pcommon.NewMap()
		applyPromotions(plan, parts, dstRes, dstLeaf)
		v, _ := dstRes.Get("derived.svc")
		if v.AsString() != "existing" {
			t.Errorf("existing key was overwritten; got %q", v.AsString())
		}
	})

	t.Run("skips empty resolved value", func(t *testing.T) {
		plan := keyPlan{{source: keySourceResource, name: "svc", promote: "derived.svc"}}
		parts := []string{""}
		dstRes := pcommon.NewMap()
		dstLeaf := pcommon.NewMap()
		applyPromotions(plan, parts, dstRes, dstLeaf)
		if _, ok := dstRes.Get("derived.svc"); ok {
			t.Error("empty resolved value should not be promoted")
		}
	})

	t.Run("skips component with no promote", func(t *testing.T) {
		plan := keyPlan{{source: keySourceResource, name: "svc", promote: ""}}
		parts := []string{"my-svc"}
		dstRes := pcommon.NewMap()
		dstLeaf := pcommon.NewMap()
		applyPromotions(plan, parts, dstRes, dstLeaf)
		if dstRes.Len() != 0 {
			t.Error("no-promote component should write nothing")
		}
	})

	t.Run("mixed plan promotes correct targets", func(t *testing.T) {
		plan := keyPlan{
			{source: keySourceResource, name: "svc", promote: "p.svc"},
			{source: keySourceDatapoint, name: "method", promote: "p.method"},
			{source: keySourceMetricName, promote: "p.metric"},
		}
		parts := []string{"my-svc", "GET", "http.duration"}
		dstRes := pcommon.NewMap()
		dstLeaf := pcommon.NewMap()
		applyPromotions(plan, parts, dstRes, dstLeaf)

		if v, ok := dstRes.Get("p.svc"); !ok || v.AsString() != "my-svc" {
			t.Errorf("p.svc in dstRes: ok=%v val=%q", ok, v.AsString())
		}
		if v, ok := dstLeaf.Get("p.method"); !ok || v.AsString() != "GET" {
			t.Errorf("p.method in dstLeaf: ok=%v val=%q", ok, v.AsString())
		}
		if v, ok := dstLeaf.Get("p.metric"); !ok || v.AsString() != "http.duration" {
			t.Errorf("p.metric in dstLeaf: ok=%v val=%q", ok, v.AsString())
		}
	})
}
