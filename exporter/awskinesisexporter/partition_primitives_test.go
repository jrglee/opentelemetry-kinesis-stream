package awskinesisexporter

import (
	"regexp"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// --- helpers ---

func newMap(pairs ...string) pcommon.Map {
	m := pcommon.NewMap()
	for i := 0; i+1 < len(pairs); i += 2 {
		m.PutStr(pairs[i], pairs[i+1])
	}
	return m
}

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
