//go:build e2e

// This file drives the metrics E2E: a load generator emits InfluxDB line
// protocol to a producer collector (influxdb -> groupbyattrs -> Kinesis),
// MiniStack emulates Kinesis/DynamoDB, and a consumer collector
// (Kinesis -> file) writes OTLP-JSON metrics back out. The test asserts no
// datapoint loss and tag locality: every datapoint of a given (host, region)
// tuple is grouped under a resource carrying that exact tuple, proving the
// groupbyattrs promotion + exporter tag_hash grouping worked end to end.
package e2e

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// attrMap aliases the pdata attribute map type returned by datapoint accessors.
type attrMap = pcommon.Map

const (
	metricsLeaseTable = "otel-leases-metrics"
	metricsOutFile    = "metrics-out.jsonl"
	metricsDeliver    = 120 * time.Second
	metricsSettle     = 12 * time.Second
)

// composeInflux runs `docker compose` against the metrics stack file. It mirrors
// the traces harness `compose` but selects docker-compose.influx.yaml so the
// two stacks stay independent.
func composeInflux(t *testing.T, env []string, timeout time.Duration, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	full := append([]string{"compose", "-f", "docker-compose.influx.yaml"}, args...)
	cmd := exec.CommandContext(ctx, "docker", full...)
	cmd.Dir = composeDir(t)
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// copySharedFrom pulls the shared volume's files into dest, copying from a named
// service (the metrics stack uses consumer-metrics, not consumer-a).
func copySharedFrom(t *testing.T, env []string, service, dest string) {
	t.Helper()
	if out, err := composeInflux(t, env, 30*time.Second, "cp", service+":/shared/.", dest); err != nil {
		t.Logf("cp shared (not ready yet?): %v\n%s", err, out)
	}
}

func TestInfluxMetricsRoundTrip(t *testing.T) {
	requireDocker(t)

	shared := t.TempDir()
	env := composeEnv()

	t.Cleanup(func() {
		out, err := composeInflux(t, env, 60*time.Second, "down", "-v")
		if err != nil {
			t.Logf("compose down failed: %v\n%s", err, out)
		}
	})

	// Build the collector image and linegen once, then start without --build so
	// parallel writers do not race the shared otelcol-kinesis:dev tag (the same
	// gotcha as the traces stack).
	if out, err := composeInflux(t, env, 5*time.Minute, "build", "producer-metrics", "linegen"); err != nil {
		t.Fatalf("compose build: %v\n%s", err, out)
	}
	if out, err := composeInflux(t, env, 3*time.Minute, "up", "-d"); err != nil {
		t.Fatalf("compose up: %v\n%s", err, out)
	}

	// Wait until the consumer owns at least one shard before generating load, so
	// the GetRecords loop is live when records land. With a single replica this
	// is ownership of all shards; we only require >=1 owned to avoid coupling to
	// shard count.
	waitForMetricsOwnership(t)

	// Run the load generator and parse its stdout contract for the expected
	// total and the distinct (host, region) tuple count.
	out, err := composeInflux(t, env, 90*time.Second, "run", "--rm", "linegen")
	if err != nil {
		t.Fatalf("linegen: %v\n%s", err, out)
	}
	total, distinct := parseLinegen(t, out)
	t.Logf("linegen sent %d measurements across %d distinct (host,region) tuples", total, distinct)

	// Poll the consumer output until the datapoint count reaches the emitted
	// total (no loss), tolerating partial mid-write lines.
	got := waitForDatapoints(t, env, shared, total)
	if got != total {
		t.Fatalf("datapoint count = %d, want %d (loss)", got, total)
	}

	// Settle: keep reading past first reaching the target so a late duplicate or
	// straggler surfaces before asserting.
	time.Sleep(metricsSettle)
	count, perTuple, violations := readDatapoints(t, env, shared)
	if count != total {
		t.Fatalf("after settle, datapoint count = %d, want %d", count, total)
	}

	// Tag locality: every resource block must carry a consistent (host, region)
	// pair on its resource attributes (promoted by groupbyattrs). A datapoint
	// whose own attributes disagree with its resource — or a resource missing
	// the promoted keys — means the grouping did not hold.
	if len(violations) > 0 {
		t.Fatalf("tag locality violated for %d datapoint(s); sample: %s", len(violations), violations[0])
	}
	if len(perTuple) != distinct {
		t.Fatalf("observed %d distinct (host,region) tuples, want %d", len(perTuple), distinct)
	}
	t.Logf("metrics round-trip OK: %d datapoints, %d tuples, tag locality held", count, len(perTuple))
}

// waitForMetricsOwnership polls the metrics lease table until at least one shard
// has an owner, proving the consumer's lease loop is live.
func waitForMetricsOwnership(t *testing.T) {
	t.Helper()
	client := dynamoClient(t)
	deadline := time.Now().Add(settleDeadline)
	for time.Now().Before(deadline) {
		_, owned, total := scanOwnersTable(t, client, metricsLeaseTable)
		if total >= 1 && owned >= 1 {
			t.Logf("metrics shard ownership live: %d/%d leases owned", owned, total)
			return
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatal("timed out waiting for metrics shard ownership")
}

// scanOwnersTable is scanOwners parameterized by table name, so the metrics
// stack's lease table (otel-leases-metrics) can be inspected without disturbing
// the traces harness.
func scanOwnersTable(t *testing.T, client *dynamodb.Client, table string) (owners map[string]int, owned, total int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(table)})
	if err != nil {
		return nil, 0, 0 // table may not exist yet
	}
	owners = make(map[string]int)
	for _, item := range out.Items {
		if v, ok := item["leaseOwner"].(*ddbtypes.AttributeValueMemberS); ok && v.Value != "" {
			owners[v.Value]++
			owned++
		}
	}
	return owners, owned, len(out.Items)
}

// parseLinegen reads linegen's stdout contract: lines "LINEGEN_SENT <n>" and
// "LINEGEN_DISTINCT_TUPLES <n>".
func parseLinegen(t *testing.T, out string) (total, distinct int) {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) != 2 {
			continue
		}
		switch fields[0] {
		case "LINEGEN_SENT":
			total = mustAtoi(t, fields[1])
		case "LINEGEN_DISTINCT_TUPLES":
			distinct = mustAtoi(t, fields[1])
		}
	}
	if total == 0 || distinct == 0 {
		t.Fatalf("could not parse linegen output (total=%d distinct=%d):\n%s", total, distinct, out)
	}
	return total, distinct
}

func mustAtoi(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("atoi %q: %v", s, err)
	}
	return n
}

// waitForDatapoints polls the consumer output until the datapoint count reaches
// want (or the deadline passes), returning the last count seen.
func waitForDatapoints(t *testing.T, env []string, shared string, want int) int {
	t.Helper()
	deadline := time.Now().Add(metricsDeliver)
	var count int
	for time.Now().Before(deadline) {
		count, _, _ = readDatapoints(t, env, shared)
		if count >= want {
			return count
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out: got %d/%d datapoints", count, want)
	return count
}

// readDatapoints copies the consumer output off the shared volume and parses
// each OTLP-JSON line, returning the total datapoint count, a per-(host,region)
// tally, and any tag-locality violation descriptions.
func readDatapoints(t *testing.T, env []string, shared string) (count int, perTuple map[string]int, violations []string) {
	t.Helper()
	copySharedFrom(t, env, "consumer-metrics", shared)
	perTuple = map[string]int{}

	path := filepath.Join(shared, metricsOutFile)
	f, err := os.Open(path)
	if err != nil {
		return 0, perTuple, nil // not created yet
	}
	defer f.Close()

	unmarshaler := &pmetric.JSONUnmarshaler{}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		md, err := unmarshaler.UnmarshalMetrics([]byte(line))
		if err != nil {
			// `docker compose cp` can snapshot the file mid-write, truncating the
			// final line; the caller re-polls until the count stabilizes, so skip.
			continue
		}
		count += tallyDatapoints(md, perTuple, &violations)
	}
	return count, perTuple, violations
}

// tallyDatapoints walks every datapoint, keying by the resource's promoted
// (host, region) attributes. It records a violation when a resource lacks the
// promoted keys or when a datapoint's own host/region attributes disagree with
// its resource — either means the groupbyattrs+exporter grouping did not hold.
func tallyDatapoints(md pmetric.Metrics, perTuple map[string]int, violations *[]string) int {
	count := 0
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		rattrs := rm.Resource().Attributes()
		host, hasHost := rattrs.Get("host")
		region, hasRegion := rattrs.Get("region")

		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			metrics := sms.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				m := metrics.At(k)
				for _, dpAttrs := range datapointAttrs(m) {
					count++
					if !hasHost || !hasRegion {
						*violations = append(*violations,
							fmt.Sprintf("resource missing promoted host/region (host=%v region=%v)", hasHost, hasRegion))
						continue
					}
					tuple := host.AsString() + "|" + region.AsString()
					perTuple[tuple]++
					// If host/region also remain on the datapoint, they must match
					// the resource they were grouped under.
					if dh, ok := dpAttrs.Get("host"); ok && dh.AsString() != host.AsString() {
						*violations = append(*violations,
							fmt.Sprintf("datapoint host %q under resource host %q", dh.AsString(), host.AsString()))
					}
					if dr, ok := dpAttrs.Get("region"); ok && dr.AsString() != region.AsString() {
						*violations = append(*violations,
							fmt.Sprintf("datapoint region %q under resource region %q", dr.AsString(), region.AsString()))
					}
				}
			}
		}
	}
	return count
}

// datapointAttrs returns the per-datapoint attribute maps for a metric across
// the datapoint-bearing metric types influxdbreceiver produces.
func datapointAttrs(m pmetric.Metric) []attrMap {
	var out []attrMap
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			out = append(out, dps.At(i).Attributes())
		}
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			out = append(out, dps.At(i).Attributes())
		}
	case pmetric.MetricTypeHistogram:
		dps := m.Histogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			out = append(out, dps.At(i).Attributes())
		}
	case pmetric.MetricTypeSummary:
		dps := m.Summary().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			out = append(out, dps.At(i).Attributes())
		}
	case pmetric.MetricTypeExponentialHistogram:
		dps := m.ExponentialHistogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			out = append(out, dps.At(i).Attributes())
		}
	}
	return out
}
