//go:build e2e

// This file drives the metrics E2E: a load generator emits InfluxDB line
// protocol to a producer collector (influxdb -> awskinesis, no processors),
// MiniStack emulates Kinesis/DynamoDB, and a consumer collector
// (Kinesis -> file) writes OTLP-JSON metrics back out.
//
// The test asserts:
//   - no datapoint loss (count == total sent)
//   - every datapoint carries device_id as a datapoint attribute (groupbyattrs
//     is gone, so tags must survive on the datapoint, not be hoisted to resource)
//   - every datapoint carries a promoted "subsystem" attribute whose value
//     matches the ^([a-z]+)_ prefix of the metric name
//   - the set of distinct device_id values seen equals the count linegen reports
//   - the set of distinct subsystem values seen equals the count linegen reports
package e2e

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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

// subsystemRe is compiled once and reused by regexSubsystem.
var subsystemRe = regexp.MustCompile(`^([a-z]+)_`)

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

	// Run the load generator and parse its stdout contract for the expected total,
	// distinct device count, and distinct subsystem count.
	out, err := composeInflux(t, env, 90*time.Second, "run", "--rm", "linegen")
	if err != nil {
		t.Fatalf("linegen: %v\n%s", err, out)
	}
	total, devices, subsystems := parseLinegen(t, out)
	t.Logf("linegen sent %d measurements, %d distinct devices, %d distinct subsystems",
		total, devices, subsystems)

	// Poll the consumer output until the datapoint count reaches the emitted
	// total (no loss), tolerating partial mid-write lines.
	got := waitForDatapoints(t, env, shared, total)
	if got != total {
		t.Fatalf("datapoint count = %d, want %d (loss)", got, total)
	}

	// Settle: keep reading past first reaching the target so a late duplicate or
	// straggler surfaces before asserting.
	time.Sleep(metricsSettle)
	count, deviceSet, subsystemSet, violations := readDatapoints(t, env, shared)
	if count != total {
		t.Fatalf("after settle, datapoint count = %d, want %d", count, total)
	}
	if len(violations) > 0 {
		t.Fatalf("attribute violations for %d datapoint(s); sample: %s", len(violations), violations[0])
	}
	if len(deviceSet) != devices {
		t.Fatalf("observed %d distinct device_id values, want %d", len(deviceSet), devices)
	}
	if len(subsystemSet) != subsystems {
		t.Fatalf("observed %d distinct subsystem values, want %d", len(subsystemSet), subsystems)
	}
	t.Logf("metrics round-trip OK: %d datapoints, %d devices, %d subsystems, no violations",
		count, len(deviceSet), len(subsystemSet))
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

// parseLinegen reads linegen's stdout contract:
//
//	LINEGEN_SENT <n>
//	LINEGEN_DISTINCT_DEVICES <n>
//	LINEGEN_DISTINCT_SUBSYSTEMS <n>
func parseLinegen(t *testing.T, out string) (total, devices, subsystems int) {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) != 2 {
			continue
		}
		switch fields[0] {
		case "LINEGEN_SENT":
			total = mustAtoi(t, fields[1])
		case "LINEGEN_DISTINCT_DEVICES":
			devices = mustAtoi(t, fields[1])
		case "LINEGEN_DISTINCT_SUBSYSTEMS":
			subsystems = mustAtoi(t, fields[1])
		}
	}
	if total == 0 || devices == 0 || subsystems == 0 {
		t.Fatalf("could not parse linegen output (total=%d devices=%d subsystems=%d):\n%s",
			total, devices, subsystems, out)
	}
	return total, devices, subsystems
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
		count, _, _, _ = readDatapoints(t, env, shared)
		if count >= want {
			return count
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out: got %d/%d datapoints", count, want)
	return count
}

// readDatapoints copies the consumer output off the shared volume and parses
// each OTLP-JSON line, returning the total datapoint count, the set of distinct
// device_id values, the set of distinct promoted subsystem values, and any
// attribute violations found.
func readDatapoints(t *testing.T, env []string, shared string) (count int, deviceSet, subsystemSet map[string]struct{}, violations []string) {
	t.Helper()
	copySharedFrom(t, env, "consumer-metrics", shared)
	deviceSet = map[string]struct{}{}
	subsystemSet = map[string]struct{}{}

	path := filepath.Join(shared, metricsOutFile)
	f, err := os.Open(path)
	if err != nil {
		return 0, deviceSet, subsystemSet, nil // not created yet
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
		count += tallyDatapoints(md, deviceSet, subsystemSet, &violations)
	}
	return count, deviceSet, subsystemSet, violations
}

// tallyDatapoints walks every datapoint and asserts the new attribute contract:
//   - device_id must be present as a datapoint attribute (groupbyattrs is gone,
//     so the InfluxDB tag must survive on the datapoint, not be hoisted away)
//   - subsystem must be present as a promoted datapoint attribute
//   - subsystem must equal regexSubsystem(m.Name()), proving the promotion
//     derived from the actual received metric name
//
// It collects the distinct device_id and subsystem values into the caller's sets.
func tallyDatapoints(md pmetric.Metrics, deviceSet, subsystemSet map[string]struct{}, violations *[]string) int {
	count := 0
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			metrics := sms.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				m := metrics.At(k)
				expectedSubsystem := regexSubsystem(m.Name())
				for _, dpAttrs := range datapointAttrs(m) {
					count++

					// device_id must survive as a datapoint attribute.
					if dev, ok := dpAttrs.Get("device_id"); ok {
						deviceSet[dev.AsString()] = struct{}{}
					} else {
						*violations = append(*violations,
							fmt.Sprintf("metric %q datapoint missing device_id attribute", m.Name()))
					}

					// subsystem must be promoted onto the datapoint and must match
					// the regex-extracted prefix of the metric name.
					if sub, ok := dpAttrs.Get("subsystem"); ok {
						subsystemSet[sub.AsString()] = struct{}{}
						if expectedSubsystem != "" && sub.AsString() != expectedSubsystem {
							*violations = append(*violations,
								fmt.Sprintf("metric %q: promoted subsystem %q != expected %q",
									m.Name(), sub.AsString(), expectedSubsystem))
						}
					} else {
						*violations = append(*violations,
							fmt.Sprintf("metric %q datapoint missing promoted subsystem attribute", m.Name()))
					}
				}
			}
		}
	}
	return count
}

// regexSubsystem applies the ^([a-z]+)_ pattern to name and returns the first
// capture group (the subsystem prefix), or "" if there is no match.
func regexSubsystem(name string) string {
	m := subsystemRe.FindStringSubmatch(name)
	if len(m) < 2 {
		return ""
	}
	return m[1]
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
