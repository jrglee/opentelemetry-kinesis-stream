//go:build e2e

// Logs E2E: telemetrygen emits log records through a producer collector
// (OTLP -> Kinesis); MiniStack emulates Kinesis/DynamoDB; a consumer
// collector (Kinesis -> file) writes OTLP-JSON logs back out. The test
// asserts every emitted log record arrives exactly once.
package e2e

import (
	"bufio"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
)

const (
	logsEmitted  = 200
	logsLeaseTbl = "otel-leases-logs"
	logsOutFile  = "logs-out.jsonl"
	logsDeliver  = 90 * time.Second
	logsSettle   = 10 * time.Second
)

// composeLogs runs `docker compose` against the logs stack file. Mirrors the
// traces and metrics harnesses but selects docker-compose.logs.yaml so the
// three stacks stay independent.
func composeLogs(t *testing.T, env []string, timeout time.Duration, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	full := append([]string{"compose", "-f", "docker-compose.logs.yaml"}, args...)
	cmd := exec.CommandContext(ctx, "docker", full...)
	cmd.Dir = composeDir(t)
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestLogsRoundTrip(t *testing.T) {
	requireDocker(t)

	shared := t.TempDir()
	env := composeEnv()

	t.Cleanup(func() {
		out, err := composeLogs(t, env, 60*time.Second, "down", "-v")
		if err != nil {
			t.Logf("compose down failed: %v\n%s", err, out)
		}
	})

	// Build the collector image once, then start without --build so parallel
	// writers do not race the shared otelcol-kinesis:dev tag (the same gotcha
	// as the traces stack).
	if out, err := composeLogs(t, env, 5*time.Minute, "build", "producer-logs"); err != nil {
		t.Fatalf("compose build: %v\n%s", err, out)
	}
	if out, err := composeLogs(t, env, 3*time.Minute, "up", "-d"); err != nil {
		t.Fatalf("compose up: %v\n%s", err, out)
	}

	// Wait until the consumer owns at least one shard before generating load,
	// so the GetRecords loop is live when records land.
	waitForLogsOwnership(t)

	// telemetrygen connects to the producer's OTLP listener; wait for the port
	// to accept connections so a fast --rate=0 burst is not dropped before the
	// gRPC server binds.
	waitForProducer(t)

	if out, err := composeLogs(t, env, 90*time.Second, "run", "--rm", "telemetrygen"); err != nil {
		t.Fatalf("telemetrygen: %v\n%s", err, out)
	}

	got := waitForLogs(t, env, shared, logsEmitted)
	if got != logsEmitted {
		t.Fatalf("log records = %d, want %d (loss)", got, logsEmitted)
	}

	// Settle so a late straggler or over-delivery has time to surface, then
	// re-assert the count.
	time.Sleep(logsSettle)
	final := readLogs(t, env, shared)
	if final != logsEmitted {
		t.Fatalf("after settle, log records = %d, want %d", final, logsEmitted)
	}
	// No per-record uniqueness assertion: telemetrygen's `logs` subcommand
	// emits the same body for every record, so we cannot distinguish a unique
	// record from a duplicate by content. This stack runs a single consumer
	// replica; the multi-replica no-double-delivery property is already
	// covered by the traces E2E and the receiver's matrix correctness sweep.
	t.Logf("logs round-trip OK: %d records delivered", final)
}

// waitForLogsOwnership polls the logs lease table until at least one shard has
// an owner, proving the consumer's lease loop is live.
func waitForLogsOwnership(t *testing.T) {
	t.Helper()
	client := dynamoClient(t)
	deadline := time.Now().Add(settleDeadline)
	for time.Now().Before(deadline) {
		_, owned, total := scanOwnersTable(t, client, logsLeaseTbl)
		if total >= 1 && owned >= 1 {
			t.Logf("logs shard ownership live: %d/%d leases owned", owned, total)
			return
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatal("timed out waiting for logs shard ownership")
}

// waitForLogs polls the consumer output until the log record count reaches
// want (or the deadline passes), returning the last count seen.
func waitForLogs(t *testing.T, env []string, shared string, want int) int {
	t.Helper()
	deadline := time.Now().Add(logsDeliver)
	var count int
	for time.Now().Before(deadline) {
		count = readLogs(t, env, shared)
		if count >= want {
			return count
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out: got %d/%d log records", count, want)
	return count
}

// readLogs copies the consumer output off the shared volume and returns the
// total log record count across every OTLP-JSON line in the file.
func readLogs(t *testing.T, env []string, shared string) int {
	t.Helper()
	copySharedLogsFrom(t, env, shared)

	path := filepath.Join(shared, logsOutFile)
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()

	unmarshaler := &plog.JSONUnmarshaler{}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)
	count := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		ld, err := unmarshaler.UnmarshalLogs([]byte(line))
		if err != nil {
			// `docker compose cp` can snapshot the file mid-write, truncating
			// the final line; the caller re-polls until the count stabilizes.
			continue
		}
		count += ld.LogRecordCount()
	}
	return count
}

func copySharedLogsFrom(t *testing.T, env []string, dest string) {
	t.Helper()
	if out, err := composeLogs(t, env, 30*time.Second, "cp", "consumer-logs:/shared/.", dest); err != nil {
		t.Logf("cp shared (not ready yet?): %v\n%s", err, out)
	}
}
