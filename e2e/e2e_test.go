//go:build e2e

// Package e2e drives the full docker-compose stack: a producer collector
// (OTLP -> Kinesis), MiniStack as the Kinesis/DynamoDB emulator, and two
// consumer collectors (Kinesis -> file) sharing one DynamoDB lease table.
// telemetrygen emits a fixed number of spans through the producer; the test
// asserts every span arrives exactly once across the two consumers, which
// proves both the wire round-trip and multi-replica lease coordination.
package e2e

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

const (
	tracesEmitted  = 100
	spansPerTrace  = 2 // telemetrygen emits a parent+child pair per trace
	expectedSpans  = tracesEmitted * spansPerTrace
	settleDeadline = 60 * time.Second
	deliverWait    = 90 * time.Second
	settleWindow   = 12 * time.Second
)

func TestRoundTripMultiReplica(t *testing.T) {
	requireDocker(t)

	// Host-local scratch dir that consumer output is copied into (not a bind
	// mount — see harness copyShared).
	shared := t.TempDir()
	env := composeEnv()

	t.Cleanup(func() {
		out, err := compose(t, env, 60*time.Second, "down", "-v")
		if err != nil {
			t.Logf("compose down failed: %v\n%s", err, out)
		}
	})

	// Build the shared image once. All three collector services reference the
	// same otelcol-kinesis:dev tag; letting `up --build` build them in parallel
	// races three writers onto one tag (AlreadyExists under the classic
	// builder). Build a single service, then start without --build.
	if out, err := compose(t, env, 5*time.Minute, "build", "producer"); err != nil {
		t.Fatalf("compose build: %v\n%s", err, out)
	}
	if out, err := compose(t, env, 3*time.Minute, "up", "-d"); err != nil {
		t.Fatalf("compose up: %v\n%s", err, out)
	}

	// Wait until shard ownership has rebalanced to a stable even split across
	// the two replicas before emitting. This both proves the leaderless
	// fair-share rebalancing converges and ensures no steal happens during
	// delivery — stealing is at-least-once around the handoff, so measuring
	// exactly-once delivery requires a settled assignment first.
	waitForBalancedOwnership(t)

	// telemetrygen connects to the producer's OTLP listener; wait for it to
	// accept connections so a fast --rate=0 burst is not dropped before the
	// gRPC server binds.
	waitForProducer(t)

	if out, err := compose(t, env, 90*time.Second, "run", "--rm", "telemetrygen"); err != nil {
		t.Fatalf("telemetrygen: %v\n%s", err, out)
	}

	unique, raw, perFile := waitForSpans(t, env, shared)
	// No loss: every emitted span is present.
	if unique != expectedSpans {
		t.Fatalf("unique spans = %d, want %d (loss)", unique, expectedSpans)
	}

	// Settle: keep reading past the point the count was first reached, so a
	// duplicate delivered a beat later (e.g. during a lease handoff) is
	// actually observed before we assert. Without this, raw==unique could be
	// sampled too early and mask a real double-delivery.
	unique, raw, perFile = settleAndRead(t, env, shared)

	// No loss after settle.
	if unique != expectedSpans {
		t.Fatalf("after settle, unique spans = %d, want %d", unique, expectedSpans)
	}
	// Bounded duplication: the receiver is at-least-once, so a bootstrap steal
	// during fair-share rebalance may re-deliver a stolen shard's in-flight window
	// (raw > unique by a few). That is correct. Fail only on gross double-delivery
	// — a shard delivered to two replicas for its whole life, which is ~half the
	// spans, far above this tolerance.
	dupes := raw - unique
	if tol := atLeastOnceDupTolerance(expectedSpans); dupes > tol {
		t.Fatalf("raw span occurrences = %d vs unique = %d: %d duplicates exceed the at-least-once tolerance %d (gross double-delivery)", raw, unique, dupes, tol)
	} else if dupes > 0 {
		t.Logf("observed %d duplicate span deliveries within at-least-once tolerance %d", dupes, tol)
	}
	// With rebalancing the shards split across replicas, so both consumers must
	// have delivered some spans — this confirms the round trip end-to-end on a
	// genuinely distributed assignment, not just a single active reader.
	if perFile["a"] == 0 || perFile["b"] == 0 {
		t.Fatalf("expected both consumers to deliver spans after rebalancing, got a=%d b=%d", perFile["a"], perFile["b"])
	}
	t.Logf("round-trip OK: %d unique spans across a balanced split (consumer-a=%d, consumer-b=%d), %d duplicate(s)",
		unique, perFile["a"], perFile["b"], raw-unique)
}

// settleAndRead reads the consumer output repeatedly over a short window after
// the expected count has been reached, returning the final counts. It exists
// so a late duplicate (raw > unique) has time to surface before the caller
// asserts the no-double-delivery property.
func settleAndRead(t *testing.T, env []string, shared string) (unique, raw int, perFile map[string]int) {
	t.Helper()
	deadline := time.Now().Add(settleWindow)
	for time.Now().Before(deadline) {
		unique, raw, perFile = readSpanIDs(t, env, shared)
		if raw > unique {
			return unique, raw, perFile // duplicate already visible; stop early
		}
		time.Sleep(2 * time.Second)
	}
	return unique, raw, perFile
}

// waitForSpans polls the two consumer output files until the unique span set
// reaches expectedSpans (or the deadline passes), then returns the unique
// count, the raw occurrence count across both files, and the per-replica raw
// counts. raw > unique signals a span delivered to more than one replica.
func waitForSpans(t *testing.T, env []string, shared string) (unique, raw int, perFile map[string]int) {
	t.Helper()
	deadline := time.Now().Add(deliverWait)
	for time.Now().Before(deadline) {
		unique, raw, perFile = readSpanIDs(t, env, shared)
		if unique >= expectedSpans {
			return unique, raw, perFile
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out: got %d/%d unique spans (consumer-a=%d, consumer-b=%d)",
		unique, expectedSpans, perFile["a"], perFile["b"])
	return unique, raw, perFile
}

func readSpanIDs(t *testing.T, env []string, shared string) (unique, raw int, perFile map[string]int) {
	t.Helper()
	copyShared(t, env, shared)
	ids := make(map[[24]byte]struct{})
	perFile = map[string]int{}
	for _, replica := range []string{"a", "b"} {
		path := filepath.Join(shared, "out-"+replica+".jsonl")
		n := collectFromFile(t, path, ids)
		perFile[replica] = n
		raw += n
	}
	return len(ids), raw, perFile
}

// collectFromFile parses each JSON line as OTLP traces and adds every span's
// (traceID,spanID) key to the set. Returns the number of spans seen.
func collectFromFile(t *testing.T, path string, ids map[[24]byte]struct{}) int {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		return 0 // not created yet
	}
	defer f.Close()

	unmarshaler := &ptrace.JSONUnmarshaler{}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)
	count := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		td, err := unmarshaler.UnmarshalTraces([]byte(line))
		if err != nil {
			// `docker compose cp` can snapshot the file mid-write, truncating
			// the final line. The caller re-polls until the count stabilizes,
			// so a partial line is complete on a later read — skip, don't fail.
			continue
		}
		count += addSpanIDs(td, ids)
	}
	return count
}

// addSpanIDs adds each span's (traceID,spanID) key to the set and returns the
// number of spans seen on this Traces (including any that were already in the
// set, so the caller's raw count reflects duplicate deliveries).
func addSpanIDs(td ptrace.Traces, ids map[[24]byte]struct{}) int {
	count := 0
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				s := spans.At(k)
				var key [24]byte
				tid := s.TraceID()
				sid := s.SpanID()
				copy(key[:16], tid[:])
				copy(key[16:], sid[:])
				ids[key] = struct{}{}
				count++
			}
		}
	}
	return count
}
