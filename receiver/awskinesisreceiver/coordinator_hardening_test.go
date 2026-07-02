package awskinesisreceiver

import (
	"context"
	"sync"
	"testing"
	"time"

	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
	"github.com/jrglee/opentelemetry-kinesis-stream/internal/lease"
)

// TestStartConcurrentWithShutdownNoRace drives start and drainAndStop from
// different goroutines. The component contract serializes Start/Shutdown, but
// the coordinator enforces it itself: baseCtx/stopDiscovery are published
// under mu, and a drainAndStop that wins the race prevents start from
// spawning the discovery loop at all. Under -race this test fails if either
// field is written or read unsynchronized.
func TestStartConcurrentWithShutdownNoRace(t *testing.T) {
	for i := 0; i < 10; i++ {
		fs := &fakeStream{shards: []*fakeShard{{id: "shard-1"}}}
		c := newTestCoordinator(t, fastCoordCfg("s", encoding.EncodingOTLPProto, encoding.CodecNone), fs, noopSink{}, "w")

		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = c.start(ctx)
		}()
		go func() {
			defer wg.Done()
			c.drainAndStop()
		}()
		wg.Wait()
		// Whichever side won, everything must wind down.
		c.drainAndStop()
		done := make(chan struct{})
		go func() { c.wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("coordinator did not stop")
		}
		cancel()
	}
}

// conflictStore fails every Acquire with ErrLeaseConflict, simulating a peer
// that wins the race for every lease.
type conflictStore struct {
	lease.Store
}

func (conflictStore) Acquire(context.Context, string, string, int64) (lease.Lease, error) {
	return lease.Lease{}, lease.ErrLeaseConflict
}

// stealEventCounts collects the lease.events datapoints for the steal label.
func stealEventCounts(t *testing.T, reader *sdkmetric.ManualReader) (success, conflict int64) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	metrics := indexMetrics(t, &rm)
	return leaseEventCount(t, metrics, leaseSteal, resultSuccess),
		leaseEventCount(t, metrics, leaseSteal, resultConflict)
}

// TestStealConflictNotCountedAsSuccess pins outcome-based steal telemetry: a
// steal that loses the counter race must count as steal/conflict, never
// steal/success. (The old code recorded success before attempting the steal.)
func TestStealConflictNotCountedAsSuccess(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	tel, err := newReceiverTelemetry(mp)
	if err != nil {
		t.Fatal(err)
	}

	fs := &fakeStream{shards: []*fakeShard{{id: "shard-1"}}}
	c := newTestCoordinator(t, fastCoordCfg("s", encoding.EncodingOTLPProto, encoding.CodecNone), fs, noopSink{}, "w")
	c.tel = tel
	c.store = conflictStore{Store: c.store}

	c.tryAcquire(context.Background(), lease.Lease{ShardID: "shard-1"}, leaseSteal)

	success, conflict := stealEventCounts(t, reader)
	if success != 0 {
		t.Fatalf("steal/success: got %d want 0 (lost steal must not count as success)", success)
	}
	if conflict != 1 {
		t.Fatalf("steal/conflict: got %d want 1", conflict)
	}
}

// TestStealSuccessCountedOnce covers the other side: a steal that wins the
// conditional write records exactly one steal/success.
func TestStealSuccessCountedOnce(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	tel, err := newReceiverTelemetry(mp)
	if err != nil {
		t.Fatal(err)
	}

	fs := &fakeStream{shards: []*fakeShard{{id: "shard-1"}}}
	c := newTestCoordinator(t, fastCoordCfg("s", encoding.EncodingOTLPProto, encoding.CodecNone), fs, noopSink{}, "w")
	c.tel = tel
	// stopped prevents startPoller from installing a live poller; the acquire
	// itself (and its telemetry) still happens, which is all this test needs.
	c.baseCtx = context.Background()
	c.stopped = true

	if err := c.store.Ensure(context.Background(), "shard-1", nil); err != nil {
		t.Fatal(err)
	}
	c.tryAcquire(context.Background(), lease.Lease{ShardID: "shard-1", Counter: 0}, leaseSteal)

	success, conflict := stealEventCounts(t, reader)
	if success != 1 {
		t.Fatalf("steal/success: got %d want 1", success)
	}
	if conflict != 0 {
		t.Fatalf("steal/conflict: got %d want 0", conflict)
	}
}

// hangingSink blocks in consume until its context would allow exit — but
// deliberately ignores cancellation, modeling a downstream that does not
// honor context. It signals when a consume is in flight.
type hangingSink struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *hangingSink) consume(context.Context, []byte) (recordResult, bool) {
	s.once.Do(func() { close(s.entered) })
	<-s.release // ignores ctx on purpose
	return recordOK, false
}

func (s *hangingSink) deadLetter(context.Context, ktypes.Record, string, string, string) error {
	return nil
}

// TestShutdownDeadlineBoundedWhenConsumerHangs pins the bounded-join contract:
// even when a poller is pinned inside a downstream Consume that ignores
// cancellation, Shutdown must return within the leak grace after its deadline
// instead of wedging the collector at exit.
func TestShutdownDeadlineBoundedWhenConsumerHangs(t *testing.T) {
	const shardID = "hang-shard"
	fs := &fakeStream{shards: []*fakeShard{{id: shardID, records: [][]byte{[]byte("r0")}}}}
	snk := &hangingSink{entered: make(chan struct{}), release: make(chan struct{})}
	c := newTestCoordinator(t, fastCoordCfg("hang", encoding.EncodingOTLPProto, encoding.CodecNone), fs, snk, "w")
	defer close(snk.release) // unblock the leaked poller at test end

	r := &kinesisReceiver{cfg: c.cfg, sink: snk, logger: c.logger, tel: c.tel, coord: c}
	bgCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	if err := c.start(bgCtx); err != nil {
		t.Fatalf("start: %v", err)
	}
	select {
	case <-snk.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("poller never reached the hanging consume")
	}

	expired, expCancel := context.WithCancel(context.Background())
	expCancel() // the collector's deadline has already fired
	start := time.Now()
	err := r.Shutdown(expired)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("deadline shutdown should return the context error")
	}
	if elapsed > shutdownLeakGrace+2*time.Second {
		t.Fatalf("Shutdown blocked %v; must abandon wedged pollers after ~%v", elapsed, shutdownLeakGrace)
	}
}

// TestStartAfterDrainIsNoOp pins the other half of the lifecycle guard: a
// start that loses the race to drainAndStop must not spawn the discovery
// loop or leave anything for wait() to wait on.
func TestStartAfterDrainIsNoOp(t *testing.T) {
	fs := &fakeStream{shards: []*fakeShard{{id: "shard-1"}}}
	c := newTestCoordinator(t, fastCoordCfg("late", encoding.EncodingOTLPProto, encoding.CodecNone), fs, noopSink{}, "w")

	c.drainAndStop()
	if err := c.start(context.Background()); err != nil {
		t.Fatalf("start after drain: %v", err)
	}
	done := make(chan struct{})
	go func() { c.wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("wait() hung: start-after-drain must not spawn the run loop")
	}
}

// TestCleanupSkippedWhenDiscoveryFails pins the absence-counting invariant:
// only passes with a successful discovery may advance the reap counter, so a
// discovery outage cannot accumulate absences against a stale shard snapshot.
func TestCleanupSkippedWhenDiscoveryFails(t *testing.T) {
	fs := &fakeStream{shards: []*fakeShard{{id: "live-shard"}}}
	c := newTestCoordinator(t, fastCoordCfg("gate", encoding.EncodingOTLPProto, encoding.CodecNone), fs, noopSink{}, "w")
	ctx := context.Background()

	// A drained lease for a shard Kinesis no longer lists.
	if err := c.store.Ensure(ctx, "gone-shard", nil); err != nil {
		t.Fatal(err)
	}
	taken, err := c.store.Acquire(ctx, "gone-shard", "w", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.store.Checkpoint(ctx, taken, lease.CheckpointShardEnd); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.liveShards = map[string]bool{"live-shard": true}
	c.mu.Unlock()

	c.reconcile(ctx, false) // discovery failed: must not count the absence
	c.mu.Lock()
	absentAfterFailed := c.absent["gone-shard"]
	c.mu.Unlock()
	if absentAfterFailed != 0 {
		t.Fatalf("absent counted %d on a failed-discovery pass; want 0", absentAfterFailed)
	}

	c.reconcile(ctx, true) // healthy pass counts
	c.mu.Lock()
	absentAfterOK := c.absent["gone-shard"]
	c.mu.Unlock()
	if absentAfterOK != 1 {
		t.Fatalf("absent after healthy pass: got %d want 1", absentAfterOK)
	}
}

// TestAbsentPrunedWhenLeaseDisappears pins the absent-map hygiene in
// refreshObservations: a shard whose lease vanished from the store (deleted by
// a peer) must not leave a dangling absence counter behind, while a shard
// still in the lease table keeps its count.
func TestAbsentPrunedWhenLeaseDisappears(t *testing.T) {
	fs := &fakeStream{shards: []*fakeShard{{id: "kept"}}}
	c := newTestCoordinator(t, fastCoordCfg("s", encoding.EncodingOTLPProto, encoding.CodecNone), fs, noopSink{}, "w")

	c.mu.Lock()
	c.absent["gone"] = 2
	c.absent["kept"] = 1
	c.refreshObservations([]lease.Lease{{ShardID: "kept"}}, time.Now())
	gone, hasGone := c.absent["gone"]
	kept, hasKept := c.absent["kept"]
	c.mu.Unlock()

	if hasGone {
		t.Fatalf("absent[gone] should be pruned, still %d", gone)
	}
	if !hasKept || kept != 1 {
		t.Fatalf("absent[kept]: got %d (present=%v) want 1", kept, hasKept)
	}
}
