package awskinesisreceiver

import (
	"context"
	"sync"
	"testing"
	"time"

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
