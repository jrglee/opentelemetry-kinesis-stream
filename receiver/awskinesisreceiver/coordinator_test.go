package awskinesisreceiver

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/lease"
)

func TestParentsDrained(t *testing.T) {
	tests := []struct {
		name    string
		target  lease.Lease
		all     []lease.Lease
		drained bool
	}{
		{
			name:    "original shard with no parents always drained",
			target:  lease.Lease{ShardID: "s-0"},
			drained: true,
		},
		{
			name:   "child blocked while parent still active",
			target: lease.Lease{ShardID: "s-1", ParentIDs: []string{"s-0"}},
			all: []lease.Lease{
				{ShardID: "s-0", Checkpoint: "49625-abc"},
			},
			drained: false,
		},
		{
			name:   "child unblocked once parent hits SHARD_END",
			target: lease.Lease{ShardID: "s-1", ParentIDs: []string{"s-0"}},
			all: []lease.Lease{
				{ShardID: "s-0", Checkpoint: lease.CheckpointShardEnd},
			},
			drained: true,
		},
		{
			name:   "child blocked when one of two parents not drained (merge)",
			target: lease.Lease{ShardID: "m-2", ParentIDs: []string{"s-0", "s-1"}},
			all: []lease.Lease{
				{ShardID: "s-0", Checkpoint: lease.CheckpointShardEnd},
				{ShardID: "s-1", Checkpoint: "49625-xyz"},
			},
			drained: false,
		},
		{
			name:    "parent trimmed past retention treated as drained",
			target:  lease.Lease{ShardID: "s-1", ParentIDs: []string{"gone"}},
			all:     nil,
			drained: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := parentsDrained(tc.target, indexCheckpoints(tc.all)); got != tc.drained {
				t.Fatalf("parentsDrained=%v want %v", got, tc.drained)
			}
		})
	}
}

func TestCleanupOrphans(t *testing.T) {
	ctx := context.Background()
	// mk builds a coordinator over a store with a live shard and a "gone" shard
	// at SHARD_END (a completed, trimmed shard — the only reapable kind).
	mk := func() (*coordinator, lease.Store) {
		store := lease.NewMemoryStore()
		_ = store.Ensure(ctx, "live", nil)
		_ = store.Ensure(ctx, "gone", nil)
		// Drive "gone" to SHARD_END so it is a completed shard.
		taken, _ := store.Acquire(ctx, "gone", "w", 0)
		if _, err := store.Checkpoint(ctx, taken, lease.CheckpointShardEnd); err != nil {
			t.Fatal(err)
		}
		rel, _ := store.List(ctx)
		for _, l := range rel {
			if l.ShardID == "gone" {
				_ = store.Release(ctx, l)
			}
		}
		c := &coordinator{
			store:      store,
			logger:     zaptest.NewLogger(t),
			tel:        testTelemetry(t),
			cfg:        &Config{LeaseDuration: time.Second},
			active:     map[string]*activePoller{},
			observed:   map[string]observation{},
			absent:     map[string]int{},
			liveShards: map[string]bool{"live": true},
		}
		return c, store
	}
	shardIDs := func(s lease.Store) map[string]bool {
		ls, _ := s.List(ctx)
		out := map[string]bool{}
		for _, l := range ls {
			out[l.ShardID] = true
		}
		return out
	}
	reapWith := func(c *coordinator, store lease.Store, passes int) {
		for i := 0; i < passes; i++ {
			ls, _ := store.List(ctx)
			c.cleanupOrphans(ctx, ls)
		}
	}

	t.Run("SHARD_END shard reaped only after threshold passes", func(t *testing.T) {
		c, store := mk()
		reapWith(c, store, orphanReapThreshold-1)
		if !shardIDs(store)["gone"] {
			t.Fatal("reaped before the absence threshold")
		}
		reapWith(c, store, 1)
		got := shardIDs(store)
		if got["gone"] || !got["live"] {
			t.Fatalf("expected only 'live' after threshold, got %v", got)
		}
	})

	t.Run("active (non-SHARD_END) checkpoint is never reaped despite absence", func(t *testing.T) {
		c, store := mk()
		// An absent shard with a real sequence-number checkpoint is a
		// consistency blip, not a trim — must survive any number of passes.
		taken, _ := store.Acquire(ctx, "live", "w", 0)
		cp, _ := store.Checkpoint(ctx, taken, "49000-active")
		_ = store.Release(ctx, cp)
		c.liveShards = map[string]bool{} // nothing live...
		// ...but len(live)==0 short-circuits; make a different shard live so the
		// pass runs while "live" (now carrying a real checkpoint) is absent.
		c.liveShards = map[string]bool{"gone": true}
		reapWith(c, store, orphanReapThreshold+2)
		if !shardIDs(store)["live"] {
			t.Fatal("reaped an active (non-SHARD_END) lease that was merely absent")
		}
	})

	t.Run("empty liveShards skips cleanup (failed discovery)", func(t *testing.T) {
		c, store := mk()
		c.liveShards = map[string]bool{}
		reapWith(c, store, orphanReapThreshold+2)
		if !shardIDs(store)["gone"] {
			t.Fatal("cleanup ran on empty live set and deleted a lease")
		}
	})

	t.Run("reappearing shard resets the absence counter", func(t *testing.T) {
		c, store := mk()
		reapWith(c, store, orphanReapThreshold-1)
		// Shard comes back (consistency blip resolved): counter resets.
		c.liveShards = map[string]bool{"live": true, "gone": true}
		reapWith(c, store, 1)
		c.liveShards = map[string]bool{"live": true}
		reapWith(c, store, orphanReapThreshold-1)
		if !shardIDs(store)["gone"] {
			t.Fatal("absence counter did not reset on reappearance")
		}
	})

	t.Run("actively-polled trimmed shard is not reaped", func(t *testing.T) {
		c, store := mk()
		c.active["gone"] = &activePoller{}
		reapWith(c, store, orphanReapThreshold+2)
		if !shardIDs(store)["gone"] {
			t.Fatal("reaped a shard with an active poller")
		}
	})

	t.Run("fresh-owned trimmed shard is not reaped", func(t *testing.T) {
		c, store := mk()
		var counter int64
		for _, l := range mustList(t, store) {
			if l.ShardID == "gone" {
				counter = l.Counter
			}
		}
		taken, err := store.Acquire(ctx, "gone", "w-1", counter)
		if err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		c.observed["gone"] = observation{counter: taken.Counter, seenAt: time.Now()}
		reapWith(c, store, orphanReapThreshold+2)
		if !shardIDs(store)["gone"] {
			t.Fatal("reaped a freshly-owned shard")
		}
	})
}

func mustList(t *testing.T, s lease.Store) []lease.Lease {
	t.Helper()
	ls, err := s.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return ls
}

// TestStartPollerAbortsAfterStop guards the shutdown TOCTOU: a poller acquired in
// the shutdown window must not install itself after drainAndStop has already
// snapshotted the active set, or it would poll on undrained and skip its graceful
// final checkpoint/release. drainAndStop sets stopped under the same lock that
// snapshots active, so startPoller reaching its install after that must abort.
func TestStartPollerAbortsAfterStop(t *testing.T) {
	ctx := context.Background()
	c := &coordinator{
		baseCtx: ctx,
		store:   lease.NewMemoryStore(),
		logger:  zaptest.NewLogger(t),
		tel:     testTelemetry(t),
		cfg:     &Config{LeaseDuration: time.Second},
		active:  map[string]*activePoller{},
	}

	// Simulate drainAndStop having begun.
	c.mu.Lock()
	c.stopped = true
	c.mu.Unlock()

	c.startPoller(ctx, lease.Lease{ShardID: "s1"})

	c.mu.Lock()
	_, installed := c.active["s1"]
	c.mu.Unlock()
	if installed {
		t.Fatal("startPoller installed a poller after stop; drainAndStop's drain would miss it")
	}
	// No poller goroutine should have started, so wait returns immediately.
	c.wait()
}
