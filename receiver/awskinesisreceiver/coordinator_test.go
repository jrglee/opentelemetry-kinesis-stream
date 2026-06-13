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
	mk := func() (*coordinator, lease.Store) {
		store := lease.NewMemoryStore()
		_ = store.Ensure(ctx, "live", nil)
		_ = store.Ensure(ctx, "trimmed", nil)
		c := &coordinator{
			store:      store,
			logger:     zaptest.NewLogger(t),
			cfg:        &Config{LeaseDuration: time.Second},
			active:     map[string]*activePoller{},
			observed:   map[string]observation{},
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

	t.Run("trimmed unowned lease is reaped", func(t *testing.T) {
		c, store := mk()
		ls, _ := store.List(ctx)
		c.cleanupOrphans(ctx, ls)
		got := shardIDs(store)
		if got["trimmed"] || !got["live"] {
			t.Fatalf("expected only 'live' to remain, got %v", got)
		}
	})

	t.Run("empty liveShards skips cleanup (failed discovery)", func(t *testing.T) {
		c, store := mk()
		c.liveShards = map[string]bool{}
		ls, _ := store.List(ctx)
		c.cleanupOrphans(ctx, ls)
		if !shardIDs(store)["trimmed"] {
			t.Fatal("cleanup ran on empty live set and deleted a lease")
		}
	})

	t.Run("actively-polled trimmed shard is not reaped", func(t *testing.T) {
		c, store := mk()
		c.active["trimmed"] = &activePoller{}
		ls, _ := store.List(ctx)
		c.cleanupOrphans(ctx, ls)
		if !shardIDs(store)["trimmed"] {
			t.Fatal("reaped a shard with an active poller")
		}
	})

	t.Run("fresh-owned trimmed shard is not reaped", func(t *testing.T) {
		c, store := mk()
		taken, _ := store.Acquire(ctx, "trimmed", "w-1", 0)
		c.observed["trimmed"] = observation{counter: taken.Counter, seenAt: time.Now()}
		ls, _ := store.List(ctx)
		c.cleanupOrphans(ctx, ls)
		if !shardIDs(store)["trimmed"] {
			t.Fatal("reaped a freshly-owned shard")
		}
	})
}
