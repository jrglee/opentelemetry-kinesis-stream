package lease

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestEnsureIsIdempotent(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	if err := s.Ensure(ctx, "shard-1", nil); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if err := s.Ensure(ctx, "shard-1", []string{"parent-a"}); err != nil {
		t.Fatalf("Ensure (second): %v", err)
	}
	got, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len=%d want 1", len(got))
	}
	if got[0].Checkpoint != CheckpointTrimHorizon {
		t.Fatalf("checkpoint=%q", got[0].Checkpoint)
	}
	if len(got[0].ParentIDs) != 0 {
		t.Fatalf("Ensure overrode parents on existing row")
	}
}

func TestAcquireFencesStaleWriter(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	mustEnsure(t, s, "shard-1")

	a, err := s.Acquire(ctx, "shard-1", "worker-A", 0)
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	if a.Owner != "worker-A" || a.Counter != 1 {
		t.Fatalf("first acquire returned %+v", a)
	}

	// A stale Acquire with the same expected counter must fail.
	if _, err := s.Acquire(ctx, "shard-1", "worker-B", 0); !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("stale Acquire err=%v, want ErrLeaseConflict", err)
	}
}

func TestHeartbeatRequiresOwnerAndCounter(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	mustEnsure(t, s, "shard-1")
	a, _ := s.Acquire(ctx, "shard-1", "worker-A", 0)

	hb, err := s.Heartbeat(ctx, a)
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if hb.Counter != a.Counter+1 {
		t.Fatalf("Heartbeat did not bump counter: %d -> %d", a.Counter, hb.Counter)
	}

	// Heartbeating with the old lease must conflict.
	if _, err := s.Heartbeat(ctx, a); !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("stale Heartbeat err=%v, want ErrLeaseConflict", err)
	}

	// Heartbeating as the wrong owner must conflict.
	wrong := hb
	wrong.Owner = "worker-B"
	if _, err := s.Heartbeat(ctx, wrong); !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("wrong-owner Heartbeat err=%v, want ErrLeaseConflict", err)
	}
}

func TestCheckpointPersistsAndFences(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	mustEnsure(t, s, "shard-1")
	a, _ := s.Acquire(ctx, "shard-1", "worker-A", 0)

	cp, err := s.Checkpoint(ctx, a, "49625-abc")
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if cp.Checkpoint != "49625-abc" {
		t.Fatalf("checkpoint=%q", cp.Checkpoint)
	}
	if cp.Counter != a.Counter+1 {
		t.Fatalf("checkpoint did not bump counter")
	}
	if _, err := s.Checkpoint(ctx, a, "49625-def"); !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("stale Checkpoint err=%v, want ErrLeaseConflict", err)
	}
}

func TestReleaseClearsOwnerAndAllowsTakeover(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	mustEnsure(t, s, "shard-1")
	a, _ := s.Acquire(ctx, "shard-1", "worker-A", 0)
	if err := s.Release(ctx, a); err != nil {
		t.Fatalf("Release: %v", err)
	}
	all, _ := s.List(ctx)
	if all[0].Owner != "" {
		t.Fatalf("Owner not cleared: %q", all[0].Owner)
	}
	// worker-B can now Acquire with the post-release counter.
	if _, err := s.Acquire(ctx, "shard-1", "worker-B", all[0].Counter); err != nil {
		t.Fatalf("takeover Acquire: %v", err)
	}
}

func TestConcurrentAcquireSerializes(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	mustEnsure(t, s, "shard-1")

	const N = 50
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		winners  int
		conflict int
	)
	wg.Add(N)
	for i := range N {
		go func(i int) {
			defer wg.Done()
			if _, err := s.Acquire(ctx, "shard-1", "worker", 0); err == nil {
				mu.Lock()
				winners++
				mu.Unlock()
			} else if errors.Is(err, ErrLeaseConflict) {
				mu.Lock()
				conflict++
				mu.Unlock()
			}
			_ = i
		}(i)
	}
	wg.Wait()
	if winners != 1 {
		t.Fatalf("winners=%d want 1", winners)
	}
	if conflict != N-1 {
		t.Fatalf("conflicts=%d want %d", conflict, N-1)
	}
}

func mustEnsure(t *testing.T, s *MemoryStore, shardID string) {
	t.Helper()
	if err := s.Ensure(context.Background(), shardID, nil); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
}
