package lease

import (
	"context"
	"errors"
	"reflect"
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

func TestEnsurePreservesParentsAndCheckpoint(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	if err := s.Ensure(ctx, "child", []string{"p-0", "p-1"}); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	got, _ := s.List(ctx)
	if len(got) != 1 || len(got[0].ParentIDs) != 2 {
		t.Fatalf("parents not stored: %+v", got)
	}
	// A re-Ensure with different parents must not clobber the original row.
	if err := s.Ensure(ctx, "child", []string{"other"}); err != nil {
		t.Fatalf("re-Ensure: %v", err)
	}
	got, _ = s.List(ctx)
	want := []string{"p-0", "p-1"}
	if !reflect.DeepEqual(got[0].ParentIDs, want) {
		t.Fatalf("parents=%v want %v", got[0].ParentIDs, want)
	}
	// List must return a clone: mutating the result must not affect the store.
	got[0].ParentIDs[0] = "tampered"
	again, _ := s.List(ctx)
	if again[0].ParentIDs[0] != "p-0" {
		t.Fatalf("List did not return a defensive copy: %v", again[0].ParentIDs)
	}
}

func TestIsOwnedBy(t *testing.T) {
	cases := []struct {
		name  string
		lease Lease
		owner string
		want  bool
	}{
		{"matching owner", Lease{Owner: "w-1"}, "w-1", true},
		{"different owner", Lease{Owner: "w-1"}, "w-2", false},
		{"unowned never matches empty query", Lease{Owner: ""}, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.lease.IsOwnedBy(tc.owner); got != tc.want {
				t.Fatalf("IsOwnedBy(%q)=%v want %v", tc.owner, got, tc.want)
			}
		})
	}
}

func TestListEmptyStore(t *testing.T) {
	got, err := NewMemoryStore().List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("len=%d want 0", len(got))
	}
}

// TestMissingRowYieldsNotFound exercises the ErrLeaseNotFound branch on every
// mutating method: a fencing token cannot reference a row that does not exist.
func TestMissingRowYieldsNotFound(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	ghost := Lease{ShardID: "missing", Owner: "worker-A", Counter: 3}

	if _, err := s.Acquire(ctx, "missing", "worker-A", 0); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("Acquire err=%v want ErrLeaseNotFound", err)
	}
	if _, err := s.Heartbeat(ctx, ghost); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("Heartbeat err=%v want ErrLeaseNotFound", err)
	}
	if _, err := s.Checkpoint(ctx, ghost, "seq"); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("Checkpoint err=%v want ErrLeaseNotFound", err)
	}
	if err := s.Release(ctx, ghost); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("Release err=%v want ErrLeaseNotFound", err)
	}
}

// TestReleaseFencesStaleOwner proves Release is gated on owner+counter: a
// stale lease (one Heartbeat behind) must not be able to release the row out
// from under the current owner.
func TestReleaseFencesStaleOwner(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	mustEnsure(t, s, "shard-1")
	a, _ := s.Acquire(ctx, "shard-1", "worker-A", 0)
	hb, _ := s.Heartbeat(ctx, a)

	// Releasing with the pre-heartbeat lease must conflict, leaving ownership.
	if err := s.Release(ctx, a); !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("stale Release err=%v want ErrLeaseConflict", err)
	}
	cur, _ := s.List(ctx)
	if cur[0].Owner != "worker-A" {
		t.Fatalf("stale Release cleared owner: %q", cur[0].Owner)
	}
	// The current lease releases cleanly.
	if err := s.Release(ctx, hb); err != nil {
		t.Fatalf("fresh Release: %v", err)
	}
}

func mustEnsure(t *testing.T, s *MemoryStore, shardID string) {
	t.Helper()
	if err := s.Ensure(context.Background(), shardID, nil); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
}

func TestDelete(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	mustEnsure(t, s, "shard-1")
	got, _ := s.List(ctx)
	counter := got[0].Counter

	// Counter mismatch must not delete.
	if err := s.Delete(ctx, "shard-1", counter+1); !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("stale Delete err=%v want ErrLeaseConflict", err)
	}
	if all, _ := s.List(ctx); len(all) != 1 {
		t.Fatalf("lease deleted despite counter mismatch")
	}
	// Correct counter deletes.
	if err := s.Delete(ctx, "shard-1", counter); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if all, _ := s.List(ctx); len(all) != 0 {
		t.Fatalf("lease not deleted: %+v", all)
	}
	// Deleting an absent row is a no-op.
	if err := s.Delete(ctx, "shard-1", 0); err != nil {
		t.Fatalf("idempotent Delete: %v", err)
	}
}
