package lease

import (
	"context"
	"slices"
	"sync"
)

// MemoryStore is an in-process Store for unit tests and single-replica
// development. Concurrent across goroutines, not across processes.
type MemoryStore struct {
	mu     sync.Mutex
	leases map[string]Lease
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{leases: make(map[string]Lease)}
}

// List implements [Store].
func (s *MemoryStore) List(_ context.Context) ([]Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Lease, 0, len(s.leases))
	for _, l := range s.leases {
		out = append(out, cloneLease(l))
	}
	return out, nil
}

// Ensure implements [Store].
func (s *MemoryStore) Ensure(_ context.Context, shardID string, parentIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.leases[shardID]; ok {
		return nil
	}
	s.leases[shardID] = Lease{
		ShardID:    shardID,
		Counter:    0,
		Checkpoint: CheckpointTrimHorizon,
		ParentIDs:  append([]string(nil), parentIDs...),
	}
	return nil
}

// Acquire implements [Store].
func (s *MemoryStore) Acquire(_ context.Context, shardID, owner string, expectedCounter int64) (Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.leases[shardID]
	if !ok {
		return Lease{}, ErrLeaseNotFound
	}
	if cur.Counter != expectedCounter {
		return Lease{}, ErrLeaseConflict
	}
	cur.Owner = owner
	cur.Counter++
	s.leases[shardID] = cur
	return cloneLease(cur), nil
}

// Heartbeat implements [Store].
func (s *MemoryStore) Heartbeat(_ context.Context, lease Lease) (Lease, error) {
	return s.conditionalUpdate(lease, func(l *Lease) { l.Counter++ })
}

// Checkpoint implements [Store].
func (s *MemoryStore) Checkpoint(_ context.Context, lease Lease, seq string) (Lease, error) {
	return s.conditionalUpdate(lease, func(l *Lease) {
		l.Checkpoint = seq
		l.Counter++
	})
}

// Release implements [Store].
func (s *MemoryStore) Release(_ context.Context, lease Lease) error {
	_, err := s.conditionalUpdate(lease, func(l *Lease) {
		l.Owner = ""
		l.Counter++
	})
	return err
}

func (s *MemoryStore) conditionalUpdate(expected Lease, mutate func(*Lease)) (Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.leases[expected.ShardID]
	if !ok {
		return Lease{}, ErrLeaseNotFound
	}
	if cur.Owner != expected.Owner || cur.Counter != expected.Counter {
		return Lease{}, ErrLeaseConflict
	}
	mutate(&cur)
	s.leases[expected.ShardID] = cur
	return cloneLease(cur), nil
}

func cloneLease(l Lease) Lease {
	l.ParentIDs = slices.Clone(l.ParentIDs)
	return l
}
