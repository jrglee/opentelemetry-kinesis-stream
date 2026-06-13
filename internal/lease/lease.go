// Package lease coordinates Kinesis shard ownership and checkpoint progress
// across one or more receiver replicas.
//
// The contract is a small Store interface so the backing storage stays
// pluggable: an in-memory implementation is used for unit tests and
// single-replica development, and a DynamoDB implementation provides the
// multi-replica path. The DynamoDB table layout intentionally mirrors KCL's
// lease table so a KCL consumer can take over, or be migrated from, the
// same stream without re-ingesting from TRIM_HORIZON. The full KCL state
// machine is *not* implemented — only the subset needed for the simplified
// coordination model documented in the receiver's ADRs.
package lease

import (
	"context"
	"errors"
)

// Checkpoint sentinels used in place of a real sequence number.
const (
	// CheckpointTrimHorizon means start at the oldest record on the shard.
	CheckpointTrimHorizon = "TRIM_HORIZON"
	// CheckpointShardEnd means the shard has been fully drained and any
	// child shards may now be claimed.
	CheckpointShardEnd = "SHARD_END"
)

// Lease is the state of a single shard's ownership.
//
// Counter is the fencing token: every successful conditional write to a row
// must observe the prior Counter and bump it. A stale writer with an outdated
// Counter is rejected, which is what makes ownership transitions safe in
// the face of network partitions.
type Lease struct {
	ShardID    string
	Owner      string
	Counter    int64
	Checkpoint string
	ParentIDs  []string
}

// IsOwnedBy reports whether the lease is currently held by the given worker.
func (l Lease) IsOwnedBy(owner string) bool {
	return l.Owner != "" && l.Owner == owner
}

// ErrLeaseConflict signals that a conditional write failed because the
// expected Counter or Owner did not match the row's current state. Callers
// must re-List to pick up the new state before retrying.
var ErrLeaseConflict = errors.New("lease conflict")

// ErrLeaseNotFound signals that the lease row does not exist.
var ErrLeaseNotFound = errors.New("lease not found")

// Store is the persistence contract for the lease coordinator.
//
// Implementations must be safe for concurrent use across goroutines and
// processes. All mutating methods use Counter as a fencing token; a stale
// caller must observe ErrLeaseConflict and re-List.
type Store interface {
	// List returns all known leases. Implementations may serve stale data
	// (eventually consistent); callers tolerate this by retrying through
	// conditional writes.
	List(ctx context.Context) ([]Lease, error)

	// Ensure creates a lease row for shardID with the given parents if no
	// row exists. Idempotent. The created row is unowned with checkpoint
	// CheckpointTrimHorizon. Used during shard discovery on Start and
	// whenever new shards appear (resharding).
	Ensure(ctx context.Context, shardID string, parentIDs []string) error

	// Acquire takes ownership of a lease. expectedCounter is the value the
	// caller observed via List; the write succeeds only if the row's Counter
	// still matches. Returns the new Lease with bumped Counter and the
	// caller as Owner. Returns ErrLeaseConflict on a counter mismatch.
	Acquire(ctx context.Context, shardID, owner string, expectedCounter int64) (Lease, error)

	// Heartbeat re-asserts ownership, bumping Counter. Conditional on the
	// row matching lease.Owner AND lease.Counter. Returns the updated Lease.
	Heartbeat(ctx context.Context, lease Lease) (Lease, error)

	// Checkpoint persists a new sequence number against an owned lease. Bumps
	// Counter. Conditional on owner+counter. Returns the updated Lease.
	Checkpoint(ctx context.Context, lease Lease, seq string) (Lease, error)

	// Release relinquishes ownership voluntarily (graceful shutdown).
	// Conditional on owner+counter. Owner is cleared; Counter is bumped.
	Release(ctx context.Context, lease Lease) error
}
