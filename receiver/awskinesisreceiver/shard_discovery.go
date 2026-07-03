// shard_discovery.go: shard enumeration, lease seeding, and orphan reaping.
//
// discoverShards/listShards are the eventually-consistent, defensive layer of
// the coordinator: ListShards may transiently omit live shards and has its own
// 5 TPS/shard limit, so orphan cleanup applies layered guards — SHARD_END
// only, sustained absence across multiple passes, quiescent owner — rather
// than acting on a single observation. reconcile (the fair-share decision loop
// that consumes discovery output) lives in coordinator.go.

package awskinesisreceiver

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"go.uber.org/zap"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/lease"
)

// observation tracks the last-seen Counter for a lease so the coordinator can
// distinguish a fresh heartbeat (counter changed) from a stale one (counter
// unchanged). Caller must hold c.mu when reading or writing c.observed.
type observation struct {
	counter int64
	seenAt  time.Time
}

// discoverShards enumerates the stream's shards, ensures a lease row exists for
// each, and records the live shard-id set (for orphan cleanup). Idempotent.
func (c *coordinator) discoverShards(ctx context.Context) error {
	shards, err := c.listShards(ctx)
	if err != nil {
		return err
	}
	live := make(map[string]bool, len(shards))
	for _, s := range shards {
		var parents []string
		if pid := aws.ToString(s.ParentShardId); pid != "" {
			parents = append(parents, pid)
		}
		if pid := aws.ToString(s.AdjacentParentShardId); pid != "" {
			parents = append(parents, pid)
		}
		shardID := aws.ToString(s.ShardId)
		live[shardID] = true
		if err := c.store.Ensure(ctx, shardID, parents); err != nil {
			return err
		}
	}
	c.mu.Lock()
	c.liveShards = live
	c.mu.Unlock()
	return nil
}

// listShards paginates over ListShards. The AWS SDK does not ship a typed
// paginator for this API, so we encode the StreamName-only-on-first-page
// rule by hand. A non-nil token-with-empty-string is treated as no more
// pages — defensive against any AWS quirk.
func (c *coordinator) listShards(ctx context.Context) ([]types.Shard, error) {
	var (
		shards []types.Shard
		token  *string
	)
	for {
		input := &kinesis.ListShardsInput{}
		if token == nil {
			input.StreamName = aws.String(c.cfg.StreamName)
		} else {
			input.NextToken = token
		}
		out, err := c.client.ListShards(ctx, input)
		if err != nil {
			return nil, err
		}
		shards = append(shards, out.Shards...)
		if out.NextToken == nil || aws.ToString(out.NextToken) == "" {
			return shards, nil
		}
		token = out.NextToken
	}
}

// orphanReapThreshold is how many consecutive discovery passes a shard must be
// absent from ListShards before its lease is reaped. ListShards is eventually
// consistent and paginated, so a single pass that omits a still-listed shard is
// not trustworthy; requiring several in a row tolerates that blip.
const orphanReapThreshold = 3

// cleanupOrphans garbage-collects leases for shards Kinesis has trimmed past
// retention, so the lease table does not accumulate dead SHARD_END rows over a
// stream's resharding history (the KCL LeaseCleanupManager role).
//
// Safety is layered, because ListShards absence alone is NOT a safe delete
// signal (a transient eventually-consistent omission of a live shard would
// otherwise destroy an active checkpoint and force a full re-read):
//   - Only a lease at SHARD_END is reapable. A lease with a real sequence-number
//     checkpoint is an ACTIVE shard; if it is momentarily missing from
//     ListShards that is a consistency blip, never a trim, so it is never
//     deleted.
//   - The shard must be absent for orphanReapThreshold consecutive successful
//     discovery passes (tracked in c.absent), not just one.
//   - Discovery must have succeeded (liveShards non-empty), the shard must not
//     be actively polled here, and the Delete is fenced on the observed counter.
//
// Note a closed (SHARD_END) shard stays listed until Kinesis trims it; requiring
// absence is what guarantees a Delete will not be undone by the next discovery
// re-Ensuring the shard at TRIM_HORIZON.
func (c *coordinator) cleanupOrphans(ctx context.Context, leases []lease.Lease) {
	c.mu.Lock()
	live := c.liveShards
	c.mu.Unlock()
	if len(live) == 0 {
		return
	}
	for _, l := range leases {
		if live[l.ShardID] {
			c.mu.Lock()
			delete(c.absent, l.ShardID)
			c.mu.Unlock()
			continue
		}
		// Only completed shards are reapable. An absent non-SHARD_END lease is a
		// consistency blip, not a trimmed shard — leave its checkpoint intact.
		if l.Checkpoint != lease.CheckpointShardEnd {
			continue
		}
		c.mu.Lock()
		c.absent[l.ShardID]++
		absentFor := c.absent[l.ShardID]
		_, active := c.active[l.ShardID]
		obs, seen := c.observed[l.ShardID]
		c.mu.Unlock()
		if active || absentFor < orphanReapThreshold {
			continue
		}
		// Skip a lease still being heartbeated by some owner: only reap shards
		// that are both trimmed and quiescent.
		if l.Owner != "" && seen && obs.counter == l.Counter && time.Since(obs.seenAt) < c.cfg.LeaseDuration {
			continue
		}
		if err := c.store.Delete(ctx, l.ShardID, l.Counter); err != nil {
			if !errors.Is(err, lease.ErrLeaseConflict) {
				c.logger.Warn("lease cleanup failed", zap.String("shard", l.ShardID), zap.Error(err))
			}
			continue
		}
		c.logger.Info("garbage-collected lease for trimmed shard", zap.String("shard", l.ShardID))
		c.mu.Lock()
		delete(c.observed, l.ShardID)
		delete(c.absent, l.ShardID)
		c.mu.Unlock()
	}
}

// refreshObservations resets the "last seen" timestamp on counters that
// changed since we last looked, and prunes entries for leases that no longer
// exist. Caller holds c.mu.
func (c *coordinator) refreshObservations(leases []lease.Lease, now time.Time) {
	for _, l := range leases {
		obs, ok := c.observed[l.ShardID]
		if !ok || obs.counter != l.Counter {
			c.observed[l.ShardID] = observation{counter: l.Counter, seenAt: now}
		}
	}
	for shardID := range c.observed {
		if !slices.ContainsFunc(leases, func(l lease.Lease) bool { return l.ShardID == shardID }) {
			delete(c.observed, shardID)
		}
	}
	// Prune absent alongside observed: a lease deleted by a peer would otherwise
	// leave its absence counter behind forever (cleanupOrphans only clears
	// entries it reaps itself).
	for shardID := range c.absent {
		if !slices.ContainsFunc(leases, func(l lease.Lease) bool { return l.ShardID == shardID }) {
			delete(c.absent, shardID)
		}
	}
}

// indexCheckpoints flattens lease state for parent-drain lookup.
func indexCheckpoints(leases []lease.Lease) map[string]string {
	out := make(map[string]string, len(leases))
	for _, l := range leases {
		out[l.ShardID] = l.Checkpoint
	}
	return out
}

// parentsDrained returns true if every parent shard's lease checkpoint is
// SHARD_END. A shard with unknown parents (e.g. the original shards) passes.
func parentsDrained(l lease.Lease, checkpoints map[string]string) bool {
	for _, pid := range l.ParentIDs {
		cp, ok := checkpoints[pid]
		if !ok {
			// Parent absent from the lease table: treat as drained. This
			// covers shards trimmed past retention so their leases were
			// garbage-collected.
			continue
		}
		if cp != lease.CheckpointShardEnd {
			return false
		}
	}
	return true
}
