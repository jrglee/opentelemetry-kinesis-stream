// poller_lease.go: lease write plumbing for the shard poller.
//
// Heartbeat, Checkpoint, and Release are the fencing surface: each store
// write succeeds only when the lease Counter matches the replica's view,
// ensuring exactly one active writer per shard across the fleet without
// cross-replica locking. leaseMu serializes the two in-process writers —
// the poll loop's Checkpoint calls and the heartbeat goroutine's Heartbeat
// calls — so the Counter advances monotonically within a single replica.
//
// On the exit path (release, writeShardEnd), killCtx bounds write attempts
// so a hung store call cannot hold the collector's graceful-shutdown deadline
// beyond releaseTimeout. Once killCtx is cancelled the write aborts
// immediately; without it, the bounded context still enforces the deadline.
package awskinesisreceiver

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/lease"
)

func (p *shardPoller) heartbeat(ctx context.Context) error {
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	updated, err := p.store.Heartbeat(ctx, p.leased)
	if err != nil {
		return err
	}
	p.leased = updated
	return nil
}

func (p *shardPoller) checkpoint(ctx context.Context, seq string) error {
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	updated, err := p.store.Checkpoint(ctx, p.leased, seq)
	if err != nil {
		return err
	}
	p.leased = updated
	return nil
}

// Bounds for the in-place checkpoint retry on transient store errors. Kept
// short: a checkpoint that cannot land within a few hundred milliseconds is
// better surfaced to the caller than silently stretched toward lease expiry.
const (
	checkpointAttempts     = 3
	checkpointRetryBackoff = 100 * time.Millisecond
)

// checkpointWithRetry retries transient store failures in place so a DynamoDB
// throttle or network blip does not tear down the poller (abandoning the
// in-flight batch and forcing a fleet-wide reacquire storm when the blip is
// shared). Lease conflicts and not-found are real lease loss and surface
// immediately; context cancellation aborts the retry.
func (p *shardPoller) checkpointWithRetry(ctx context.Context, seq string) error {
	var err error
	for attempt := 0; attempt < checkpointAttempts; attempt++ {
		if attempt > 0 {
			t := time.NewTimer(checkpointRetryBackoff)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
			}
			p.logger.Warn("checkpoint attempt failed; retrying",
				zap.String("shard", p.shardID()), zap.Int("attempt", attempt), zap.Error(err))
		}
		err = p.checkpoint(ctx, seq)
		if err == nil ||
			errors.Is(err, lease.ErrLeaseConflict) ||
			errors.Is(err, lease.ErrLeaseNotFound) ||
			errors.Is(err, context.Canceled) {
			return err
		}
	}
	return err
}

// exitCtx is the parent for best-effort exit-path store writes: killCtx when
// the coordinator wired one (cancelled on hard shutdown), background otherwise.
func (p *shardPoller) exitCtx() context.Context {
	if p.killCtx != nil {
		return p.killCtx
	}
	return context.Background()
}

// release is the deferred exit path. It runs under a bounded exit context so a
// hung DynamoDB Release never blocks the collector's graceful-shutdown
// deadline — and aborts immediately once the deadline has already hard-
// cancelled killCtx. Lease-conflict errors are silently dropped: they mean the
// lease was already stolen, which is the postcondition Release would have
// achieved anyway.
func (p *shardPoller) release() {
	ctx, cancel := context.WithTimeout(p.exitCtx(), releaseTimeout)
	defer cancel()
	p.leaseMu.Lock()
	leased := p.leased
	p.leaseMu.Unlock()
	if err := p.store.Release(ctx, leased); err != nil && !errors.Is(err, lease.ErrLeaseConflict) {
		p.logger.Warn(
			"release failed",
			zap.String("shard", leased.ShardID),
			zap.Error(err),
		)
	}
}

func (p *shardPoller) shardID() string {
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	return p.leased.ShardID
}

func (p *shardPoller) leaseCounter() int64 {
	p.leaseMu.Lock()
	defer p.leaseMu.Unlock()
	return p.leased.Counter
}
