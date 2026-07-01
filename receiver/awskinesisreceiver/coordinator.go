package awskinesisreceiver

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"go.uber.org/zap"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
	"github.com/jrglee/opentelemetry-kinesis-stream/internal/lease"
)

// coordinator runs the shard-discovery and lease-acquisition loop. It does
// not hold any shard state itself — each owned lease has its own poller
// goroutine, and the poller owns the lease's Heartbeat/Checkpoint writes.
//
// Multi-replica safety relies on the lease store's Counter being a fencing
// token: a stale acquire-or-checkpoint from a dead-but-not-yet-known owner
// fails the conditional write, so two replicas never both make progress on
// the same shard.
type coordinator struct {
	cfg      *Config
	client   *kinesis.Client
	store    lease.Store
	comp     encoding.Compressor
	sink     sink
	logger   *zap.Logger
	tel      *receiverTelemetry
	workerID string

	// baseCtx is the lifetime of the pollers; it outlives the discovery loop so
	// that stopping discovery (graceful shutdown) does not hard-cancel pollers
	// mid-batch. stopDiscovery cancels only the discovery/reconcile loop.
	baseCtx       context.Context
	stopDiscovery context.CancelFunc

	mu       sync.Mutex
	active   map[string]*activePoller
	observed map[string]observation
	// stopped is set once drainAndStop begins, under mu, in the same critical
	// section that snapshots active. startPoller checks it before installing so a
	// poller acquired in the shutdown window cannot escape the drain snapshot.
	stopped bool
	// liveShards is the shard-id set from the most recent successful
	// ListShards. nil until the first discovery succeeds. Used to garbage-
	// collect leases for shards Kinesis has trimmed past retention.
	liveShards map[string]bool
	// absent counts consecutive discovery passes a shard has been missing from
	// liveShards, so a lease is only reaped after sustained absence.
	absent map[string]int

	wg sync.WaitGroup
}

// activePoller is the coordinator's handle on a running shardPoller goroutine.
// The pointer identity also acts as a generation token: the poller's exit
// defer only deletes the c.active entry when the current entry is still its
// own activePoller, so a later Acquire+startPoller cycle for the same shard
// is not clobbered by a delayed defer from a prior generation. cancel aborts
// the poller; drain asks it to stop gracefully after a final checkpoint.
type activePoller struct {
	cancel context.CancelFunc
	drain  func()
}

type observation struct {
	counter int64
	seenAt  time.Time
}

func (c *coordinator) start(ctx context.Context) error {
	c.baseCtx = ctx
	if err := c.discoverShards(ctx); err != nil {
		return err
	}
	discCtx, cancel := context.WithCancel(ctx)
	c.stopDiscovery = cancel
	c.wg.Add(1)
	go c.run(discCtx)
	return nil
}

// drainAndStop is the graceful-shutdown entry point. It stops the discovery
// loop so no new pollers start, then asks every active poller to drain
// (finish its in-flight batch, checkpoint, release). Pollers run on baseCtx,
// which is NOT cancelled here, so they get to finish cleanly. The caller waits
// on wait() and hard-cancels baseCtx only if a deadline forces it.
func (c *coordinator) drainAndStop() {
	if c.stopDiscovery != nil {
		c.stopDiscovery()
	}
	c.mu.Lock()
	c.stopped = true
	pollers := make([]*activePoller, 0, len(c.active))
	for _, ap := range c.active {
		pollers = append(pollers, ap)
	}
	c.mu.Unlock()
	for _, ap := range pollers {
		ap.drain()
	}
}

func (c *coordinator) run(ctx context.Context) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.cfg.DiscoveryInterval)
	defer ticker.Stop()
	// Reconcile once immediately so initial pollers don't wait the full interval.
	c.reconcile(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.discoverShards(ctx); err != nil {
				c.logger.Warn("shard discovery failed", zap.Error(err))
			}
			c.reconcile(ctx)
		}
	}
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

// reconcile is the fair-share rebalancing pass. It snapshots the lease table,
// computes this worker's target share via lease.Plan, and executes the
// resulting acquire / release / steal actions. Every replica runs the same
// computation against the same snapshot, so they converge without a leader.
func (c *coordinator) reconcile(ctx context.Context) {
	leases, err := c.store.List(ctx)
	if err != nil {
		c.logger.Warn("list leases failed", zap.Error(err))
		return
	}
	now := time.Now()
	checkpoints := indexCheckpoints(leases)

	c.mu.Lock()
	c.refreshObservations(leases, now)
	fresh := make(map[string]bool, len(leases))
	for _, l := range leases {
		obs, ok := c.observed[l.ShardID]
		fresh[l.ShardID] = l.Owner != "" && ok && now.Sub(obs.seenAt) < c.cfg.LeaseDuration
	}
	owned := make(map[string]bool, len(c.active))
	for shardID := range c.active {
		owned[shardID] = true
	}
	c.mu.Unlock()

	drained := make(map[string]bool, len(leases))
	byID := make(map[string]lease.Lease, len(leases))
	for _, l := range leases {
		drained[l.ShardID] = parentsDrained(l, checkpoints)
		byID[l.ShardID] = l
	}

	// lease.Plan is the KCL LeaseTaker step: pure fair-share decision over the
	// snapshot, identical on every replica so they converge leaderlessly.
	plan := lease.Plan(lease.PlanInput{
		Leases:  leases,
		Self:    c.workerID,
		Fresh:   fresh,
		Drained: drained,
		Owned:   owned,
	})

	for _, shardID := range plan.Release {
		c.releasePoller(shardID)
	}
	for _, shardID := range plan.Acquire {
		c.tryAcquire(ctx, byID[shardID])
	}
	if plan.Steal != "" {
		c.logger.Info("stealing shard to rebalance", zap.String("shard", plan.Steal))
		c.tel.recordLeaseEvent(ctx, leaseSteal, resultSuccess)
		c.tryAcquire(ctx, byID[plan.Steal])
	}

	c.logger.Debug(
		"reconcile pass",
		zap.Int("owned", len(owned)),
		zap.Int("acquire", len(plan.Acquire)),
		zap.Int("release", len(plan.Release)),
		zap.Bool("steal", plan.Steal != ""),
	)

	c.cleanupOrphans(ctx, leases)
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

// tryAcquire claims a lease for this worker, conditional on the counter we
// observed, and starts a poller on success. A conflict means another worker
// won the race or the owner heartbeated since the snapshot; it is retried next
// pass and not logged as an error.
func (c *coordinator) tryAcquire(ctx context.Context, l lease.Lease) {
	c.mu.Lock()
	_, own := c.active[l.ShardID]
	c.mu.Unlock()
	if own {
		return
	}
	taken, err := c.store.Acquire(ctx, l.ShardID, c.workerID, l.Counter)
	if err != nil {
		if errors.Is(err, lease.ErrLeaseConflict) {
			c.tel.recordLeaseEvent(ctx, leaseAcquire, resultConflict)
		} else {
			c.logger.Warn("acquire failed", zap.String("shard", l.ShardID), zap.Error(err))
		}
		return
	}
	c.tel.recordLeaseEvent(ctx, leaseAcquire, resultSuccess)
	c.logger.Debug("lease acquired", zap.String("shard", taken.ShardID), zap.Int64("counter", taken.Counter))
	c.startPoller(ctx, taken)
}

// releasePoller asks the poller for a shard this worker is giving up to drain
// gracefully: finish its in-flight batch, persist a final checkpoint, then
// Release the lease. Draining (not cancelling) means the peer that acquires
// the freed lease resumes from a current checkpoint with no re-delivered
// records — the handoff is effectively exactly-once.
func (c *coordinator) releasePoller(shardID string) {
	c.mu.Lock()
	ap, ok := c.active[shardID]
	c.mu.Unlock()
	if !ok {
		return
	}
	c.logger.Info("releasing shard to rebalance", zap.String("shard", shardID))
	c.tel.recordLeaseEvent(c.baseCtx, leaseRelease, resultSuccess)
	ap.drain()
}

// refreshObservations resets the "last seen" timestamp on counters that
// changed since we last looked. Caller holds c.mu.
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
}

func (c *coordinator) startPoller(_ context.Context, l lease.Lease) {
	// Pollers run on baseCtx, not the discovery context, so a graceful shutdown
	// can stop discovery and let pollers drain rather than aborting them.
	pollerCtx, cancel := context.WithCancel(c.baseCtx)
	p := &shardPoller{
		cfg:     c.cfg,
		client:  c.client,
		store:   c.store,
		comp:    c.comp,
		sink:    c.sink,
		logger:  c.logger,
		tel:     c.tel,
		leased:  l,
		drainCh: make(chan struct{}),
	}
	ap := &activePoller{cancel: cancel, drain: p.drain}

	c.mu.Lock()
	if c.stopped {
		// Shutdown began (drainAndStop set stopped under this same lock) between
		// our Acquire and this install, so drainAndStop's snapshot has already
		// missed us. Do not start a poller that would never be drained: cancel the
		// context and abandon the just-taken lease. It expires after lease_duration
		// and a surviving replica (or this worker's next start) reclaims it from
		// the last checkpoint — at-least-once, no stuck shard.
		c.mu.Unlock()
		cancel()
		return
	}
	c.active[l.ShardID] = ap
	c.mu.Unlock()
	c.tel.addOwnedShards(c.baseCtx, 1)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer func() {
			c.mu.Lock()
			// Generation check: only clear the slot if it still holds *this*
			// poller. A later Acquire+startPoller cycle for the same shard
			// installs its own activePoller; we must not delete that one.
			cleared := c.active[l.ShardID] == ap
			if cleared {
				delete(c.active, l.ShardID)
			}
			c.mu.Unlock()
			if cleared {
				c.tel.addOwnedShards(c.baseCtx, -1)
			}
		}()
		p.run(pollerCtx)
	}()
}

func (c *coordinator) wait() { c.wg.Wait() }

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
