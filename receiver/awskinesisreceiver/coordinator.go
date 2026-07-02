// coordinator.go: shard-lease coordinator lifecycle and fair-share reconcile loop.
//
// The coordinator owns Acquire for each shard; pollers own Heartbeat and
// Checkpoint. This split keeps the lease Counter monotonic: only one goroutine
// per shard advances it (the active poller), while the coordinator only
// acquires or steals — never writes to an owned lease mid-poll.
//
// Multi-replica safety relies on the lease store's Counter as a fencing token:
// a stale Acquire or Checkpoint from a dead-but-not-yet-known owner fails the
// conditional write, so two replicas never both make progress on the same shard.
//
// Shard enumeration, lease seeding, and orphan reaping live in
// shard_discovery.go — the eventually-consistent, defensive layer that
// feeds this reconcile loop.
package awskinesisreceiver

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"go.uber.org/zap"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
	"github.com/jrglee/opentelemetry-kinesis-stream/internal/lease"
)

// coordinator runs the shard-discovery and lease-acquisition loop. It does
// not hold any shard state itself — each owned lease has its own poller
// goroutine, and the poller owns the lease's Heartbeat/Checkpoint writes.
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
	// killCtx is handed to each poller to bound exit-path store writes; the
	// receiver cancels it when the shutdown deadline fires. Nil in tests that
	// construct the coordinator directly (pollers fall back to background).
	killCtx context.Context

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
// The pointer identity acts as a generation token: the poller's exit defer
// only deletes the c.active entry when the current entry is still its own
// activePoller, so a later Acquire+startPoller cycle for the same shard is
// not clobbered by a delayed defer from a prior generation.
type activePoller struct {
	cancel context.CancelFunc
	drain  func()
}

func (c *coordinator) start(ctx context.Context) error {
	discCtx, cancel := context.WithCancel(ctx)
	// baseCtx and stopDiscovery are published under mu because drainAndStop can
	// run from another goroutine; the component contract serializes Start and
	// Shutdown, but that contract is enforced here, not assumed.
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		cancel()
		return nil
	}
	c.baseCtx = ctx
	c.stopDiscovery = cancel
	c.mu.Unlock()
	if err := c.discoverShards(ctx); err != nil {
		cancel()
		return err
	}
	// Re-check under mu: a drainAndStop that raced in while discoverShards was
	// on the network must not see a run goroutine appear after its snapshot,
	// and wg.Add must never race a wait() at counter zero.
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		cancel()
		return nil
	}
	c.wg.Add(1)
	c.mu.Unlock()
	go c.run(discCtx)
	return nil
}

// drainAndStop is the graceful-shutdown entry point. It stops the discovery
// loop so no new pollers start, then asks every active poller to drain
// (finish its in-flight batch, checkpoint, release). Pollers run on baseCtx,
// which is NOT cancelled here, so they get to finish cleanly. The caller waits
// on wait() and hard-cancels baseCtx only if a deadline forces it.
func (c *coordinator) drainAndStop() {
	c.mu.Lock()
	c.stopped = true
	stop := c.stopDiscovery
	pollers := make([]*activePoller, 0, len(c.active))
	for _, ap := range c.active {
		pollers = append(pollers, ap)
	}
	c.mu.Unlock()
	if stop != nil {
		stop()
	}
	for _, ap := range pollers {
		ap.drain()
	}
}

func (c *coordinator) run(ctx context.Context) {
	defer c.wg.Done()
	if ctx.Err() != nil {
		// Shutdown won the start/stop race; do no work on a dead context.
		return
	}
	ticker := time.NewTicker(c.cfg.DiscoveryInterval)
	defer ticker.Stop()
	// Reconcile once immediately so initial pollers don't wait the full
	// interval. start's discoverShards just succeeded, so orphan cleanup may
	// trust the live-shard snapshot.
	c.reconcile(ctx, true)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			discoveryOK := true
			if err := c.discoverShards(ctx); err != nil {
				discoveryOK = false
				c.logger.Warn("shard discovery failed", zap.Error(err))
			}
			c.reconcile(ctx, discoveryOK)
		}
	}
}

// reconcile is the fair-share rebalancing pass. It snapshots the lease table,
// computes this worker's target share via lease.Plan, and executes the
// resulting acquire / release / steal actions. Every replica runs the same
// computation against the same snapshot, so they converge without a leader.
// discoveryOK reports whether the current pass's shard discovery succeeded;
// orphan cleanup is skipped otherwise so a discovery outage cannot advance
// absence counters against a stale live-shard snapshot.
func (c *coordinator) reconcile(ctx context.Context, discoveryOK bool) {
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
		c.tryAcquire(ctx, byID[shardID], leaseAcquire)
	}
	if plan.Steal != "" {
		c.logger.Info("stealing shard to rebalance", zap.String("shard", plan.Steal))
		c.tryAcquire(ctx, byID[plan.Steal], leaseSteal)
	}

	c.logger.Debug(
		"reconcile pass",
		zap.Int("owned", len(owned)),
		zap.Int("acquire", len(plan.Acquire)),
		zap.Int("release", len(plan.Release)),
		zap.Bool("steal", plan.Steal != ""),
	)

	if discoveryOK {
		c.cleanupOrphans(ctx, leases)
	}
}

// tryAcquire claims a lease for this worker, conditional on the counter we
// observed, and starts a poller on success. A conflict means another worker
// won the race or the owner heartbeated since the snapshot; it is retried next
// pass and not logged as an error. event names the telemetry label (acquire or
// steal); the outcome is recorded here, after the store call, so a lost race
// is never counted as a success.
func (c *coordinator) tryAcquire(ctx context.Context, l lease.Lease, event string) {
	c.mu.Lock()
	_, own := c.active[l.ShardID]
	c.mu.Unlock()
	if own {
		return
	}
	taken, err := c.store.Acquire(ctx, l.ShardID, c.workerID, l.Counter)
	if err != nil {
		if errors.Is(err, lease.ErrLeaseConflict) {
			c.tel.recordLeaseEvent(ctx, event, resultConflict)
		} else {
			c.logger.Warn("acquire failed", zap.String("shard", l.ShardID), zap.Error(err))
		}
		return
	}
	c.tel.recordLeaseEvent(ctx, event, resultSuccess)
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
		killCtx: c.killCtx,
		leased:  l,
		drainCh: make(chan struct{}),
	}
	ap := &activePoller{cancel: cancel, drain: p.drain}

	c.mu.Lock()
	if c.stopped {
		// Shutdown began (drainAndStop set stopped under this same lock) between
		// our Acquire and this install, so drainAndStop's snapshot has already
		// missed us. Do not start a poller that would never be drained: cancel
		// the context and free the just-taken lease with a best-effort fenced
		// Release so a peer need not wait out lease_duration. A conflict means
		// it was already re-taken — the postcondition Release wanted.
		c.mu.Unlock()
		cancel()
		relCtx, relCancel := context.WithTimeout(c.exitCtx(), releaseTimeout)
		defer relCancel()
		if err := c.store.Release(relCtx, l); err != nil && !errors.Is(err, lease.ErrLeaseConflict) {
			c.logger.Warn("release of shutdown-raced lease failed; peers reclaim after lease_duration",
				zap.String("shard", l.ShardID), zap.Error(err))
		}
		return
	}
	c.active[l.ShardID] = ap
	c.mu.Unlock()
	c.tel.addOwnedShards(c.baseCtx, 1)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		// Release the pollerCtx once the poller exits; otherwise every
		// finished poller leaves a live cancelCtx child registered on baseCtx
		// for the life of the receiver.
		defer cancel()
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

// exitCtx parents best-effort exit-path store writes: killCtx when the
// receiver wired one, background otherwise (tests).
func (c *coordinator) exitCtx() context.Context {
	if c.killCtx != nil {
		return c.killCtx
	}
	return context.Background()
}
