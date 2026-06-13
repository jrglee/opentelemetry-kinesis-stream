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
	"go.opentelemetry.io/collector/consumer"
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
	decoder  encoding.TracesDecoder
	comp     encoding.Compressor
	consumer consumer.Traces
	logger   *zap.Logger
	workerID string

	mu       sync.Mutex
	active   map[string]*activePoller
	observed map[string]observation

	wg sync.WaitGroup
}

// activePoller is the coordinator's handle on a running shardPoller goroutine.
// The pointer identity also acts as a generation token: the poller's exit
// defer only deletes the c.active entry when the current entry is still its
// own activePoller, so a later Acquire+startPoller cycle for the same shard
// is not clobbered by a delayed defer from a prior generation.
type activePoller struct {
	cancel context.CancelFunc
}

type observation struct {
	counter int64
	seenAt  time.Time
}

func (c *coordinator) start(ctx context.Context) error {
	if err := c.discoverShards(ctx); err != nil {
		return err
	}
	c.wg.Add(1)
	go c.run(ctx)
	return nil
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

// discoverShards enumerates the stream's shards and ensures a lease row
// exists for each. Idempotent.
func (c *coordinator) discoverShards(ctx context.Context) error {
	shards, err := c.listShards(ctx)
	if err != nil {
		return err
	}
	for _, s := range shards {
		var parents []string
		if pid := aws.ToString(s.ParentShardId); pid != "" {
			parents = append(parents, pid)
		}
		if pid := aws.ToString(s.AdjacentParentShardId); pid != "" {
			parents = append(parents, pid)
		}
		if err := c.store.Ensure(ctx, aws.ToString(s.ShardId), parents); err != nil {
			return err
		}
	}
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
// computes this worker's target share via planRebalance, and executes the
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

	plan := planRebalance(rebalanceInput{
		leases:  leases,
		self:    c.workerID,
		fresh:   fresh,
		drained: drained,
		owned:   owned,
	})

	for _, shardID := range plan.release {
		c.releasePoller(shardID)
	}
	for _, shardID := range plan.acquire {
		c.tryAcquire(ctx, byID[shardID])
	}
	if plan.steal != "" {
		c.logger.Info("stealing shard to rebalance", zap.String("shard", plan.steal))
		c.tryAcquire(ctx, byID[plan.steal])
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
		if !errors.Is(err, lease.ErrLeaseConflict) {
			c.logger.Warn("acquire failed", zap.String("shard", l.ShardID), zap.Error(err))
		}
		return
	}
	c.startPoller(ctx, taken)
}

// releasePoller cancels the poller for a shard this worker is giving up. The
// poller's deferred Release frees the lease row so an under-target peer can
// take it.
func (c *coordinator) releasePoller(shardID string) {
	c.mu.Lock()
	ap, ok := c.active[shardID]
	c.mu.Unlock()
	if !ok {
		return
	}
	c.logger.Info("releasing shard to rebalance", zap.String("shard", shardID))
	ap.cancel()
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

func (c *coordinator) startPoller(ctx context.Context, l lease.Lease) {
	pollerCtx, cancel := context.WithCancel(ctx)
	ap := &activePoller{cancel: cancel}

	c.mu.Lock()
	c.active[l.ShardID] = ap
	c.mu.Unlock()

	p := &shardPoller{
		cfg:      c.cfg,
		client:   c.client,
		store:    c.store,
		decoder:  c.decoder,
		comp:     c.comp,
		consumer: c.consumer,
		logger:   c.logger,
		leased:   l,
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer func() {
			c.mu.Lock()
			// Generation check: only clear the slot if it still holds *this*
			// poller. A later Acquire+startPoller cycle for the same shard
			// installs its own activePoller; we must not delete that one.
			if c.active[l.ShardID] == ap {
				delete(c.active, l.ShardID)
			}
			c.mu.Unlock()
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
