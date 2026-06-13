package awskinesisreceiver

import (
	"context"
	"errors"
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

type activePoller struct {
	cancel context.CancelFunc
	done   chan struct{}
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

func (c *coordinator) listShards(ctx context.Context) ([]types.Shard, error) {
	var (
		shards []types.Shard
		token  *string
	)
	for {
		input := &kinesis.ListShardsInput{NextToken: token}
		if token == nil {
			input.StreamName = aws.String(c.cfg.StreamName)
		}
		out, err := c.client.ListShards(ctx, input)
		if err != nil {
			return nil, err
		}
		shards = append(shards, out.Shards...)
		if out.NextToken == nil {
			return shards, nil
		}
		token = out.NextToken
	}
}

// reconcile is the lease-acquisition pass. It snapshots known leases,
// classifies each, and attempts to take ownership of stealable ones whose
// parents are drained.
func (c *coordinator) reconcile(ctx context.Context) {
	leases, err := c.store.List(ctx)
	if err != nil {
		c.logger.Warn("list leases failed", zap.Error(err))
		return
	}
	now := time.Now()
	parentCheckpoints := indexCheckpoints(leases)

	c.mu.Lock()
	c.refreshObservations(leases, now)
	// Prune pollers whose context already exited.
	for shardID, ap := range c.active {
		select {
		case <-ap.done:
			delete(c.active, shardID)
		default:
		}
	}
	c.mu.Unlock()

	for _, l := range leases {
		if l.Checkpoint == lease.CheckpointShardEnd {
			continue
		}
		if !parentsDrained(l, parentCheckpoints) {
			continue
		}
		c.mu.Lock()
		_, alreadyOwn := c.active[l.ShardID]
		c.mu.Unlock()
		if alreadyOwn {
			continue
		}
		if !c.shouldAttempt(l, now) {
			continue
		}
		taken, err := c.store.Acquire(ctx, l.ShardID, c.workerID, l.Counter)
		if err != nil {
			if !errors.Is(err, lease.ErrLeaseConflict) {
				c.logger.Warn(
					"acquire failed",
					zap.String("shard", l.ShardID),
					zap.Error(err),
				)
			}
			continue
		}
		c.startPoller(ctx, taken)
	}
}

// shouldAttempt decides whether to try Acquire on a given lease. Unowned
// leases are always fair game. Owned leases are stealable only if they
// haven't been heartbeated for at least LeaseDuration.
func (c *coordinator) shouldAttempt(l lease.Lease, now time.Time) bool {
	if l.Owner == "" || l.Owner == c.workerID {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	obs, ok := c.observed[l.ShardID]
	if !ok {
		return false
	}
	return obs.counter == l.Counter && now.Sub(obs.seenAt) >= c.cfg.LeaseDuration
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
		if !leaseExists(leases, shardID) {
			delete(c.observed, shardID)
		}
	}
}

func (c *coordinator) startPoller(ctx context.Context, l lease.Lease) {
	pollerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	ap := &activePoller{cancel: cancel, done: done}

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
		defer close(done)
		defer func() {
			c.mu.Lock()
			delete(c.active, l.ShardID)
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

func leaseExists(leases []lease.Lease, shardID string) bool {
	for _, l := range leases {
		if l.ShardID == shardID {
			return true
		}
	}
	return false
}
