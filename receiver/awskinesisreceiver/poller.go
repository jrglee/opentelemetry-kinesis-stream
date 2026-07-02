// poller.go: poll loop and lifecycle for a single Kinesis shard.
//
// Contains the timing machinery: GetRecords pacing (the Kinesis API allows
// five reads/second/shard), iterator open/reopen, stuck-shard exponential
// backoff, and the graceful drain gate. Record handling is in
// poller_records.go; lease writes (the fencing surface, serialized by
// leaseMu) are in poller_lease.go.

package awskinesisreceiver

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"go.uber.org/zap"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
	"github.com/jrglee/opentelemetry-kinesis-stream/internal/lease"
)

// releaseTimeout bounds the best-effort Release attempt on exit so a hung
// store call cannot block the collector's graceful shutdown deadline.
const releaseTimeout = 5 * time.Second

// When the downstream consumer keeps rejecting the head record (recordRetry),
// the poll loop re-reads the same checkpoint without advancing. Rather than
// hammer GetRecords/GetShardIterator at PollInterval (which risks the 5 TPS/
// shard limit and offers no relief), consecutive no-progress passes back off
// exponentially from PollInterval up to stuckBackoffMax, and a warning is
// surfaced after stuckWarnAfter passes so a wedged shard is observable. The
// lease is intentionally still held and the records are never dropped — at-
// least-once is preserved; this only changes the cadence and visibility.
const (
	stuckBackoffMax = 30 * time.Second
	stuckWarnAfter  = 5
)

// shardPoller runs the GetRecords loop for one shard while holding its lease.
// It owns Heartbeat and Checkpoint writes for the lease; the coordinator
// owns Acquire. The split of write responsibility keeps the lease's Counter
// monotonic without cross-replica locking, and the leaseMu serializes the
// poll-loop's Checkpoint with the heartbeat goroutine's Heartbeat so a
// single in-process writer never races itself either.
type shardPoller struct {
	cfg    *Config
	client *kinesis.Client
	store  lease.Store
	comp   encoding.Compressor
	sink   sink
	logger *zap.Logger
	tel    *receiverTelemetry

	// killCtx bounds the best-effort store writes on the exit path (release,
	// SHARD_END sentinel). It is NOT the poll context: a graceful drain leaves
	// it alive so final writes complete, while the receiver cancels it when the
	// collector's shutdown deadline fires so a hung store call cannot hold
	// shutdown for releaseTimeout per poller. Nil means background (tests).
	killCtx context.Context

	leaseMu sync.Mutex
	leased  lease.Lease

	// drainCh is closed to request a graceful stop: finish the in-flight
	// batch, persist its checkpoint, then release. Distinct from context
	// cancellation, which aborts mid-batch. Graceful drain is what makes a
	// planned handoff (rebalance or shutdown) avoid re-delivering an
	// uncheckpointed batch to the next owner.
	drainCh   chan struct{}
	drainOnce sync.Once
}

// drain requests a graceful stop. Idempotent.
func (p *shardPoller) drain() {
	p.drainOnce.Do(func() { close(p.drainCh) })
}

// run is the poller's main entry point. The heartbeat lives on its own
// goroutine so a slow GetRecords or a long PollInterval cannot starve it
// and silently lose the lease. Either goroutine signalling "lost lease"
// cancels the local pollCtx so the other exits promptly.
func (p *shardPoller) run(parentCtx context.Context) {
	logger := p.logger.With(zap.String("shard", p.shardID()))
	defer p.release()

	pollCtx, stop := context.WithCancel(parentCtx)
	defer stop()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.runHeartbeat(pollCtx, stop, logger)
	}()
	p.runPoll(pollCtx, stop, logger)
	// runPoll may return without cancelling (e.g. SHARD_END); cancel here so
	// the heartbeat goroutine always exits and wg.Wait does not hang.
	stop()
	wg.Wait()
}

func (p *shardPoller) runHeartbeat(ctx context.Context, stop context.CancelFunc, logger *zap.Logger) {
	ticker := time.NewTicker(p.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := p.heartbeat(ctx); err != nil {
			switch {
			case errors.Is(err, context.Canceled):
				stop()
				return
			case errors.Is(err, lease.ErrLeaseConflict), errors.Is(err, lease.ErrLeaseNotFound):
				logger.Warn("heartbeat lost lease; stopping poller", zap.Error(err))
				p.tel.recordLeaseEvent(ctx, leaseHeartbeatGot, resultConflict)
				stop()
				return
			default:
				// A store blip (throttle, 5xx, network) is not lease loss:
				// keep polling and retry on the next tick. If the outage
				// outlasts lease_duration a peer steals the lease and the
				// next heartbeat surfaces the conflict above.
				logger.Warn("heartbeat attempt failed; retrying next tick", zap.Error(err))
				continue
			}
		}
		logger.Debug("heartbeat ok", zap.Int64("counter", p.leaseCounter()))
	}
}

func (p *shardPoller) runPoll(ctx context.Context, stop context.CancelFunc, logger *zap.Logger) {
	iter, err := p.openIterator(ctx)
	if err != nil {
		logger.Error("open iterator failed", zap.Error(err))
		stop()
		return
	}
	stuckPasses := 0
	// lastPoll paces GetRecords to PollInterval per call — the API allows five
	// reads per second per shard, and an unpaced loop on a busy shard would
	// hammer straight into ProvisionedThroughputExceededException. The zero
	// value makes the first poll immediate.
	var lastPoll time.Time

	for {
		if ctx.Err() != nil {
			return
		}
		// A drained (closed) shard earned its SHARD_END sentinel even if a
		// graceful drain arrives in the same instant — write it before honoring
		// the drain so the next owner need not re-discover the closure.
		if iter == nil {
			p.writeShardEnd(ctx, logger)
			return
		}
		// Graceful drain: the last completed batch is already checkpointed, so
		// exiting here (before the next GetRecords) hands off cleanly.
		select {
		case <-p.drainCh:
			logger.Info("draining shard; releasing after final checkpoint")
			return
		default:
		}
		if wait := p.cfg.PollInterval - time.Since(lastPoll); wait > 0 {
			if p.waitDurationOrStop(ctx, wait) {
				return
			}
		}
		lastPoll = time.Now()
		pollStart := time.Now()
		out, err := p.client.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: iter,
			Limit:         aws.Int32(p.cfg.MaxRecords),
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Re-open the iterator ONLY for an expired one (iterators live ~5
			// minutes); reusing it would spin forever. For throttling and other
			// transient errors, keep the same iterator and back off — calling
			// GetShardIterator has its own 5 TPS/shard limit, so re-opening under
			// throttle would add load to an already-throttled shard.
			var expired *types.ExpiredIteratorException
			if errors.As(err, &expired) {
				logger.Warn("shard iterator expired; re-opening from checkpoint", zap.Error(err))
				if iter, err = p.openIterator(ctx); err != nil {
					logger.Error("re-open iterator failed; stopping poller", zap.Error(err))
					stop()
					return
				}
				// Fresh iterator after a ≥5-minute expiry stall: the shard is
				// nowhere near its read quota, so skip the pacing wait and poll
				// again immediately instead of adding an interval to the stall.
				lastPoll = time.Time{}
				continue
			}
			logger.Warn("get_records failed; backing off", zap.Error(err))
			if p.waitDurationOrStop(ctx, p.cfg.PollInterval) {
				return
			}
			continue
		}

		pollMs := float64(time.Since(pollStart).Microseconds()) / 1000
		pollBytes := 0
		for i := range out.Records {
			pollBytes += len(out.Records[i].Data)
		}
		p.tel.recordPoll(ctx, len(out.Records), pollBytes, pollMs)
		logger.Debug(
			"polled shard",
			zap.Int("records", len(out.Records)),
			zap.Int("bytes", pollBytes),
			zap.Float64("duration_ms", pollMs),
		)

		// Advance the checkpoint only over records that were delivered or are
		// permanently unprocessable. A record the downstream transiently
		// rejected must NOT be skipped — stop the batch there, checkpoint the
		// good prefix, and re-read from that point so valid telemetry is not
		// silently dropped under backpressure.
		var advanceSeq string
		retry := false
		for _, rec := range out.Records {
			if p.handleRecord(ctx, rec) == recordRetry {
				retry = true
				break
			}
			if s := aws.ToString(rec.SequenceNumber); s != "" {
				advanceSeq = s
			}
		}
		if advanceSeq != "" {
			if err := p.checkpointWithRetry(ctx, advanceSeq); err != nil {
				if !errors.Is(err, context.Canceled) {
					logger.Warn("checkpoint lost lease; stopping poller", zap.Error(err))
				}
				stop()
				return
			}
			p.tel.recordLeaseEvent(ctx, leaseCheckpoint, resultSuccess)
			logger.Debug("checkpoint advanced", zap.String("seq", advanceSeq))
		}
		if retry {
			// Re-read from the just-advanced checkpoint so the rejected record
			// (and the rest of the batch) is retried. Only a pass that
			// checkpointed nothing counts as "stuck" and earns a longer backoff;
			// any forward progress resets the counter.
			if advanceSeq != "" {
				stuckPasses = 0
			} else {
				stuckPasses++
			}
			if iter, err = p.openIterator(ctx); err != nil {
				logger.Error("re-open iterator failed; stopping poller", zap.Error(err))
				stop()
				return
			}
			delay := p.stuckBackoff(stuckPasses)
			if stuckPasses == stuckWarnAfter {
				logger.Warn(
					"shard not advancing; downstream keeps rejecting the head record — backing off (lease still held, no data dropped)",
					zap.Int("consecutive_stuck_passes", stuckPasses),
					zap.Duration("backoff", delay),
				)
			}
			if p.waitDurationOrStop(ctx, delay) {
				return
			}
			continue
		}
		// Reset stuck counter on forward progress or an empty poll. A nil
		// NextShardIterator means the shard is closed; the top of the loop
		// will write the SHARD_END sentinel on the next iteration.
		stuckPasses = 0
		iter = out.NextShardIterator
	}
}

// waitDurationOrStop blocks for d or until the context is cancelled or a
// graceful drain is requested. Returns true if the caller should stop — in
// both cases the last batch is already checkpointed.
func (p *shardPoller) waitDurationOrStop(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return true
	case <-p.drainCh:
		return true
	case <-timer.C:
		return false
	}
}

// stuckBackoff grows the re-read delay exponentially from PollInterval (passes
// <= 1) up to stuckBackoffMax, so a poll loop wedged on a rejecting downstream
// stops hammering the Kinesis read path.
func (p *shardPoller) stuckBackoff(passes int) time.Duration {
	if passes <= 1 {
		return p.cfg.PollInterval
	}
	shift := passes - 1
	if shift > 16 {
		shift = 16 // guard against overflow on a pathologically long stall
	}
	d := p.cfg.PollInterval << shift
	if d <= 0 || d > stuckBackoffMax {
		return stuckBackoffMax
	}
	return d
}

// openIterator picks the right ShardIteratorType based on the lease's
// checkpoint. TRIM_HORIZON starts at the oldest record; otherwise we
// resume just after the persisted sequence number.
func (p *shardPoller) openIterator(ctx context.Context) (*string, error) {
	p.leaseMu.Lock()
	checkpoint := p.leased.Checkpoint
	shardID := p.leased.ShardID
	p.leaseMu.Unlock()

	input := &kinesis.GetShardIteratorInput{
		StreamName: aws.String(p.cfg.StreamName),
		ShardId:    aws.String(shardID),
	}
	switch checkpoint {
	case "", lease.CheckpointTrimHorizon:
		input.ShardIteratorType = types.ShardIteratorTypeTrimHorizon
	case lease.CheckpointShardEnd:
		return nil, errors.New("shard already drained")
	default:
		input.ShardIteratorType = types.ShardIteratorTypeAfterSequenceNumber
		input.StartingSequenceNumber = aws.String(checkpoint)
	}
	out, err := p.client.GetShardIterator(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("get_shard_iterator: %w", err)
	}
	p.logger.Debug(
		"opened iterator",
		zap.String("shard", shardID),
		zap.String("type", string(input.ShardIteratorType)),
		zap.String("checkpoint", checkpoint),
	)
	return out.ShardIterator, nil
}

// writeShardEnd persists the SHARD_END sentinel so child shards become
// acquirable. Losing the sentinel forces the next owner to re-discover the
// closure from scratch — correct but wasteful — so a write that fails only
// because the poll context was cancelled (graceful shutdown racing the write)
// is retried once under the bounded exit context.
func (p *shardPoller) writeShardEnd(ctx context.Context, logger *zap.Logger) {
	err := p.checkpoint(ctx, lease.CheckpointShardEnd)
	if err != nil && (ctx.Err() != nil || errors.Is(err, context.Canceled)) {
		writeCtx, cancel := context.WithTimeout(p.exitCtx(), releaseTimeout)
		defer cancel()
		err = p.checkpoint(writeCtx, lease.CheckpointShardEnd)
	}
	if err != nil {
		logger.Warn("checkpoint SHARD_END failed", zap.Error(err))
	}
}
