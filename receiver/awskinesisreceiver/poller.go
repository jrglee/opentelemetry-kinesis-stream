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
			if !errors.Is(err, context.Canceled) {
				logger.Warn("heartbeat lost lease; stopping poller", zap.Error(err))
			}
			stop()
			return
		}
	}
}

func (p *shardPoller) runPoll(ctx context.Context, stop context.CancelFunc, logger *zap.Logger) {
	iter, err := p.openIterator(ctx)
	if err != nil {
		logger.Error("open iterator failed", zap.Error(err))
		stop()
		return
	}
	pollTick := time.NewTicker(p.cfg.PollInterval)
	defer pollTick.Stop()

	for {
		if ctx.Err() != nil {
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
		if iter == nil {
			p.writeShardEnd(ctx, logger)
			return
		}
		out, err := p.client.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: iter,
			Limit:         aws.Int32(p.cfg.MaxRecords),
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Re-open from the persisted checkpoint rather than reusing the
			// iterator: an ExpiredIteratorException would otherwise make the
			// loop spin on a dead iterator forever. Other transient errors
			// (throttling) resume from the same point harmlessly.
			logger.Warn("get_records failed; re-opening iterator", zap.Error(err))
			if iter, err = p.openIterator(ctx); err != nil {
				logger.Error("re-open iterator failed; stopping poller", zap.Error(err))
				stop()
				return
			}
			if p.waitOrStop(ctx, pollTick) {
				return
			}
			continue
		}

		// Advance the checkpoint only over records that were delivered or are
		// permanently unprocessable. A record the downstream transiently
		// rejected must NOT be skipped — we stop the batch there, checkpoint
		// the good prefix, and re-read from that point so valid telemetry is
		// not silently dropped under backpressure.
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
			if err := p.checkpoint(ctx, advanceSeq); err != nil {
				if !errors.Is(err, context.Canceled) {
					logger.Warn("checkpoint lost lease; stopping poller", zap.Error(err))
				}
				stop()
				return
			}
		}
		if retry {
			// Re-read from the just-advanced checkpoint after a short pause so
			// the rejected record (and the rest of the batch) is retried.
			if iter, err = p.openIterator(ctx); err != nil {
				logger.Error("re-open iterator failed; stopping poller", zap.Error(err))
				stop()
				return
			}
			if p.waitOrStop(ctx, pollTick) {
				return
			}
			continue
		}
		iter = out.NextShardIterator

		if len(out.Records) == 0 {
			if p.waitOrStop(ctx, pollTick) {
				return
			}
		}
	}
}

// waitOrStop blocks until the poll ticker fires, the context is cancelled, or
// a graceful drain is requested. It returns true if the caller should stop
// (cancel or drain) — in both cases the last batch is already checkpointed.
func (p *shardPoller) waitOrStop(ctx context.Context, tick *time.Ticker) bool {
	select {
	case <-ctx.Done():
		return true
	case <-p.drainCh:
		return true
	case <-tick.C:
		return false
	}
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
	return out.ShardIterator, nil
}

// writeShardEnd persists the SHARD_END sentinel so child shards become
// acquirable. On graceful shutdown ctx is already cancelled, in which case
// we attempt the write under a bounded background context — losing the
// sentinel forces the next owner of the shard to re-discover it from
// scratch, which is correct but wasteful.
func (p *shardPoller) writeShardEnd(ctx context.Context, logger *zap.Logger) {
	writeCtx := ctx
	if ctx.Err() != nil {
		var cancel context.CancelFunc
		writeCtx, cancel = context.WithTimeout(context.Background(), releaseTimeout)
		defer cancel()
	}
	if err := p.checkpoint(writeCtx, lease.CheckpointShardEnd); err != nil {
		logger.Warn("checkpoint SHARD_END failed", zap.Error(err))
	}
}

// recordResult tells the poll loop whether the checkpoint may advance past a
// record. recordRetry means a valid record was transiently rejected and must
// be re-read; recordSkip and recordOK both let the checkpoint advance.
type recordResult int

const (
	recordOK    recordResult = iota // delivered downstream
	recordSkip                      // permanently unprocessable — safe to skip
	recordRetry                     // transient downstream failure — must re-read
)

func (p *shardPoller) handleRecord(ctx context.Context, rec types.Record) recordResult {
	raw, err := p.comp.Decompress(rec.Data)
	if err != nil {
		p.logger.Warn(
			"decompress failed; skipping record",
			zap.String("shard", p.shardID()),
			zap.String("seq", aws.ToString(rec.SequenceNumber)),
			zap.Error(err),
		)
		p.maybeDeadLetter(ctx, rec, "decompress")
		return recordSkip
	}
	// The sink performs the only signal-specific work: decode + deliver. It
	// reports a decode failure so the unprocessable bytes can be dead-lettered;
	// a transient consume failure becomes recordRetry so the checkpoint does
	// not advance past valid telemetry the downstream merely rejected.
	result, decodeFailed := p.sink.consume(ctx, raw)
	switch {
	case decodeFailed:
		p.logger.Warn(
			"decode failed; skipping record",
			zap.String("shard", p.shardID()),
			zap.String("seq", aws.ToString(rec.SequenceNumber)),
		)
		p.maybeDeadLetter(ctx, rec, "decode")
	case result == recordRetry:
		p.logger.Warn(
			"consume failed; will retry record",
			zap.String("shard", p.shardID()),
			zap.String("seq", aws.ToString(rec.SequenceNumber)),
		)
	case result == recordSkip:
		p.logger.Warn(
			"consume permanently rejected; skipping record",
			zap.String("shard", p.shardID()),
			zap.String("seq", aws.ToString(rec.SequenceNumber)),
		)
	}
	return result
}

// maybeDeadLetter re-emits an unprocessable raw record into the pipeline when
// dead-lettering is enabled, so the bytes are observable rather than silently
// dropped. Emit failures are logged and ignored — the record is already being
// skipped.
func (p *shardPoller) maybeDeadLetter(ctx context.Context, rec types.Record, failureClass string) {
	if !p.cfg.DeadLetter.Enabled {
		return
	}
	if err := p.sink.deadLetter(ctx, rec, failureClass, string(p.cfg.Encoding), string(p.cfg.Compression)); err != nil {
		p.logger.Warn(
			"dead-letter emit failed",
			zap.String("shard", p.shardID()),
			zap.String("seq", aws.ToString(rec.SequenceNumber)),
			zap.Error(err),
		)
	}
}

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

// release is the deferred exit path. It runs under a bounded background
// context so a hung DynamoDB Release never blocks the collector's
// graceful-shutdown deadline. Lease-conflict errors are silently dropped:
// they mean the lease was already stolen, which is the postcondition
// Release would have achieved anyway.
func (p *shardPoller) release() {
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
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
