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
	"go.opentelemetry.io/collector/consumer"
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
	cfg      *Config
	client   *kinesis.Client
	store    lease.Store
	decoder  encoding.TracesDecoder
	comp     encoding.Compressor
	consumer consumer.Traces
	logger   *zap.Logger

	leaseMu sync.Mutex
	leased  lease.Lease
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
			logger.Warn("get_records failed", zap.Error(err))
			select {
			case <-ctx.Done():
				return
			case <-pollTick.C:
			}
			continue
		}

		// PoC failure policy: skip bad records but advance the checkpoint
		// past them so a takeover does not replay the same poison-pill
		// forever. Records that successfully decode + ConsumeTraces are
		// delivered downstream; records that fail are logged.
		var lastSeq string
		for _, rec := range out.Records {
			p.handleRecord(ctx, rec)
			if s := aws.ToString(rec.SequenceNumber); s != "" {
				lastSeq = s
			}
		}
		if lastSeq != "" {
			if err := p.checkpoint(ctx, lastSeq); err != nil {
				if !errors.Is(err, context.Canceled) {
					logger.Warn("checkpoint lost lease; stopping poller", zap.Error(err))
				}
				stop()
				return
			}
		}
		iter = out.NextShardIterator

		if len(out.Records) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-pollTick.C:
			}
		}
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

func (p *shardPoller) handleRecord(ctx context.Context, rec types.Record) {
	raw, err := p.comp.Decompress(rec.Data)
	if err != nil {
		p.logger.Warn(
			"decompress failed",
			zap.String("shard", p.shardID()),
			zap.String("seq", aws.ToString(rec.SequenceNumber)),
			zap.Error(err),
		)
		return
	}
	td, err := p.decoder.Unmarshal(raw)
	if err != nil {
		p.logger.Warn(
			"decode failed",
			zap.String("shard", p.shardID()),
			zap.String("seq", aws.ToString(rec.SequenceNumber)),
			zap.Error(err),
		)
		return
	}
	if err := p.consumer.ConsumeTraces(ctx, td); err != nil {
		p.logger.Warn(
			"consume_traces failed",
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
