package awskinesisreceiver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
	"github.com/jrglee/opentelemetry-kinesis-stream/internal/lease"
)

// shardPoller runs the GetRecords loop for one shard while holding its lease.
// It owns Heartbeat and Checkpoint writes for the lease; the coordinator
// owns Acquire and Release. Splitting writer responsibility this way keeps
// the lease's Counter monotonic without cross-goroutine locking.
type shardPoller struct {
	cfg      *Config
	client   *kinesis.Client
	store    lease.Store
	decoder  encoding.TracesDecoder
	comp     encoding.Compressor
	consumer consumer.Traces
	logger   *zap.Logger

	leased lease.Lease
}

func (p *shardPoller) run(ctx context.Context) {
	logger := p.logger.With(zap.String("shard", p.leased.ShardID))

	iter, err := p.openIterator(ctx)
	if err != nil {
		logger.Error("open iterator failed", zap.Error(err))
		p.releaseQuiet(ctx)
		return
	}

	heartbeatTick := time.NewTicker(p.cfg.HeartbeatInterval)
	defer heartbeatTick.Stop()
	pollTick := time.NewTicker(p.cfg.PollInterval)
	defer pollTick.Stop()

	for {
		select {
		case <-ctx.Done():
			p.releaseQuiet(context.Background())
			return
		case <-heartbeatTick.C:
			if err := p.heartbeat(ctx); err != nil {
				logger.Warn("heartbeat lost lease; stopping poller", zap.Error(err))
				return
			}
			continue
		default:
		}
		if iter == nil {
			// SHARD_END reached. Persist the sentinel and release; children
			// become acquirable next discovery cycle.
			if err := p.checkpoint(ctx, lease.CheckpointShardEnd); err != nil {
				logger.Warn("checkpoint SHARD_END failed", zap.Error(err))
			}
			p.releaseQuiet(ctx)
			return
		}
		out, err := p.client.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: iter,
			Limit:         aws.Int32(p.cfg.MaxRecords),
		})
		if err != nil {
			if ctx.Err() != nil {
				p.releaseQuiet(context.Background())
				return
			}
			logger.Warn("get_records failed", zap.Error(err))
			select {
			case <-ctx.Done():
				p.releaseQuiet(context.Background())
				return
			case <-pollTick.C:
			}
			continue
		}

		var lastSeq string
		for _, rec := range out.Records {
			if p.handleRecord(ctx, rec) {
				lastSeq = aws.ToString(rec.SequenceNumber)
			}
		}
		if lastSeq != "" {
			if err := p.checkpoint(ctx, lastSeq); err != nil {
				logger.Warn("checkpoint lost lease; stopping poller", zap.Error(err))
				return
			}
		}
		iter = out.NextShardIterator

		if len(out.Records) == 0 {
			select {
			case <-ctx.Done():
				p.releaseQuiet(context.Background())
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
	input := &kinesis.GetShardIteratorInput{
		StreamName: aws.String(p.cfg.StreamName),
		ShardId:    aws.String(p.leased.ShardID),
	}
	switch p.leased.Checkpoint {
	case "", lease.CheckpointTrimHorizon:
		input.ShardIteratorType = types.ShardIteratorTypeTrimHorizon
	case lease.CheckpointShardEnd:
		return nil, errors.New("shard already drained")
	default:
		input.ShardIteratorType = types.ShardIteratorTypeAfterSequenceNumber
		input.StartingSequenceNumber = aws.String(p.leased.Checkpoint)
	}
	out, err := p.client.GetShardIterator(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("get_shard_iterator: %w", err)
	}
	return out.ShardIterator, nil
}

func (p *shardPoller) handleRecord(ctx context.Context, rec types.Record) bool {
	raw, err := p.comp.Decompress(rec.Data)
	if err != nil {
		p.logger.Warn(
			"decompress failed",
			zap.String("shard", p.leased.ShardID),
			zap.String("seq", aws.ToString(rec.SequenceNumber)),
			zap.Error(err),
		)
		return false
	}
	td, err := p.decoder.Unmarshal(raw)
	if err != nil {
		p.logger.Warn(
			"decode failed",
			zap.String("shard", p.leased.ShardID),
			zap.String("seq", aws.ToString(rec.SequenceNumber)),
			zap.Error(err),
		)
		return false
	}
	if err := p.consumer.ConsumeTraces(ctx, td); err != nil {
		p.logger.Warn(
			"consume_traces failed",
			zap.String("shard", p.leased.ShardID),
			zap.String("seq", aws.ToString(rec.SequenceNumber)),
			zap.Error(err),
		)
		return false
	}
	return true
}

func (p *shardPoller) heartbeat(ctx context.Context) error {
	updated, err := p.store.Heartbeat(ctx, p.leased)
	if err != nil {
		return err
	}
	p.leased = updated
	return nil
}

func (p *shardPoller) checkpoint(ctx context.Context, seq string) error {
	updated, err := p.store.Checkpoint(ctx, p.leased, seq)
	if err != nil {
		return err
	}
	p.leased = updated
	return nil
}

// releaseQuiet attempts a best-effort Release. Failures are logged but do
// not propagate — lease expiry will reclaim the row from a dead owner anyway.
func (p *shardPoller) releaseQuiet(ctx context.Context) {
	if err := p.store.Release(ctx, p.leased); err != nil && !errors.Is(err, lease.ErrLeaseConflict) {
		p.logger.Warn(
			"release failed",
			zap.String("shard", p.leased.ShardID),
			zap.Error(err),
		)
	}
}
