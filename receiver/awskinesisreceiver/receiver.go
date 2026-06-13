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
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// kinesisReceiver polls one goroutine per shard at PollInterval. Cursor state
// is in-memory (the live shard iterator), which means restarts replay from
// TRIM_HORIZON. Durable checkpointing into DynamoDB is the next milestone.
type kinesisReceiver struct {
	cfg      *Config
	client   *kinesis.Client
	decoder  encoding.TracesDecoder
	comp     encoding.Compressor
	consumer consumer.Traces
	logger   *zap.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newReceiver(cfg *Config, set consumer.Traces, logger *zap.Logger) (*kinesisReceiver, error) {
	dec, err := encoding.NewTracesDecoder(cfg.Encoding)
	if err != nil {
		return nil, fmt.Errorf("decoder: %w", err)
	}
	comp, err := encoding.NewCompressor(cfg.Compression)
	if err != nil {
		return nil, fmt.Errorf("compressor: %w", err)
	}
	return &kinesisReceiver{
		cfg:      cfg,
		decoder:  dec,
		comp:     comp,
		consumer: set,
		logger:   logger,
	}, nil
}

func (r *kinesisReceiver) Start(ctx context.Context, _ component.Host) error {
	client, err := newKinesisClient(ctx, r.cfg.Region, r.cfg.Endpoint)
	if err != nil {
		return fmt.Errorf("kinesis client: %w", err)
	}
	r.client = client

	shards, err := r.listShards(ctx)
	if err != nil {
		return fmt.Errorf("list shards: %w", err)
	}
	if len(shards) == 0 {
		return errors.New("stream has no shards")
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())
	for _, shardID := range shards {
		r.wg.Add(1)
		go r.pollShard(shardID)
	}
	r.logger.Info(
		"kinesis receiver started",
		zap.String("stream", r.cfg.StreamName),
		zap.Int("shards", len(shards)),
	)
	return nil
}

func (r *kinesisReceiver) Shutdown(_ context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
	return nil
}

// listShards enumerates the stream's shards via the paginated ListShards API.
// DescribeStream is the older alternative; ListShards is preferred for new
// clients per AWS guidance and works against MiniStack.
func (r *kinesisReceiver) listShards(ctx context.Context) ([]string, error) {
	var (
		shards []string
		token  *string
	)
	for {
		input := &kinesis.ListShardsInput{NextToken: token}
		if token == nil {
			input.StreamName = aws.String(r.cfg.StreamName)
		}
		out, err := r.client.ListShards(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, s := range out.Shards {
			shards = append(shards, aws.ToString(s.ShardId))
		}
		if out.NextToken == nil {
			return shards, nil
		}
		token = out.NextToken
	}
}

func (r *kinesisReceiver) pollShard(shardID string) {
	defer r.wg.Done()

	iterOut, err := r.client.GetShardIterator(r.ctx, &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(r.cfg.StreamName),
		ShardId:           aws.String(shardID),
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	})
	if err != nil {
		r.logger.Error(
			"get_shard_iterator failed",
			zap.String("shard", shardID),
			zap.Error(err),
		)
		return
	}
	iter := iterOut.ShardIterator

	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		default:
		}
		if iter == nil {
			// Shard closed (resharding). PoC scope: stop reading; durable
			// reshard handling lands with parent-drains-before-child.
			r.logger.Info("shard closed; ending poll", zap.String("shard", shardID))
			return
		}
		out, err := r.client.GetRecords(r.ctx, &kinesis.GetRecordsInput{
			ShardIterator: iter,
			Limit:         aws.Int32(r.cfg.MaxRecords),
		})
		if err != nil {
			if r.ctx.Err() != nil {
				return
			}
			r.logger.Warn(
				"get_records failed",
				zap.String("shard", shardID),
				zap.Error(err),
			)
			select {
			case <-r.ctx.Done():
				return
			case <-ticker.C:
			}
			continue
		}
		for _, rec := range out.Records {
			r.handleRecord(shardID, rec)
		}
		iter = out.NextShardIterator

		if len(out.Records) == 0 {
			select {
			case <-r.ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}
}

func (r *kinesisReceiver) handleRecord(shardID string, rec types.Record) {
	raw, err := r.comp.Decompress(rec.Data)
	if err != nil {
		r.logger.Warn(
			"decompress failed",
			zap.String("shard", shardID),
			zap.String("seq", aws.ToString(rec.SequenceNumber)),
			zap.Error(err),
		)
		return
	}
	td, err := r.decoder.Unmarshal(raw)
	if err != nil {
		r.logger.Warn(
			"decode failed",
			zap.String("shard", shardID),
			zap.String("seq", aws.ToString(rec.SequenceNumber)),
			zap.Error(err),
		)
		return
	}
	if err := r.consumer.ConsumeTraces(r.ctx, td); err != nil {
		r.logger.Warn(
			"consume_traces failed",
			zap.String("shard", shardID),
			zap.String("seq", aws.ToString(rec.SequenceNumber)),
			zap.Error(err),
		)
	}
}
