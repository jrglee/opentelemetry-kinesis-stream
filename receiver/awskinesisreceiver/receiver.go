package awskinesisreceiver

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
	"github.com/jrglee/opentelemetry-kinesis-stream/internal/lease"
)

// kinesisReceiver is the public component. It owns the lease store and the
// coordinator goroutine; per-shard polling lives in coordinator/poller.
type kinesisReceiver struct {
	cfg      *Config
	consumer consumer.Traces
	logger   *zap.Logger

	coord  *coordinator
	cancel context.CancelFunc
}

func newReceiver(cfg *Config, next consumer.Traces, logger *zap.Logger) (*kinesisReceiver, error) {
	return &kinesisReceiver{cfg: cfg, consumer: next, logger: logger}, nil
}

func (r *kinesisReceiver) Start(ctx context.Context, _ component.Host) error {
	client, err := newKinesisClient(ctx, r.cfg.Region, r.cfg.Endpoint)
	if err != nil {
		return fmt.Errorf("kinesis client: %w", err)
	}
	dec, err := encoding.NewTracesDecoder(r.cfg.Encoding)
	if err != nil {
		return fmt.Errorf("decoder: %w", err)
	}
	comp, err := encoding.NewCompressor(r.cfg.Compression)
	if err != nil {
		return fmt.Errorf("compressor: %w", err)
	}
	store, err := newLeaseStore(ctx, r.cfg)
	if err != nil {
		return fmt.Errorf("lease store: %w", err)
	}

	if r.cfg.LeaseBackend == LeaseBackendMemory {
		r.logger.Warn("memory lease backend selected; checkpoints will not survive process restart, " +
			"and every replica in a multi-replica deployment will independently re-read every shard — " +
			"set lease_backend: dynamodb in production")
	}

	workerID := r.cfg.WorkerID
	if workerID == "" {
		workerID = "otelcol-" + uuid.NewString()
	}

	r.coord = &coordinator{
		cfg:      r.cfg,
		client:   client,
		store:    store,
		decoder:  dec,
		comp:     comp,
		consumer: r.consumer,
		logger:   r.logger,
		workerID: workerID,
		active:   make(map[string]*activePoller),
		observed: make(map[string]observation),
	}

	bgCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	if err := r.coord.start(bgCtx); err != nil {
		cancel()
		return fmt.Errorf("coordinator start: %w", err)
	}
	r.logger.Info(
		"kinesis receiver started",
		zap.String("stream", r.cfg.StreamName),
		zap.String("worker_id", workerID),
		zap.String("lease_backend", string(r.cfg.LeaseBackend)),
	)
	return nil
}

// Shutdown drains the receiver gracefully: it stops shard discovery and asks
// every poller to finish its in-flight batch, persist a final checkpoint, and
// release its lease. A clean drain means a redeployed or rebalanced consumer
// resumes from a current checkpoint without re-delivering records. If the
// collector-provided deadline fires before draining completes, in-flight
// pollers are hard-cancelled (their Release calls are independently bounded)
// and the deadline error is returned.
func (r *kinesisReceiver) Shutdown(ctx context.Context) error {
	if r.coord == nil {
		if r.cancel != nil {
			r.cancel()
		}
		return nil
	}

	r.coord.drainAndStop()
	done := make(chan struct{})
	go func() {
		r.coord.wait()
		close(done)
	}()
	select {
	case <-done:
		r.cancel()
		return nil
	case <-ctx.Done():
		r.logger.Warn("shutdown deadline expired; hard-cancelling pollers", zap.Error(ctx.Err()))
		r.cancel()
		<-done
		return ctx.Err()
	}
}

// newLeaseStore constructs the lease.Store named by cfg.LeaseBackend. The
// dynamodb implementation lives in internal/lease; this switch is the only
// place receiver code names backend implementations.
func newLeaseStore(ctx context.Context, cfg *Config) (lease.Store, error) {
	switch cfg.LeaseBackend {
	case LeaseBackendMemory:
		return lease.NewMemoryStore(), nil
	case LeaseBackendDynamoDB:
		client, err := newDynamoDBClient(ctx, cfg.Region, cfg.Endpoint)
		if err != nil {
			return nil, fmt.Errorf("dynamodb client: %w", err)
		}
		return lease.NewDynamoDBStore(client, cfg.LeaseTable), nil
	default:
		return nil, fmt.Errorf("unknown lease_backend %q", cfg.LeaseBackend)
	}
}

// newDynamoDBClient mirrors newKinesisClient: SDK default credential chain
// with an optional endpoint override for emulators.
func newDynamoDBClient(ctx context.Context, region, endpoint string) (*dynamodb.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, err
	}
	opts := []func(*dynamodb.Options){}
	if endpoint != "" {
		opts = append(opts, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	}
	return dynamodb.NewFromConfig(cfg, opts...), nil
}
