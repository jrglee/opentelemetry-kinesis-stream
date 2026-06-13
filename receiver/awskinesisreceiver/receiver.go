package awskinesisreceiver

import (
	"context"
	"fmt"

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
	store, err := newLeaseStore(r.cfg)
	if err != nil {
		return fmt.Errorf("lease store: %w", err)
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

func (r *kinesisReceiver) Shutdown(_ context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	if r.coord != nil {
		r.coord.wait()
	}
	return nil
}

// newLeaseStore constructs the lease.Store named by cfg.LeaseBackend. The
// dynamodb implementation lands in a sibling file; this switch is the only
// place receiver code names backend implementations.
func newLeaseStore(cfg *Config) (lease.Store, error) {
	switch cfg.LeaseBackend {
	case LeaseBackendMemory:
		return lease.NewMemoryStore(), nil
	case LeaseBackendDynamoDB:
		return nil, fmt.Errorf("dynamodb lease backend not yet implemented")
	default:
		return nil, fmt.Errorf("unknown lease_backend %q", cfg.LeaseBackend)
	}
}
