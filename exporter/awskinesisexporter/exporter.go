package awskinesisexporter

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/google/uuid"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// kinesisExporter is the traces exporter. Microbatching of records below the
// 1 MiB limit is deferred to the upstream batchprocessor; this component
// encodes each ConsumeTraces call into a single Kinesis record. PutRecords
// is used (rather than PutRecord) so the batch shape is in place when
// in-exporter batching lands.
type kinesisExporter struct {
	cfg     *Config
	client  *kinesis.Client
	encoder encoding.TracesEncoder
	comp    encoding.Compressor
	logger  *zap.Logger
}

func newExporter(ctx context.Context, cfg *Config, logger *zap.Logger) (*kinesisExporter, error) {
	enc, err := encoding.NewTracesEncoder(cfg.Encoding)
	if err != nil {
		return nil, fmt.Errorf("encoder: %w", err)
	}
	comp, err := encoding.NewCompressor(cfg.Compression)
	if err != nil {
		return nil, fmt.Errorf("compressor: %w", err)
	}
	client, err := newKinesisClient(ctx, cfg.Region, cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("kinesis client: %w", err)
	}
	return &kinesisExporter{
		cfg:     cfg,
		client:  client,
		encoder: enc,
		comp:    comp,
		logger:  logger,
	}, nil
}

func (e *kinesisExporter) Start(_ context.Context, _ component.Host) error { return nil }
func (e *kinesisExporter) Shutdown(_ context.Context) error                { return nil }
func (e *kinesisExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *kinesisExporter) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	raw, err := e.encoder.Marshal(td)
	if err != nil {
		return fmt.Errorf("marshal traces: %w", err)
	}
	payload, err := e.comp.Compress(raw)
	if err != nil {
		return fmt.Errorf("compress payload: %w", err)
	}
	if len(payload) > e.cfg.MaxRecordSize {
		// PoC failure policy: log and continue. Repacking lands later.
		e.logger.Warn(
			"dropping oversize kinesis record",
			zap.Int("payload_bytes", len(payload)),
			zap.Int("limit_bytes", e.cfg.MaxRecordSize),
			zap.Int("span_count", td.SpanCount()),
		)
		return nil
	}
	out, err := e.client.PutRecords(ctx, &kinesis.PutRecordsInput{
		StreamName: aws.String(e.cfg.StreamName),
		Records: []types.PutRecordsRequestEntry{{
			Data:         payload,
			PartitionKey: aws.String(uuid.NewString()),
		}},
	})
	if err != nil {
		return fmt.Errorf("put_records: %w", err)
	}
	if out.FailedRecordCount != nil && *out.FailedRecordCount > 0 {
		// Per-record failures: log with codes and let Collector retry the
		// whole call. Granular per-record retry is a v1 concern, not PoC.
		for i, r := range out.Records {
			if r.ErrorCode == nil {
				continue
			}
			e.logger.Warn(
				"kinesis record rejected",
				zap.Int("index", i),
				zap.String("code", aws.ToString(r.ErrorCode)),
				zap.String("message", aws.ToString(r.ErrorMessage)),
			)
		}
		return fmt.Errorf("put_records: %d records failed", *out.FailedRecordCount)
	}
	return nil
}
