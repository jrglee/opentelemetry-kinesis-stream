package awskinesisexporter

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// kinesisExporter exports both traces and metrics. A single type serves either
// signal: the factory returns it for WithTraces and WithMetrics alike, and the
// group/compress/oversize/PutRecords pipeline is shared (see record.go), with
// only pdata marshaling differing per signal (see signal.go). PutRecords is
// used throughout so tag-grouped microbatches ride one call.
type kinesisExporter struct {
	cfg        *Config
	client     *kinesis.Client
	tracesEnc  encoding.TracesEncoder
	metricsEnc encoding.MetricsEncoder
	logsEnc    encoding.LogsEncoder
	comp       encoding.Compressor
	logger     *zap.Logger
	tel        *exporterTelemetry
}

func newExporter(ctx context.Context, cfg *Config, set exporter.Settings) (*kinesisExporter, error) {
	tEnc, err := encoding.NewTracesEncoder(cfg.Encoding)
	if err != nil {
		return nil, fmt.Errorf("traces encoder: %w", err)
	}
	mEnc, err := encoding.NewMetricsEncoder(cfg.Encoding)
	if err != nil {
		return nil, fmt.Errorf("metrics encoder: %w", err)
	}
	lEnc, err := encoding.NewLogsEncoder(cfg.Encoding)
	if err != nil {
		return nil, fmt.Errorf("logs encoder: %w", err)
	}
	comp, err := encoding.NewCompressor(cfg.Compression)
	if err != nil {
		return nil, fmt.Errorf("compressor: %w", err)
	}
	client, err := newKinesisClient(ctx, cfg.Region, cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("kinesis client: %w", err)
	}
	tel, err := newExporterTelemetry(set.MeterProvider)
	if err != nil {
		return nil, fmt.Errorf("telemetry: %w", err)
	}
	return &kinesisExporter{
		cfg:        cfg,
		client:     client,
		tracesEnc:  tEnc,
		metricsEnc: mEnc,
		logsEnc:    lEnc,
		comp:       comp,
		logger:     set.Logger,
		tel:        tel,
	}, nil
}

func (e *kinesisExporter) Start(_ context.Context, _ component.Host) error { return nil }
func (e *kinesisExporter) Shutdown(_ context.Context) error                { return nil }
func (e *kinesisExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *kinesisExporter) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	return emit(ctx, e, td, tracesCodec(e.tracesEnc))
}

func (e *kinesisExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	return emit(ctx, e, md, metricsCodec(e.metricsEnc))
}

func (e *kinesisExporter) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	return emit(ctx, e, ld, logsCodec(e.logsEnc))
}

// classifyPutRecordsError marks errors the Collector's retry helper has no
// reason to retry. Throttling and unclassified AWS errors stay unwrapped so
// the standard retry/backoff path picks them up.
func classifyPutRecordsError(err error) error {
	var notFound *types.ResourceNotFoundException
	if errors.As(err, &notFound) {
		return consumererror.NewPermanent(fmt.Errorf("put_records: %w", err))
	}
	var invalid *types.InvalidArgumentException
	if errors.As(err, &invalid) {
		return consumererror.NewPermanent(fmt.Errorf("put_records: %w", err))
	}
	return fmt.Errorf("put_records: %w", err)
}
