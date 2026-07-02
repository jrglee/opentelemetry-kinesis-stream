package awskinesisexporter

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

var componentType = component.MustNewType("awskinesis")

// NewFactory returns the exporter factory.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		componentType,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, component.StabilityLevelDevelopment),
		exporter.WithMetrics(createMetricsExporter, component.StabilityLevelDevelopment),
		exporter.WithLogs(createLogsExporter, component.StabilityLevelDevelopment),
	)
}

func createDefaultConfig() component.Config {
	// The per-attempt timeout must comfortably exceed the internal PutRecords
	// retry budget (maxPutAttempts with backoff ≈ 3s worst case) plus the RTTs
	// of a multi-chunk flush; the helper's 5s default leaves attempts dying
	// mid-flush, duplicating head chunks on every retry. 30s gives a full
	// flush room to finish or fail on its own terms.
	timeout := exporterhelper.NewDefaultTimeoutConfig()
	timeout.Timeout = 30 * time.Second
	return &Config{
		TimeoutConfig: timeout,
		QueueConfig:   configoptional.Some(exporterhelper.NewDefaultQueueConfig()),
		RetryConfig:   configretry.NewDefaultBackOffConfig(),
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecNone,
		MaxRecordSize: 1 << 20, // 1 MiB: conservative floor every stream accepts
		PartitionKey: PartitionKeyConfig{
			Strategy: partitionStrategyRandom,
			Hash:     hashXXHash,
		},
		Oversize: OversizeConfig{
			Policies:               []string{oversizeSplitHalf},
			MaxAttempts:            8,
			MaxAttributeValueBytes: 4096,
		},
		PutRecords: PutRecordsConfig{
			MaxRecords: 500,     // conservative per-call record count
			MaxBytes:   5 << 20, // 5 MiB: conservative per-call aggregate
		},
	}
}

// helperOptions is the shared exporterhelper wiring: queue, retry, and timeout
// from config, plus lifecycle passthrough. The helper owns whole-request
// retries; the exporter's PutRecords loop keeps only per-record partial
// failures (see record.go).
func helperOptions(e *kinesisExporter, cfg *Config) []exporterhelper.Option {
	return []exporterhelper.Option{
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		exporterhelper.WithStart(e.Start),
		exporterhelper.WithShutdown(e.Shutdown),
		exporterhelper.WithTimeout(cfg.TimeoutConfig),
		exporterhelper.WithRetry(cfg.RetryConfig),
		exporterhelper.WithQueue(cfg.QueueConfig),
	}
}

func createTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	rawCfg component.Config,
) (exporter.Traces, error) {
	cfg, ok := rawCfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("unexpected config type %T", rawCfg)
	}
	e, err := newExporter(ctx, cfg, set)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewTraces(ctx, set, cfg, e.ConsumeTraces, helperOptions(e, cfg)...)
}

func createMetricsExporter(
	ctx context.Context,
	set exporter.Settings,
	rawCfg component.Config,
) (exporter.Metrics, error) {
	cfg, ok := rawCfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("unexpected config type %T", rawCfg)
	}
	e, err := newExporter(ctx, cfg, set)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewMetrics(ctx, set, cfg, e.ConsumeMetrics, helperOptions(e, cfg)...)
}

func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	rawCfg component.Config,
) (exporter.Logs, error) {
	cfg, ok := rawCfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("unexpected config type %T", rawCfg)
	}
	e, err := newExporter(ctx, cfg, set)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewLogs(ctx, set, cfg, e.ConsumeLogs, helperOptions(e, cfg)...)
}
