package awskinesisexporter

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"

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
	)
}

func createDefaultConfig() component.Config {
	return &Config{
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

func createTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	rawCfg component.Config,
) (exporter.Traces, error) {
	cfg, ok := rawCfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("unexpected config type %T", rawCfg)
	}
	return newExporter(ctx, cfg, set)
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
	return newExporter(ctx, cfg, set)
}
