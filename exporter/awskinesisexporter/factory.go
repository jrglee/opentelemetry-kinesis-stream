package awskinesisexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

var componentType = component.MustNewType("awskinesis")

// NewFactory returns the exporter factory.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		componentType,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, component.StabilityLevelDevelopment),
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

func createTracesExporter(
	_ context.Context,
	_ exporter.Settings,
	_ component.Config,
) (exporter.Traces, error) {
	return noopTracesExporter{}, nil
}

type noopTracesExporter struct{}

func (noopTracesExporter) Start(_ context.Context, _ component.Host) error { return nil }
func (noopTracesExporter) Shutdown(_ context.Context) error                { return nil }
func (noopTracesExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (noopTracesExporter) ConsumeTraces(_ context.Context, _ ptrace.Traces) error { return nil }
