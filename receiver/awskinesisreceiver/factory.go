package awskinesisreceiver

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

var componentType = component.MustNewType("awskinesis")

// NewFactory returns the receiver factory.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		componentType,
		createDefaultConfig,
		receiver.WithTraces(createTracesReceiver, component.StabilityLevelDevelopment),
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

func createTracesReceiver(
	_ context.Context,
	_ receiver.Settings,
	_ component.Config,
	_ consumer.Traces,
) (receiver.Traces, error) {
	return noopReceiver{}, nil
}

type noopReceiver struct{}

func (noopReceiver) Start(_ context.Context, _ component.Host) error { return nil }
func (noopReceiver) Shutdown(_ context.Context) error                { return nil }
