package awskinesisreceiver

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
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
	return &Config{
		Encoding:     encoding.EncodingOTLPProto,
		Compression:  encoding.CodecNone,
		PollInterval: 250 * time.Millisecond,
		MaxRecords:   10000,
	}
}

func createTracesReceiver(
	_ context.Context,
	set receiver.Settings,
	rawCfg component.Config,
	next consumer.Traces,
) (receiver.Traces, error) {
	cfg, ok := rawCfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("unexpected config type %T", rawCfg)
	}
	return newReceiver(cfg, next, set.Logger)
}
