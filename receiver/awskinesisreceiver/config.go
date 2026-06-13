package awskinesisreceiver

import (
	"errors"
	"fmt"
	"time"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// Config is the configuration for the Kinesis traces receiver.
//
// The PoC surface is narrow: in-memory shard cursor only, single replica, no
// DynamoDB leases. Fields for the checkpoint backing store, failure policy
// granularity, and EFO mode land when the round-trip is working end-to-end.
type Config struct {
	// StreamName is the source Kinesis Data Stream.
	StreamName string `mapstructure:"stream_name"`
	// Region is the AWS region for the stream.
	Region string `mapstructure:"region"`
	// Endpoint optionally overrides the AWS endpoint. Used by MiniStack and
	// other emulators; empty means the SDK default resolver.
	Endpoint string `mapstructure:"endpoint"`
	// Encoding names the wire-level marshaling format expected on records.
	Encoding encoding.Encoding `mapstructure:"encoding"`
	// Compression names the wire-level codec expected on records.
	Compression encoding.Codec `mapstructure:"compression"`
	// PollInterval is the delay between GetRecords calls on a single shard
	// when the previous response was empty. PoC default is 250 ms, which
	// stays well under Kinesis's 5-TPS-per-shard read limit.
	PollInterval time.Duration `mapstructure:"poll_interval"`
	// MaxRecords caps the GetRecords response size. Default 10000 is the
	// Kinesis maximum.
	MaxRecords int32 `mapstructure:"max_records"`
}

// Validate fails fast on configuration shapes the receiver cannot serve.
func (c *Config) Validate() error {
	if c.StreamName == "" {
		return errors.New("stream_name is required")
	}
	if c.Region == "" {
		return errors.New("region is required")
	}
	if _, err := encoding.NewTracesDecoder(c.Encoding); err != nil {
		return fmt.Errorf("encoding: %w", err)
	}
	if _, err := encoding.NewCompressor(c.Compression); err != nil {
		return fmt.Errorf("compression: %w", err)
	}
	if c.PollInterval <= 0 {
		return errors.New("poll_interval must be positive")
	}
	if c.MaxRecords <= 0 || c.MaxRecords > 10000 {
		return errors.New("max_records must be in (0, 10000]")
	}
	return nil
}
