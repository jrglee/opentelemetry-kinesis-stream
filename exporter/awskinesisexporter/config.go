package awskinesisexporter

import (
	"errors"
	"fmt"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// Config is the configuration for the Kinesis traces exporter.
//
// The PoC surface is intentionally narrow. Fields landing later (partition-key
// strategy, microbatch triggers, oversize-record policy, retry/queue tuning)
// stay out of the type until they have a real use case to justify them.
type Config struct {
	// StreamName is the target Kinesis Data Stream.
	StreamName string `mapstructure:"stream_name"`
	// Region is the AWS region for the stream.
	Region string `mapstructure:"region"`
	// Endpoint optionally overrides the AWS endpoint. Used by MiniStack and
	// other emulators; empty means the SDK default resolver.
	Endpoint string `mapstructure:"endpoint"`
	// Encoding is the wire-level marshaling format of the payload.
	Encoding encoding.Encoding `mapstructure:"encoding"`
	// Compression is the wire-level codec applied to the marshaled payload.
	Compression encoding.Codec `mapstructure:"compression"`
	// MaxRecordSize caps the post-compression record payload in bytes. The
	// hard Kinesis ceiling is 1 MiB on the standard API and 5 MiB once raised.
	// PoC default is 1 MiB; oversize records are dropped with a log line
	// rather than repacked.
	MaxRecordSize int `mapstructure:"max_record_size"`
}

// Validate fails fast on configuration shapes the exporter cannot serve.
func (c *Config) Validate() error {
	if c.StreamName == "" {
		return errors.New("stream_name is required")
	}
	if c.Region == "" {
		return errors.New("region is required")
	}
	if _, err := encoding.NewTracesEncoder(c.Encoding); err != nil {
		return fmt.Errorf("encoding: %w", err)
	}
	if _, err := encoding.NewCompressor(c.Compression); err != nil {
		return fmt.Errorf("compression: %w", err)
	}
	if c.MaxRecordSize <= 0 {
		return errors.New("max_record_size must be positive")
	}
	return nil
}
