package awskinesisreceiver

import (
	"errors"
	"fmt"
	"time"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// LeaseBackend selects the implementation behind the shard-lease store.
type LeaseBackend string

const (
	// LeaseBackendMemory keeps lease state in-process. Single-replica only.
	LeaseBackendMemory LeaseBackend = "memory"
	// LeaseBackendDynamoDB persists lease state in a KCL-compatible DynamoDB
	// table. Required for multi-replica deployments.
	LeaseBackendDynamoDB LeaseBackend = "dynamodb"
)

// Config is the configuration for the Kinesis traces receiver.
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
	// when the previous response was empty. Default is 250 ms.
	PollInterval time.Duration `mapstructure:"poll_interval"`
	// MaxRecords caps the GetRecords response size. Default 10000 (Kinesis maximum).
	MaxRecords int32 `mapstructure:"max_records"`

	// WorkerID uniquely identifies this receiver replica. Two replicas with
	// the same WorkerID will fight over leases. Empty means a random UUID is
	// generated at startup; persisting a stable WorkerID across restarts is
	// recommended in production.
	WorkerID string `mapstructure:"worker_id"`
	// LeaseBackend selects the lease store. Default: memory.
	LeaseBackend LeaseBackend `mapstructure:"lease_backend"`
	// LeaseTable names the DynamoDB table for the dynamodb backend. Ignored
	// for memory.
	LeaseTable string `mapstructure:"lease_table"`
	// LeaseDuration is the time after which a lease is considered expired
	// and may be stolen. Must be greater than HeartbeatInterval. Default 30s.
	LeaseDuration time.Duration `mapstructure:"lease_duration"`
	// HeartbeatInterval is how often a poller re-asserts ownership. Default 5s.
	HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval"`
	// DiscoveryInterval is how often the coordinator re-lists shards and
	// re-attempts acquisition of unowned leases. Default 30s.
	DiscoveryInterval time.Duration `mapstructure:"discovery_interval"`

	// DeadLetter controls re-emitting unprocessable records into the pipeline.
	DeadLetter DeadLetterConfig `mapstructure:"dead_letter"`
}

// DeadLetterConfig controls dead-letter handling. When enabled, a record that
// cannot be decompressed or decoded is wrapped (raw bytes + failure metadata)
// and re-emitted into the receiver's own pipeline — as a span for a traces
// receiver, a gauge for a metrics receiver — so operators route failures with
// standard components rather than losing them silently.
type DeadLetterConfig struct {
	Enabled bool `mapstructure:"enabled"`
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
	switch c.LeaseBackend {
	case LeaseBackendMemory:
	case LeaseBackendDynamoDB:
		if c.LeaseTable == "" {
			return errors.New("lease_table is required when lease_backend=dynamodb")
		}
	default:
		return fmt.Errorf("unknown lease_backend %q", c.LeaseBackend)
	}
	if c.LeaseDuration <= 0 {
		return errors.New("lease_duration must be positive")
	}
	if c.HeartbeatInterval <= 0 {
		return errors.New("heartbeat_interval must be positive")
	}
	if c.HeartbeatInterval >= c.LeaseDuration {
		return errors.New("heartbeat_interval must be less than lease_duration")
	}
	if c.DiscoveryInterval <= 0 {
		return errors.New("discovery_interval must be positive")
	}
	return nil
}
