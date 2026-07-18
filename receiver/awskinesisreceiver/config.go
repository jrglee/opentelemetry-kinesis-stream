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

// WorkerResolutionStrategy selects how the lease-owner identity (the DynamoDB
// leaseOwner attribute) is resolved at startup.
//
// The identity MUST be unique per live replica: this is a correctness
// invariant, not just an efficiency knob. A replica reclaims a lease whose
// stored owner equals its own identity without waiting out lease_duration (that
// is what makes a restart fast), so two live replicas that resolve the same
// identity will each keep reclaiming the other's shards and deliver them twice.
// A stable identity additionally lets a restarted replica reclaim its own
// leases immediately instead of waiting out lease_duration.
type WorkerResolutionStrategy string

const (
	// WorkerStrategyStatic uses WorkerID verbatim; when WorkerID is empty a
	// random "otelcol-<uuid>" is generated (unique, but not stable across
	// restarts). The default, and the only strategy that reads WorkerID.
	WorkerStrategyStatic WorkerResolutionStrategy = "static"
	// WorkerStrategyHostname uses the OS hostname. Safe only where each replica
	// has a unique, stable hostname (e.g. a Kubernetes StatefulSet pod). It is
	// the wrong choice where hostnames collide (host-network mode, or several
	// replicas per host) — that double-delivers — or where the hostname is the
	// container id (plain Docker, ECS bridge mode), which is unique but changes
	// every restart, so no leases are reclaimed.
	WorkerStrategyHostname WorkerResolutionStrategy = "hostname"
	// WorkerStrategyECS reads the task ID from the ECS container metadata
	// endpoint. Unique per task and stable across in-task container restarts,
	// so a container that restarts within its task reclaims its own leases with
	// no operator wiring. A task *replacement* gets a new task ID (a fresh
	// identity), which is safe — just the normal reclaim-after-lease_duration.
	WorkerStrategyECS WorkerResolutionStrategy = "ecs"
	// WorkerStrategyFile reads the trimmed contents of WorkerIDFile. For
	// platforms that project a unique, stable identity onto a file (e.g. the
	// downward API) rather than an environment variable. Mounting the same
	// content to two replicas double-delivers.
	WorkerStrategyFile WorkerResolutionStrategy = "file"
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

	// WorkerResolutionStrategy selects how the lease-owner identity is resolved
	// at startup. Default: static. See WorkerResolutionStrategy for the options.
	WorkerResolutionStrategy WorkerResolutionStrategy `mapstructure:"worker_resolution_strategy"`
	// WorkerID uniquely identifies this receiver replica under the static
	// strategy. Two replicas with the same WorkerID will fight over leases.
	// Empty means a random UUID is generated at startup; persisting a stable
	// WorkerID across restarts is recommended in production. Ignored by every
	// non-static strategy.
	WorkerID string `mapstructure:"worker_id"`
	// WorkerIDFile is the path read by the file strategy. Ignored otherwise.
	WorkerIDFile string `mapstructure:"worker_id_file"`
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
	switch c.WorkerResolutionStrategy {
	case WorkerStrategyStatic, WorkerStrategyHostname, WorkerStrategyECS, WorkerStrategyFile:
	default:
		return fmt.Errorf("unknown worker_resolution_strategy %q", c.WorkerResolutionStrategy)
	}
	if c.WorkerID != "" && c.WorkerResolutionStrategy != WorkerStrategyStatic {
		return fmt.Errorf("worker_id is only used with worker_resolution_strategy: static, not %q", c.WorkerResolutionStrategy)
	}
	if c.WorkerIDFile != "" && c.WorkerResolutionStrategy != WorkerStrategyFile {
		return fmt.Errorf("worker_id_file is only used with worker_resolution_strategy: file, not %q", c.WorkerResolutionStrategy)
	}
	if c.WorkerResolutionStrategy == WorkerStrategyFile && c.WorkerIDFile == "" {
		return errors.New("worker_id_file is required when worker_resolution_strategy=file")
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
