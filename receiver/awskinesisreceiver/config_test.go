package awskinesisreceiver

import (
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/confmap"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// TestDefaultConfigWorkerStrategy pins the default: the static strategy, which
// reproduces the historical worker_id-or-random-UUID behavior.
func TestDefaultConfigWorkerStrategy(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	if cfg.WorkerResolutionStrategy != WorkerStrategyStatic {
		t.Fatalf("default worker_resolution_strategy: got %q want %q", cfg.WorkerResolutionStrategy, WorkerStrategyStatic)
	}
}

// TestConfigUnmarshalsWorkerFields proves the new keys reach their fields.
func TestConfigUnmarshalsWorkerFields(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	conf := confmap.NewFromStringMap(map[string]any{
		"stream_name":                "s",
		"region":                     "us-east-1",
		"worker_resolution_strategy": "file",
		"worker_id_file":             "/var/run/worker-id",
	})
	if err := conf.Unmarshal(cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if cfg.WorkerResolutionStrategy != WorkerStrategyFile {
		t.Fatalf("worker_resolution_strategy: got %q want file", cfg.WorkerResolutionStrategy)
	}
	if cfg.WorkerIDFile != "/var/run/worker-id" {
		t.Fatalf("worker_id_file: got %q", cfg.WorkerIDFile)
	}
}

// baseValidCfg is the minimal Config that passes Validate. Each table case
// mutates one field to exercise its rule in isolation.
func baseValidCfg() *Config {
	return &Config{
		StreamName:               "s",
		Region:                   "us-east-1",
		Encoding:                 encoding.EncodingOTLPProto,
		Compression:              encoding.CodecNone,
		PollInterval:             250 * time.Millisecond,
		MaxRecords:               10000,
		WorkerResolutionStrategy: WorkerStrategyStatic,
		LeaseBackend:             LeaseBackendMemory,
		LeaseDuration:            30 * time.Second,
		HeartbeatInterval:        5 * time.Second,
		DiscoveryInterval:        30 * time.Second,
	}
}

func TestValidateWorkerStrategy(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(c *Config)
		wantErr  string // substring; empty means must succeed
		wantPass bool
	}{
		{
			name:     "default static passes",
			mutate:   func(_ *Config) {},
			wantPass: true,
		},
		{
			name: "static with explicit worker_id passes",
			mutate: func(c *Config) {
				c.WorkerID = "replica-a"
			},
			wantPass: true,
		},
		{
			name: "hostname passes",
			mutate: func(c *Config) {
				c.WorkerResolutionStrategy = WorkerStrategyHostname
			},
			wantPass: true,
		},
		{
			name: "ecs passes",
			mutate: func(c *Config) {
				c.WorkerResolutionStrategy = WorkerStrategyECS
			},
			wantPass: true,
		},
		{
			name: "file with path passes",
			mutate: func(c *Config) {
				c.WorkerResolutionStrategy = WorkerStrategyFile
				c.WorkerIDFile = "/var/run/worker-id"
			},
			wantPass: true,
		},
		{
			name: "empty strategy is rejected",
			mutate: func(c *Config) {
				c.WorkerResolutionStrategy = ""
			},
			wantErr: `unknown worker_resolution_strategy ""`,
		},
		{
			name: "unknown strategy is rejected",
			mutate: func(c *Config) {
				c.WorkerResolutionStrategy = "magic"
			},
			wantErr: `unknown worker_resolution_strategy "magic"`,
		},
		{
			name: "file without a path is rejected",
			mutate: func(c *Config) {
				c.WorkerResolutionStrategy = WorkerStrategyFile
			},
			wantErr: "worker_id_file is required when worker_resolution_strategy=file",
		},
		{
			name: "worker_id with hostname strategy is rejected",
			mutate: func(c *Config) {
				c.WorkerResolutionStrategy = WorkerStrategyHostname
				c.WorkerID = "replica-a"
			},
			wantErr: "worker_id is only used with worker_resolution_strategy: static",
		},
		{
			name: "worker_id with ecs strategy is rejected",
			mutate: func(c *Config) {
				c.WorkerResolutionStrategy = WorkerStrategyECS
				c.WorkerID = "replica-a"
			},
			wantErr: "worker_id is only used with worker_resolution_strategy: static",
		},
		{
			name: "worker_id with file strategy is rejected",
			mutate: func(c *Config) {
				c.WorkerResolutionStrategy = WorkerStrategyFile
				c.WorkerIDFile = "/var/run/worker-id"
				c.WorkerID = "replica-a"
			},
			wantErr: "worker_id is only used with worker_resolution_strategy: static",
		},
		{
			name: "worker_id_file with a non-file strategy is rejected",
			mutate: func(c *Config) {
				c.WorkerIDFile = "/var/run/worker-id"
			},
			wantErr: "worker_id_file is only used with worker_resolution_strategy: file",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseValidCfg()
			tc.mutate(cfg)
			err := cfg.Validate()
			if tc.wantPass {
				if err != nil {
					t.Fatalf("expected pass, got: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error mismatch: got %q want substring %q", err.Error(), tc.wantErr)
			}
		})
	}
}
