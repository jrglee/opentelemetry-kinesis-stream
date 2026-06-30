package awskinesisexporter

import (
	"strings"
	"testing"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// baseValidCfg is the minimal Config that passes Validate. Each table case
// mutates one field to exercise its rule in isolation.
func baseValidCfg() *Config {
	return &Config{
		StreamName:    "s",
		Region:        "us-east-1",
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecNone,
		MaxRecordSize: 1 << 20,
		PartitionKey:  PartitionKeyConfig{Strategy: partitionStrategyRandom, Hash: hashXXHash},
		Oversize: OversizeConfig{
			Policies:               []string{oversizeSplitHalf},
			MaxAttempts:            8,
			MaxAttributeValueBytes: 4096,
		},
		PutRecords: PutRecordsConfig{MaxRecords: 500, MaxBytes: 5 << 20},
	}
}

func TestValidatePartitionKey(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(c *Config)
		wantErr  string // substring; empty means must succeed
		wantPass bool
	}{
		{
			name: "tags only (shorthand) still valid",
			mutate: func(c *Config) {
				c.PartitionKey = PartitionKeyConfig{
					Strategy: partitionStrategyTagHash,
					Tags:     []string{"service.name"},
					Hash:     hashXXHash,
				}
			},
			wantPass: true,
		},
		{
			name: "keys list with resource, datapoint, metric_name, regex, and promote",
			mutate: func(c *Config) {
				c.PartitionKey = PartitionKeyConfig{
					Strategy: partitionStrategyTagHash,
					Keys: []PartitionKeySource{
						{Source: keySourceResource, Name: "service.name"},
						{Source: keySourceDatapoint, Name: "http.method", Regex: `^(GET|POST)`, Promote: "derived.method"},
						{Source: keySourceMetricName},
					},
					Hash: hashXXHash,
				}
			},
			wantPass: true,
		},
		{
			name: "keys source empty defaults to resource (name required)",
			mutate: func(c *Config) {
				c.PartitionKey = PartitionKeyConfig{
					Strategy: partitionStrategyTagHash,
					Keys:     []PartitionKeySource{{Name: "host.name"}},
					Hash:     hashXXHash,
				}
			},
			wantPass: true,
		},
		{
			name: "both tags and keys set is an error",
			mutate: func(c *Config) {
				c.PartitionKey = PartitionKeyConfig{
					Strategy: partitionStrategyTagHash,
					Tags:     []string{"service.name"},
					Keys:     []PartitionKeySource{{Source: keySourceResource, Name: "host.name"}},
					Hash:     hashXXHash,
				}
			},
			wantErr: "partition_key.tags and partition_key.keys are mutually exclusive",
		},
		{
			name: "neither tags nor keys set is an error",
			mutate: func(c *Config) {
				c.PartitionKey = PartitionKeyConfig{
					Strategy: partitionStrategyTagHash,
					Hash:     hashXXHash,
				}
			},
			wantErr: "partition_key.tags or partition_key.keys is required for strategy tag_hash",
		},
		{
			name: "unknown source value",
			mutate: func(c *Config) {
				c.PartitionKey = PartitionKeyConfig{
					Strategy: partitionStrategyTagHash,
					Keys:     []PartitionKeySource{{Source: "span", Name: "foo"}},
					Hash:     hashXXHash,
				}
			},
			wantErr: `partition_key.keys[0]: unknown source "span"`,
		},
		{
			name: "missing name on resource source",
			mutate: func(c *Config) {
				c.PartitionKey = PartitionKeyConfig{
					Strategy: partitionStrategyTagHash,
					Keys:     []PartitionKeySource{{Source: keySourceResource}},
					Hash:     hashXXHash,
				}
			},
			wantErr: "partition_key.keys[0]: name is required for source",
		},
		{
			name: "missing name on datapoint source",
			mutate: func(c *Config) {
				c.PartitionKey = PartitionKeyConfig{
					Strategy: partitionStrategyTagHash,
					Keys:     []PartitionKeySource{{Source: keySourceDatapoint}},
					Hash:     hashXXHash,
				}
			},
			wantErr: "partition_key.keys[0]: name is required for source",
		},
		{
			name: "name set on metric_name source",
			mutate: func(c *Config) {
				c.PartitionKey = PartitionKeyConfig{
					Strategy: partitionStrategyTagHash,
					Keys:     []PartitionKeySource{{Source: keySourceMetricName, Name: "foo"}},
					Hash:     hashXXHash,
				}
			},
			wantErr: "partition_key.keys[0]: name must be empty for source",
		},
		{
			name: "bad regex won't compile",
			mutate: func(c *Config) {
				c.PartitionKey = PartitionKeyConfig{
					Strategy: partitionStrategyTagHash,
					Keys:     []PartitionKeySource{{Source: keySourceResource, Name: "service.name", Regex: `[invalid`}},
					Hash:     hashXXHash,
				}
			},
			wantErr: "partition_key.keys[0]: invalid regex",
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

func TestValidateOversizePolicies(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(c *Config)
		wantErr  string // substring; empty means must succeed
		wantPass bool
	}{
		{
			name:     "default split_half only",
			mutate:   func(_ *Config) {},
			wantPass: true,
		},
		{
			name: "chain truncate then split",
			mutate: func(c *Config) {
				c.Oversize.Policies = []string{oversizeTruncateAttrs, oversizeSplitHalf}
			},
			wantPass: true,
		},
		{
			name: "reject alone",
			mutate: func(c *Config) {
				c.Oversize.Policies = []string{oversizeReject}
			},
			wantPass: true,
		},
		{
			name: "empty policies",
			mutate: func(c *Config) {
				c.Oversize.Policies = nil
			},
			wantErr: "oversize.policies must contain at least one policy",
		},
		{
			name: "drop_largest is rejected",
			mutate: func(c *Config) {
				c.Oversize.Policies = []string{"drop_largest"}
			},
			wantErr: `unknown oversize.policies entry "drop_largest"`,
		},
		{
			name: "unknown policy is rejected",
			mutate: func(c *Config) {
				c.Oversize.Policies = []string{"yeet"}
			},
			wantErr: `unknown oversize.policies entry "yeet"`,
		},
		{
			name: "split_half not last is rejected",
			mutate: func(c *Config) {
				c.Oversize.Policies = []string{oversizeSplitHalf, oversizeTruncateAttrs}
			},
			wantErr: `terminates the chain`,
		},
		{
			name: "reject not last is rejected",
			mutate: func(c *Config) {
				c.Oversize.Policies = []string{oversizeReject, oversizeSplitHalf}
			},
			wantErr: `terminates the chain`,
		},
		{
			name: "split_half last after truncate is valid",
			mutate: func(c *Config) {
				c.Oversize.Policies = []string{oversizeTruncateAttrs, oversizeSplitHalf}
			},
			wantPass: true,
		},
		{
			name: "legacy oversize.policy field is rejected with migration message",
			mutate: func(c *Config) {
				old := "split_half"
				c.Oversize.LegacyPolicy = &old
			},
			wantErr: `oversize.policy is removed; rename it to oversize.policies`,
		},
		{
			name: "max_attempts must be positive",
			mutate: func(c *Config) {
				c.Oversize.MaxAttempts = 0
			},
			wantErr: "oversize.max_attempts must be positive",
		},
		{
			name: "max_attribute_value_bytes must be positive",
			mutate: func(c *Config) {
				c.Oversize.MaxAttributeValueBytes = 0
			},
			wantErr: "oversize.max_attribute_value_bytes must be positive",
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
