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
	// MaxRecordSize caps the post-compression record payload in bytes; oversize
	// records are repacked per Oversize.Policy. This is an operator-owned limit:
	// the exporter enforces the value you set and asserts nothing about the
	// stream's actual ceiling, which varies by account, region, and stream
	// configuration. The default (1 MiB) is the conservative floor that every
	// stream accepts; raise it if your stream is configured for larger records.
	MaxRecordSize int `mapstructure:"max_record_size"`
	// PartitionKey controls how a record's Kinesis partition key is derived,
	// which in turn controls shard fan-out and tag-grouped microbatching.
	PartitionKey PartitionKeyConfig `mapstructure:"partition_key"`
	// Oversize controls how a batch whose compressed payload exceeds
	// MaxRecordSize is repacked into one or more fitting records.
	Oversize OversizeConfig `mapstructure:"oversize"`
	// PutRecords caps each PutRecords call. Like MaxRecordSize, these are
	// operator-owned limits the exporter enforces verbatim — it does not track
	// AWS's current per-call ceilings, which change over time and differ by
	// environment.
	PutRecords PutRecordsConfig `mapstructure:"put_records"`
}

// PutRecordsConfig bounds a single PutRecords call. The exporter chunks a flush
// so no call exceeds either limit. Defaults are the conservative values every
// Kinesis stream has historically accepted; raise them to match a stream
// configured for larger requests.
type PutRecordsConfig struct {
	// MaxRecords is the maximum number of records per PutRecords call.
	MaxRecords int `mapstructure:"max_records"`
	// MaxBytes is the maximum aggregate record-data bytes per PutRecords call.
	MaxBytes int `mapstructure:"max_bytes"`
}

// PartitionKeyConfig selects the partition-key strategy. "random" spreads
// records uniformly across shards; "tag_hash" co-locates records that share
// the same ordered resource-attribute tuple onto a stable key so a downstream
// consumer sees them in order and so per-tuple microbatching is possible.
type PartitionKeyConfig struct {
	// Strategy is "random" (default) or "tag_hash".
	Strategy string `mapstructure:"strategy"`
	// Tags is the ordered list of resource attribute keys hashed into the
	// partition key. Required and non-empty when Strategy is "tag_hash".
	Tags []string `mapstructure:"tags"`
	// Hash names the hash function for "tag_hash"; only "xxhash" (default).
	Hash string `mapstructure:"hash"`
}

// OversizeConfig is the recovery chain for a payload that exceeds MaxRecordSize.
//
// Policies are tried in order against the still-oversize compressed payload;
// the first that produces a fitting result wins. If the chain is exhausted
// the remainder is dropped and counted with a specific reason (see
// telemetry.go). The chain shape lets attribute-bloat strategies run before
// wasteful recursive splits, which is the common real-world failure mode.
type OversizeConfig struct {
	// LegacyPolicy is a tombstone for the removed singular "policy" field.
	// Validation errors if it is set so an upgraded config that still uses
	// the old key fails loud with a migration message rather than silently
	// reverting to the default.
	LegacyPolicy *string `mapstructure:"policy"`
	// Policies is the ordered recovery chain. Valid entries:
	//   - "truncate_attribute_values": clone the batch and clamp any string
	//     attribute value longer than MaxAttributeValueBytes. Targets the
	//     "single long tag value" failure mode that split_half cannot recover.
	//     Safe in any position because it always returns a (possibly
	//     unmodified) batch for the next policy to try.
	//   - "split_half": recursively halve resources, then spans/datapoints
	//     within a resource, until each piece fits or MaxAttempts is reached.
	//     Terminal — must appear last because its drops are not re-presentable
	//     to subsequent policies.
	//   - "reject": stop here; drop the remainder and count as reject_policy.
	//     Terminal — must appear last.
	// Default: ["split_half"].
	Policies []string `mapstructure:"policies"`
	// MaxAttempts bounds split_half recursion depth per chain step so a
	// pathological input cannot loop; an irreducible leaf falls through to the
	// next policy (or is dropped at chain exhaustion). Default 8.
	MaxAttempts int `mapstructure:"max_attempts"`
	// MaxAttributeValueBytes is the per-attribute UTF-8 byte ceiling enforced
	// by truncate_attribute_values. Values strictly longer than this are
	// truncated to a codepoint boundary at or before this length; values at
	// or under it are left alone. Non-string attribute kinds are never
	// touched. Default 4096.
	MaxAttributeValueBytes int `mapstructure:"max_attribute_value_bytes"`
}

const (
	partitionStrategyRandom  = "random"
	partitionStrategyTagHash = "tag_hash"
	hashXXHash               = "xxhash"
	oversizeSplitHalf        = "split_half"
	oversizeTruncateAttrs    = "truncate_attribute_values"
	oversizeReject           = "reject"
)

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
	if c.PutRecords.MaxRecords <= 0 {
		return errors.New("put_records.max_records must be positive")
	}
	if c.PutRecords.MaxBytes <= 0 {
		return errors.New("put_records.max_bytes must be positive")
	}
	if c.MaxRecordSize > c.PutRecords.MaxBytes {
		return errors.New("max_record_size must not exceed put_records.max_bytes")
	}
	switch c.PartitionKey.Strategy {
	case "", partitionStrategyRandom:
	case partitionStrategyTagHash:
		if len(c.PartitionKey.Tags) == 0 {
			return errors.New("partition_key.tags is required for strategy tag_hash")
		}
	default:
		return fmt.Errorf("unknown partition_key.strategy %q", c.PartitionKey.Strategy)
	}
	switch c.PartitionKey.Hash {
	case "", hashXXHash:
	default:
		return fmt.Errorf("unknown partition_key.hash %q", c.PartitionKey.Hash)
	}
	if c.Oversize.LegacyPolicy != nil {
		return fmt.Errorf(
			"oversize.policy is removed; rename it to oversize.policies (a list). Migration: oversize.policies: [%q]",
			*c.Oversize.LegacyPolicy,
		)
	}
	if len(c.Oversize.Policies) == 0 {
		return errors.New("oversize.policies must contain at least one policy")
	}
	for i, p := range c.Oversize.Policies {
		switch p {
		case oversizeTruncateAttrs:
			// truncate is the only safely-chainable policy: it returns a
			// (possibly unmodified) batch the next policy operates on.
		case oversizeSplitHalf, oversizeReject:
			// Terminal policies: their drops cannot be retried by a
			// subsequent policy, so listing anything after them is a
			// configuration trap.
			if i != len(c.Oversize.Policies)-1 {
				return fmt.Errorf(
					"oversize.policies[%d]=%q terminates the chain; policies listed after it would never run (move %q to the end or drop the trailing entries)",
					i, p, p,
				)
			}
		default:
			return fmt.Errorf("unknown oversize.policies entry %q", p)
		}
	}
	if c.Oversize.MaxAttempts <= 0 {
		return errors.New("oversize.max_attempts must be positive")
	}
	if c.Oversize.MaxAttributeValueBytes <= 0 {
		return errors.New("oversize.max_attribute_value_bytes must be positive")
	}
	return nil
}

// tagHash reports whether the resolved strategy is tag_hash.
func (c *Config) tagHash() bool {
	return c.PartitionKey.Strategy == partitionStrategyTagHash
}
