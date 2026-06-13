package awskinesisexporter

// Config is the configuration for the Kinesis exporter.
//
// Fields will land alongside the implementation. The surface needs to expose
// at minimum: the target stream identity and AWS region, the wire encoding
// and compression codec/level, the partition-key strategy, the microbatching
// triggers (record count and time window), and the policy for handling
// records that exceed the per-record size limit after compression.
type Config struct{}
