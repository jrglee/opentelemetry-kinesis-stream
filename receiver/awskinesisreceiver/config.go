package awskinesisreceiver

// Config is the configuration for the Kinesis receiver.
//
// Fields will land alongside the implementation. The surface needs to expose
// at minimum: the source stream identity and AWS region, the wire encoding
// and compression codec expected on records, the checkpoint backing store
// (DynamoDB for multi-replica deployments, filesystem-local or in-process for
// development), and the failure policy for decode errors, downstream
// rejection, and checkpoint write failures.
type Config struct{}
