// Package encoding defines the wire surface shared by the Kinesis exporter
// and receiver.
//
// A Kinesis record carries a payload that is the compression of the
// marshaling of OpenTelemetry telemetry. Encoding (how telemetry is
// marshaled) and compression (how the marshaled bytes are compressed) are
// the two axes the wire commits to. The two ends must agree on both, by
// configuration; the wire itself is headerless.
//
// This package holds the names and interfaces for those axes. Implementations
// land alongside the components that drive them; the names here are the
// commitment.
package encoding
