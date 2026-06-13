package encoding

// Codec names the wire-level compression algorithm applied to a marshaled
// telemetry payload before it is written into a Kinesis record.
type Codec string

const (
	// CodecNone is the identity codec: the payload is written uncompressed.
	CodecNone Codec = "none"
	// CodecGzip is RFC 1952 gzip.
	CodecGzip Codec = "gzip"
	// CodecZstd is RFC 8478 zstd.
	CodecZstd Codec = "zstd"
)

// Encoding names the wire-level marshaling format of the telemetry payload
// inside a Kinesis record.
type Encoding string

const (
	// EncodingOTLPProto is the OTLP protobuf encoding.
	EncodingOTLPProto Encoding = "otlp_proto"
	// EncodingOTLPJSON is the OTLP JSON encoding.
	EncodingOTLPJSON Encoding = "otlp_json"
	// EncodingOTelArrow is the OpenTelemetry Arrow encoding.
	EncodingOTelArrow Encoding = "otel_arrow"
)
