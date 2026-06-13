package encoding

import (
	"fmt"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Codec names the wire-level compression algorithm applied to a marshaled
// telemetry payload before it is written into a Kinesis record.
type Codec string

const (
	// CodecNone is the identity codec: the payload is written uncompressed.
	CodecNone Codec = "none"
	// CodecGzip is RFC 1952 gzip.
	CodecGzip Codec = "gzip"
	// CodecZstd is RFC 8478 zstd. Not yet implemented in this PoC.
	CodecZstd Codec = "zstd"
)

// Encoding names the wire-level marshaling format of the telemetry payload
// inside a Kinesis record.
type Encoding string

const (
	// EncodingOTLPProto is the OTLP protobuf encoding.
	EncodingOTLPProto Encoding = "otlp_proto"
	// EncodingOTLPJSON is the OTLP JSON encoding. Not yet implemented in this PoC.
	EncodingOTLPJSON Encoding = "otlp_json"
	// EncodingOTelArrow is the OpenTelemetry Arrow encoding. Not yet implemented in this PoC.
	EncodingOTelArrow Encoding = "otel_arrow"
)

// TracesEncoder serializes a ptrace.Traces to its on-wire byte form. The
// returned slice is owned by the caller — implementations may reuse internal
// buffers between calls but must not retain a reference to the returned bytes.
type TracesEncoder interface {
	Marshal(td ptrace.Traces) ([]byte, error)
}

// TracesDecoder is the inverse of TracesEncoder. Input bytes are not retained.
type TracesDecoder interface {
	Unmarshal(buf []byte) (ptrace.Traces, error)
}

// Compressor wraps the two compression operations as a pair so call sites can
// stay symmetric. Implementations are required to be safe for concurrent use.
type Compressor interface {
	Compress(in []byte) ([]byte, error)
	Decompress(in []byte) ([]byte, error)
}

// NewTracesEncoder returns the traces encoder for the named wire encoding.
// Unknown or not-yet-implemented encodings return an error so configuration
// validation fails fast rather than at first record.
func NewTracesEncoder(e Encoding) (TracesEncoder, error) {
	switch e {
	case EncodingOTLPProto:
		return otlpProtoTraces{}, nil
	case EncodingOTLPJSON, EncodingOTelArrow:
		return nil, fmt.Errorf("encoding %q is not implemented in this PoC", e)
	default:
		return nil, fmt.Errorf("unknown encoding %q", e)
	}
}

// NewTracesDecoder is the symmetric counterpart of NewTracesEncoder.
func NewTracesDecoder(e Encoding) (TracesDecoder, error) {
	switch e {
	case EncodingOTLPProto:
		return otlpProtoTraces{}, nil
	case EncodingOTLPJSON, EncodingOTelArrow:
		return nil, fmt.Errorf("encoding %q is not implemented in this PoC", e)
	default:
		return nil, fmt.Errorf("unknown encoding %q", e)
	}
}

// NewCompressor returns the compressor for the named codec.
func NewCompressor(c Codec) (Compressor, error) {
	switch c {
	case CodecNone:
		return noneCodec{}, nil
	case CodecGzip:
		return gzipCodec{}, nil
	case CodecZstd:
		return nil, fmt.Errorf("codec %q is not implemented in this PoC", c)
	default:
		return nil, fmt.Errorf("unknown codec %q", c)
	}
}
