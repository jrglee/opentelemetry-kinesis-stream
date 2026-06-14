package encoding

import (
	"fmt"

	"go.opentelemetry.io/collector/pdata/pmetric"
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
	// CodecZstd is RFC 8478 zstd.
	CodecZstd Codec = "zstd"
	// CodecSnappy is the Snappy block format.
	CodecSnappy Codec = "snappy"
	// CodecSnappyFramed is the Snappy stream (framing) format, matching the
	// collector's "x-snappy-framed".
	CodecSnappyFramed Codec = "x-snappy-framed"
	// CodecZlib is RFC 1950 zlib.
	CodecZlib Codec = "zlib"
	// CodecDeflate is the raw RFC 1951 DEFLATE stream.
	CodecDeflate Codec = "deflate"
)

// Encoding names the wire-level marshaling format of the telemetry payload
// inside a Kinesis record.
type Encoding string

const (
	// EncodingOTLPProto is the OTLP protobuf encoding.
	EncodingOTLPProto Encoding = "otlp_proto"
	// EncodingOTLPJSON is the OTLP JSON encoding.
	EncodingOTLPJSON Encoding = "otlp_json"
	// EncodingOTelArrow is the OpenTelemetry Arrow encoding. Each Kinesis
	// record carries a fully self-contained Arrow batch (fresh producer per
	// Marshal, fresh consumer per Unmarshal) so that any single record is
	// decodable in isolation, which is required by Kinesis's store-and-forward
	// delivery model. Cross-batch dictionary-delta compression is forfeited as
	// a deliberate trade-off; see ADR-0018.
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

// MetricsEncoder and MetricsDecoder are the metrics counterparts of the traces
// pair. The same Codec compresses either signal's bytes; only the marshaling
// differs.
type MetricsEncoder interface {
	Marshal(md pmetric.Metrics) ([]byte, error)
}

// MetricsDecoder is the inverse of MetricsEncoder.
type MetricsDecoder interface {
	Unmarshal(buf []byte) (pmetric.Metrics, error)
}

// Compressor wraps the two compression operations as a pair so call sites can
// stay symmetric. Implementations are required to be safe for concurrent use.
type Compressor interface {
	Compress(in []byte) ([]byte, error)
	Decompress(in []byte) ([]byte, error)
}

// NewTracesEncoder returns the traces encoder for the named wire encoding.
// Unknown encodings return an error so configuration validation fails fast
// rather than at first record.
func NewTracesEncoder(e Encoding) (TracesEncoder, error) {
	switch e {
	case EncodingOTLPProto:
		return otlpProtoTraces{}, nil
	case EncodingOTLPJSON:
		return otlpJSONTraces{}, nil
	case EncodingOTelArrow:
		return otlpArrowTraces{}, nil
	default:
		return nil, fmt.Errorf("unknown encoding %q", e)
	}
}

// NewTracesDecoder is the symmetric counterpart of NewTracesEncoder.
func NewTracesDecoder(e Encoding) (TracesDecoder, error) {
	switch e {
	case EncodingOTLPProto:
		return otlpProtoTraces{}, nil
	case EncodingOTLPJSON:
		return otlpJSONTraces{}, nil
	case EncodingOTelArrow:
		return otlpArrowTraces{}, nil
	default:
		return nil, fmt.Errorf("unknown encoding %q", e)
	}
}

// NewMetricsEncoder returns the metrics encoder for the named wire encoding.
func NewMetricsEncoder(e Encoding) (MetricsEncoder, error) {
	switch e {
	case EncodingOTLPProto:
		return otlpProtoMetrics{}, nil
	case EncodingOTLPJSON:
		return otlpJSONMetrics{}, nil
	case EncodingOTelArrow:
		return otlpArrowMetrics{}, nil
	default:
		return nil, fmt.Errorf("unknown encoding %q", e)
	}
}

// NewMetricsDecoder is the symmetric counterpart of NewMetricsEncoder.
func NewMetricsDecoder(e Encoding) (MetricsDecoder, error) {
	switch e {
	case EncodingOTLPProto:
		return otlpProtoMetrics{}, nil
	case EncodingOTLPJSON:
		return otlpJSONMetrics{}, nil
	case EncodingOTelArrow:
		return otlpArrowMetrics{}, nil
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
		return zstdCodec{}, nil
	case CodecSnappy:
		return snappyCodec{}, nil
	case CodecSnappyFramed:
		return snappyFramedCodec{}, nil
	case CodecZlib:
		return zlibCodec{}, nil
	case CodecDeflate:
		return deflateCodec{}, nil
	default:
		return nil, fmt.Errorf("unknown codec %q", c)
	}
}
