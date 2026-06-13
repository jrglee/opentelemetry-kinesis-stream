package encoding

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

// noneCodec is the identity compressor. It exists so call sites can stay
// branch-free: pick a Compressor at construction time and use it everywhere.
type noneCodec struct{}

func (noneCodec) Compress(in []byte) ([]byte, error)   { return in, nil }
func (noneCodec) Decompress(in []byte) ([]byte, error) { return in, nil }

// gzipCodec uses the stdlib gzip implementation. The PoC accepts the per-call
// writer/reader allocation cost; pooling lands when measurements show it matters.
type gzipCodec struct{}

func (gzipCodec) Compress(in []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(in); err != nil {
		return nil, fmt.Errorf("gzip write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("gzip close: %w", err)
	}
	return buf.Bytes(), nil
}

func (gzipCodec) Decompress(in []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		return nil, fmt.Errorf("gzip open: %w", err)
	}
	out, err := io.ReadAll(r)
	if cerr := r.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, fmt.Errorf("gzip read: %w", err)
	}
	return out, nil
}

// zstd encoder/decoder are safe for concurrent use and expensive to build, so a
// single shared instance of each is reused for every record. Built once; the
// nil-writer/reader EncodeAll/DecodeAll APIs do not stream and need no Reset.
var (
	zstdEnc *zstd.Encoder
	zstdDec *zstd.Decoder
)

func init() {
	var err error
	if zstdEnc, err = zstd.NewWriter(nil); err != nil {
		panic("zstd: build encoder: " + err.Error())
	}
	if zstdDec, err = zstd.NewReader(nil); err != nil {
		panic("zstd: build decoder: " + err.Error())
	}
}

// zstdCodec is RFC 8478 zstd via a shared pooled encoder/decoder.
type zstdCodec struct{}

func (zstdCodec) Compress(in []byte) ([]byte, error) {
	return zstdEnc.EncodeAll(in, make([]byte, 0, len(in)/3)), nil
}

func (zstdCodec) Decompress(in []byte) ([]byte, error) {
	out, err := zstdDec.DecodeAll(in, nil)
	if err != nil {
		return nil, fmt.Errorf("zstd decode: %w", err)
	}
	return out, nil
}

// snappyCodec uses the stateless Snappy block format (not the streaming frame
// format). Encode/Decode allocate their own destination when passed nil.
type snappyCodec struct{}

func (snappyCodec) Compress(in []byte) ([]byte, error) {
	return snappy.Encode(nil, in), nil
}

func (snappyCodec) Decompress(in []byte) ([]byte, error) {
	out, err := snappy.Decode(nil, in)
	if err != nil {
		return nil, fmt.Errorf("snappy decode: %w", err)
	}
	return out, nil
}
