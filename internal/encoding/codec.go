package encoding

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
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
