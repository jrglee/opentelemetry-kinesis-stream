package encoding

import (
	"bytes"
	"sync"
	"testing"
)

// TestCodecConcurrent guards the "safe for concurrent use" claim on the
// Compressor contract under -race. zstd is the one that matters: it shares a
// single pooled encoder/decoder across all callers, so a concurrency bug there
// would corrupt output; gzip and snappy are stateless but cheap to include.
func TestCodecConcurrent(t *testing.T) {
	const (
		goroutines = 32
		rounds     = 20
		payloadSz  = 64 * 1024
	)

	codecs := map[Codec]Compressor{
		CodecGzip:         gzipCodec{},
		CodecZstd:         zstdCodec{},
		CodecSnappy:       snappyCodec{},
		CodecSnappyFramed: snappyFramedCodec{},
		CodecZlib:         zlibCodec{},
		CodecDeflate:      deflateCodec{},
	}

	// Mixed-entropy deterministic payload. A constant byte would compress
	// implausibly well; the index stir keeps the codecs honest without breaking
	// reproducibility.
	base := bytes.Repeat([]byte("opentelemetry-kinesis-stream-"), payloadSz/29+1)[:payloadSz]
	mkPayload := func(seed int) []byte {
		out := make([]byte, payloadSz)
		copy(out, base)
		for i := 0; i < payloadSz; i += 17 {
			out[i] ^= byte(seed + i)
		}
		return out
	}

	for name, codec := range codecs {
		t.Run(string(name), func(t *testing.T) {
			var wg sync.WaitGroup
			wg.Add(goroutines)
			for g := 0; g < goroutines; g++ {
				go func(seed int) {
					defer wg.Done()
					payload := mkPayload(seed)
					for r := 0; r < rounds; r++ {
						zipped, err := codec.Compress(payload)
						if err != nil {
							t.Errorf("goroutine %d round %d: Compress: %v", seed, r, err)
							return
						}
						back, err := codec.Decompress(zipped)
						if err != nil {
							t.Errorf("goroutine %d round %d: Decompress: %v", seed, r, err)
							return
						}
						if !bytes.Equal(back, payload) {
							t.Errorf("goroutine %d round %d: round-trip mismatch", seed, r)
							return
						}
					}
				}(g)
			}
			wg.Wait()
		})
	}
}
