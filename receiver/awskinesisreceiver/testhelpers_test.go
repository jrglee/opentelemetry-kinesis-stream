package awskinesisreceiver

import (
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
	"github.com/jrglee/opentelemetry-kinesis-stream/internal/lease"
)

// fastCoordCfg returns a Config tuned for tight, deterministic tests against
// the fakeStream — small poll intervals, sub-second lease timings,
// memory-only state. Centralized so adding a new required Config field
// updates one place instead of every coordinator-driving test.
func fastCoordCfg(streamName string, enc encoding.Encoding, codec encoding.Codec) *Config {
	return &Config{
		StreamName:        streamName,
		Encoding:          enc,
		Compression:       codec,
		PollInterval:      20 * time.Millisecond,
		MaxRecords:        100,
		LeaseDuration:     500 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		DiscoveryInterval: 40 * time.Millisecond,
	}
}

// newTestCoordinator wires a coordinator against the provided fake Kinesis
// client, sink, and config, with default in-memory lease state and a
// zaptest-backed logger. Replaces the ~25-line struct literal that
// reshard_test.go and matrix_e2e_test.go would otherwise each carry.
func newTestCoordinator(t *testing.T, cfg *Config, fs *fakeStream, snk sink, workerID string) *coordinator {
	t.Helper()
	comp, err := encoding.NewCompressor(cfg.Compression)
	if err != nil {
		t.Fatalf("NewCompressor(%s): %v", cfg.Compression, err)
	}
	return &coordinator{
		cfg:      cfg,
		client:   fakeKinesisClient(fs),
		store:    lease.NewMemoryStore(),
		comp:     comp,
		sink:     snk,
		logger:   zaptest.NewLogger(t),
		tel:      testTelemetry(t),
		workerID: workerID,
		active:   make(map[string]*activePoller),
		observed: make(map[string]observation),
		absent:   make(map[string]int),
	}
}
