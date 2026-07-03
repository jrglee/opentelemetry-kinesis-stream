package awskinesisreceiver

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
	"github.com/jrglee/opentelemetry-kinesis-stream/internal/lease"
)

// deadLetterSink decode-fails every record and fails the first failuresBefore
// dead-letter emits, recording the shard's checkpoint at each emit attempt so
// the test can prove the checkpoint never advanced past an undelivered record.
type deadLetterSink struct {
	store   lease.Store
	shardID string

	mu             sync.Mutex
	failuresBefore int
	calls          int
	checkpoints    []string
}

func (s *deadLetterSink) consume(context.Context, []byte) (recordResult, bool) {
	return recordSkip, true // decode failure: dead-letter territory
}

func (s *deadLetterSink) deadLetter(ctx context.Context, _ ktypes.Record, _, _, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	leases, err := s.store.List(ctx)
	if err == nil {
		for _, l := range leases {
			if l.ShardID == s.shardID {
				s.checkpoints = append(s.checkpoints, l.Checkpoint)
			}
		}
	}
	if s.calls <= s.failuresBefore {
		return errors.New("dead-letter pipeline rejecting")
	}
	return nil
}

func (s *deadLetterSink) stats() (calls int, checkpoints []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, append([]string(nil), s.checkpoints...)
}

// TestDeadLetterEmitFailureRetries pins the loss-prevention semantics: while
// the dead-letter emit is failing, the checkpoint must not advance past the
// unprocessable record (it is re-read and re-attempted); once the emit
// succeeds the record is skipped and the shard drains.
func TestDeadLetterEmitFailureRetries(t *testing.T) {
	const shardID = "dl-shard"
	fs := &fakeStream{shards: []*fakeShard{{id: shardID, records: [][]byte{[]byte("garbage")}}}}
	store := lease.NewMemoryStore()
	snk := &deadLetterSink{store: store, shardID: shardID, failuresBefore: 2}

	cfg := fastCoordCfg("dl", encoding.EncodingOTLPProto, encoding.CodecNone)
	cfg.DeadLetter.Enabled = true
	p := newHardeningPoller(t, cfg, fakeKinesisClient(fs), store, snk, shardID)

	runPollerToExit(t, p, 10*time.Second)

	calls, checkpoints := snk.stats()
	if calls != 3 {
		t.Fatalf("dead-letter attempts: got %d want 3 (two failures, then success)", calls)
	}
	// At every emit attempt the record was still unclaimed by the checkpoint.
	for i, cp := range checkpoints {
		if cp != lease.CheckpointTrimHorizon && cp != "" {
			t.Fatalf("attempt %d: checkpoint advanced to %q before dead-letter was delivered", i+1, cp)
		}
	}
	if cp := shardCheckpoint(t, store, shardID); cp != lease.CheckpointShardEnd {
		t.Fatalf("final checkpoint: got %q want SHARD_END", cp)
	}
}

// TestDeadLetterDisabledSkips pins the disabled path: no emit attempts, the
// unprocessable record is skipped, and the shard drains.
func TestDeadLetterDisabledSkips(t *testing.T) {
	const shardID = "dl-off-shard"
	fs := &fakeStream{shards: []*fakeShard{{id: shardID, records: [][]byte{[]byte("garbage")}}}}
	store := lease.NewMemoryStore()
	snk := &deadLetterSink{store: store, shardID: shardID, failuresBefore: 99}

	cfg := fastCoordCfg("dl-off", encoding.EncodingOTLPProto, encoding.CodecNone)
	p := newHardeningPoller(t, cfg, fakeKinesisClient(fs), store, snk, shardID)

	runPollerToExit(t, p, 10*time.Second)

	if calls, _ := snk.stats(); calls != 0 {
		t.Fatalf("dead-letter attempts with dead_letter disabled: got %d want 0", calls)
	}
	if cp := shardCheckpoint(t, store, shardID); cp != lease.CheckpointShardEnd {
		t.Fatalf("final checkpoint: got %q want SHARD_END", cp)
	}
}
