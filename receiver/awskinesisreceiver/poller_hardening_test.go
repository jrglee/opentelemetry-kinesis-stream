package awskinesisreceiver

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/smithy-go/middleware"
	"go.uber.org/zap/zaptest"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
	"github.com/jrglee/opentelemetry-kinesis-stream/internal/lease"
)

// newHardeningPoller wires a shardPoller directly against the given client,
// store, and sink with an acquired lease — the same construction the
// heartbeat-loss test uses, centralized for this file's scenarios.
func newHardeningPoller(t *testing.T, cfg *Config, client *kinesis.Client, store lease.Store, snk sink, shardID string) *shardPoller {
	t.Helper()
	ctx := context.Background()
	if err := store.Ensure(ctx, shardID, nil); err != nil {
		t.Fatal(err)
	}
	taken, err := store.Acquire(ctx, shardID, "hardening-worker", 0)
	if err != nil {
		t.Fatal(err)
	}
	comp, err := encoding.NewCompressor(cfg.Compression)
	if err != nil {
		t.Fatal(err)
	}
	return &shardPoller{
		cfg:     cfg,
		client:  client,
		store:   store,
		comp:    comp,
		sink:    snk,
		logger:  zaptest.NewLogger(t),
		tel:     testTelemetry(t),
		leased:  taken,
		drainCh: make(chan struct{}),
	}
}

// runPollerToExit runs the poller and fails the test if it does not exit
// within the deadline.
func runPollerToExit(t *testing.T, p *shardPoller, deadline time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.run(ctx)
	}()
	select {
	case <-done:
	case <-time.After(deadline + time.Second):
		t.Fatal("poller did not exit")
	}
}

// shardCheckpoint reads the current checkpoint for a shard from the store.
func shardCheckpoint(t *testing.T, store lease.Store, shardID string) string {
	t.Helper()
	leases, err := store.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range leases {
		if l.ShardID == shardID {
			return l.Checkpoint
		}
	}
	t.Fatalf("shard %s not in lease table", shardID)
	return ""
}

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

// blockingReleaseStore blocks Release until its context is done, simulating a
// hung DynamoDB call on the exit path.
type blockingReleaseStore struct {
	lease.Store
}

func (s *blockingReleaseStore) Release(ctx context.Context, _ lease.Lease) error {
	<-ctx.Done()
	return ctx.Err()
}

// TestShutdownDeadlineAbortsRelease pins the kill-context contract: once the
// receiver's shutdown deadline has fired (killCtx cancelled), a hung store
// Release must abort immediately instead of consuming its full releaseTimeout.
func TestShutdownDeadlineAbortsRelease(t *testing.T) {
	const shardID = "kill-shard"
	store := &blockingReleaseStore{Store: lease.NewMemoryStore()}
	fs := &fakeStream{shards: []*fakeShard{{id: shardID}}}
	cfg := fastCoordCfg("kill", encoding.EncodingOTLPProto, encoding.CodecNone)
	p := newHardeningPoller(t, cfg, fakeKinesisClient(fs), store, noopSink{}, shardID)

	killCtx, killCancel := context.WithCancel(context.Background())
	killCancel() // the shutdown deadline already fired
	p.killCtx = killCtx

	start := time.Now()
	p.release()
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("release blocked %v after kill; want immediate abort (releaseTimeout is %v)", elapsed, releaseTimeout)
	}
}

// gateSink delivers every payload but blocks on one designated payload until
// released, signalling when the gate is reached — the hook that lets a test
// steal the lease while a record is mid-consume.
type gateSink struct {
	gateOn   string
	gateHit  chan struct{}
	gateOpen chan struct{}
	hitOnce  sync.Once

	mu        sync.Mutex
	delivered []string
}

func (s *gateSink) consume(ctx context.Context, payload []byte) (recordResult, bool) {
	if string(payload) == s.gateOn {
		s.hitOnce.Do(func() { close(s.gateHit) })
		select {
		case <-s.gateOpen:
		case <-ctx.Done():
			return recordRetry, false
		}
	}
	s.mu.Lock()
	s.delivered = append(s.delivered, string(payload))
	s.mu.Unlock()
	return recordOK, false
}

func (s *gateSink) deadLetter(context.Context, ktypes.Record, string, string, string) error {
	return nil
}

func (s *gateSink) all() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.delivered...)
}

// TestStealMidPollAtLeastOnce documents the duplicate-delivery window when a
// lease is stolen while the old owner is mid-consume: the stolen-from poller's
// checkpoint write loses the fencing race and it stops; the thief resumes from
// the last persisted checkpoint, re-delivering only the uncheckpointed window.
// Every record is delivered at least once; the duplicate set is exactly the
// window between the last checkpoint and the steal.
func TestStealMidPollAtLeastOnce(t *testing.T) {
	const shardID = "steal-shard"
	payloads := [][]byte{[]byte("r0"), []byte("r1"), []byte("r2"), []byte("r3"), []byte("r4")}
	fs := &fakeStream{shards: []*fakeShard{{id: shardID, records: payloads}}}
	store := lease.NewMemoryStore()

	sinkA := &gateSink{gateOn: "r2", gateHit: make(chan struct{}), gateOpen: make(chan struct{})}
	cfg := fastCoordCfg("steal", encoding.EncodingOTLPProto, encoding.CodecNone)
	// Keep A's heartbeat from racing the steal: heartbeats stop mattering once
	// the counter moves, but a long interval makes the sequence deterministic.
	cfg.HeartbeatInterval = time.Hour
	pA := newHardeningPoller(t, cfg, fakeKinesisClient(fs), store, sinkA, shardID)

	ctxA, cancelA := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelA()
	doneA := make(chan struct{})
	go func() {
		defer close(doneA)
		pA.run(ctxA)
	}()

	// Wait until A is blocked mid-consume on r2 (r0 and r1 checkpointed).
	select {
	case <-sinkA.gateHit:
	case <-time.After(5 * time.Second):
		t.Fatal("poller A never reached the gated record")
	}

	// Steal the lease while r2 is in flight.
	leases, err := store.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	stolen, err := store.Acquire(context.Background(), shardID, "worker-b", leases[0].Counter)
	if err != nil {
		t.Fatalf("steal: %v", err)
	}

	// Release A: it finishes delivering r2, then its checkpoint loses the
	// fencing race and the poller stops without advancing.
	close(sinkA.gateOpen)
	select {
	case <-doneA:
	case <-time.After(5 * time.Second):
		t.Fatal("poller A did not stop after losing the lease")
	}

	// B resumes from the last persisted checkpoint (r1).
	sinkB := &gateSink{gateOn: "", gateHit: make(chan struct{}), gateOpen: make(chan struct{})}
	comp, err := encoding.NewCompressor(cfg.Compression)
	if err != nil {
		t.Fatal(err)
	}
	pB := &shardPoller{
		cfg:     cfg,
		client:  fakeKinesisClient(fs),
		store:   store,
		comp:    comp,
		sink:    sinkB,
		logger:  zaptest.NewLogger(t),
		tel:     testTelemetry(t),
		leased:  stolen,
		drainCh: make(chan struct{}),
	}
	runPollerToExit(t, pB, 10*time.Second)

	if cp := shardCheckpoint(t, store, shardID); cp != lease.CheckpointShardEnd {
		t.Fatalf("final checkpoint: got %q want SHARD_END", cp)
	}

	// At-least-once: the union covers every record.
	seen := map[string]int{}
	for _, p := range append(sinkA.all(), sinkB.all()...) {
		seen[p]++
	}
	for _, p := range payloads {
		if seen[string(p)] == 0 {
			t.Fatalf("record %s lost across the steal", p)
		}
	}
	// Duplicates confined to the uncheckpointed window (r2 only): r0/r1 were
	// checkpointed before the steal, r3/r4 were only ever read by B.
	for p, n := range seen {
		if p == "r2" {
			if n != 2 {
				t.Fatalf("r2 delivered %d times; want exactly 2 (once per owner)", n)
			}
			continue
		}
		if n != 1 {
			t.Fatalf("record %s delivered %d times; only the in-flight record may duplicate", p, n)
		}
	}
}

// expireOnceStream serves the wrapped fakeStream but fails the first
// GetRecords with ExpiredIteratorException.
type expireOnceStream struct {
	fs      *fakeStream
	expired atomic.Bool
}

func (s *expireOnceStream) handle(
	ctx context.Context,
	in middleware.InitializeInput,
	next middleware.InitializeHandler,
) (middleware.InitializeOutput, middleware.Metadata, error) {
	if _, ok := in.Parameters.(*kinesis.GetRecordsInput); ok && s.expired.CompareAndSwap(false, true) {
		return middleware.InitializeOutput{}, middleware.Metadata{},
			&ktypes.ExpiredIteratorException{Message: aws.String("iterator expired")}
	}
	return s.fs.handle(ctx, in, next)
}

// TestExpiredIteratorReopensImmediately pins the re-open cadence: after a
// successful iterator re-open the poller polls again immediately instead of
// sleeping a full PollInterval on top of the expiry stall. The interval here
// is deliberately long so the old sleep-after-reopen behavior would blow the
// deadline.
func TestExpiredIteratorReopensImmediately(t *testing.T) {
	const shardID = "exp-shard"
	fs := &fakeStream{shards: []*fakeShard{{id: shardID, records: [][]byte{[]byte("r0")}}}}
	es := &expireOnceStream{fs: fs}
	client := kinesis.New(kinesis.Options{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
		APIOptions: []func(*middleware.Stack) error{
			func(stack *middleware.Stack) error {
				return stack.Initialize.Add(
					middleware.InitializeMiddlewareFunc("expireOnce", es.handle),
					middleware.Before,
				)
			},
		},
	})

	store := lease.NewMemoryStore()
	cfg := fastCoordCfg("exp", encoding.EncodingOTLPProto, encoding.CodecNone)
	cfg.PollInterval = 5 * time.Second // sleeping this after re-open = bug
	p := newHardeningPoller(t, cfg, client, store, noopSink{}, shardID)

	start := time.Now()
	runPollerToExit(t, p, 10*time.Second)
	elapsed := time.Since(start)

	if cp := shardCheckpoint(t, store, shardID); cp != lease.CheckpointShardEnd {
		t.Fatalf("final checkpoint: got %q want SHARD_END", cp)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("drain took %v; re-open must poll immediately, not wait out PollInterval", elapsed)
	}
}
