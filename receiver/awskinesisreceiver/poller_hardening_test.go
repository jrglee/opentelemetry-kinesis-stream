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

// Dead-letter scenarios (TestDeadLetterEmitFailureRetries,
// TestDeadLetterDisabledSkips, deadLetterSink) live in
// poller_deadletter_test.go.

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

// TestPollPacingRespectsInterval pins the GetRecords cadence: the API allows
// five reads per second per shard, so consecutive polls — including ones that
// returned data — must be spaced at PollInterval, not issued back-to-back.
func TestPollPacingRespectsInterval(t *testing.T) {
	const shardID = "pace-shard"
	fs := &fakeStream{shards: []*fakeShard{{
		id:      shardID,
		records: [][]byte{[]byte("r0"), []byte("r1"), []byte("r2")},
	}}}
	store := lease.NewMemoryStore()
	cfg := fastCoordCfg("pace", encoding.EncodingOTLPProto, encoding.CodecNone)
	cfg.PollInterval = 150 * time.Millisecond
	p := newHardeningPoller(t, cfg, fakeKinesisClient(fs), store, noopSink{}, shardID)

	start := time.Now()
	runPollerToExit(t, p, 10*time.Second)
	elapsed := time.Since(start)

	if cp := shardCheckpoint(t, store, shardID); cp != lease.CheckpointShardEnd {
		t.Fatalf("final checkpoint: got %q want SHARD_END", cp)
	}
	// Four calls total (three data + the closing empty poll); the first is
	// immediate, the rest are paced: >= 3 intervals.
	if want := 3 * cfg.PollInterval; elapsed < want {
		t.Fatalf("drained in %v; paced polling requires at least %v", elapsed, want)
	}
}

// transientErrStore fails a chosen operation with a generic (non-conflict)
// error a fixed number of times, simulating a store blip.
type transientErrStore struct {
	lease.Store
	heartbeatFails  atomic.Int32
	checkpointFails atomic.Int32
}

func (s *transientErrStore) Heartbeat(ctx context.Context, l lease.Lease) (lease.Lease, error) {
	if s.heartbeatFails.Add(-1) >= 0 {
		return lease.Lease{}, errors.New("dynamodb 500: transient blip")
	}
	return s.Store.Heartbeat(ctx, l)
}

func (s *transientErrStore) Checkpoint(ctx context.Context, l lease.Lease, seq string) (lease.Lease, error) {
	if s.checkpointFails.Add(-1) >= 0 {
		return lease.Lease{}, errors.New("dynamodb 500: transient blip")
	}
	return s.Store.Checkpoint(ctx, l, seq)
}

// TestHeartbeatSurvivesTransientStoreError pins the blip semantics: a store
// error that is not a lease conflict must not tear down the poller — the
// next heartbeat tick retries. (Previously any error stopped the poller,
// so a shared DynamoDB blip triggered a fleet-wide reacquire storm.)
func TestHeartbeatSurvivesTransientStoreError(t *testing.T) {
	const shardID = "hb-blip-shard"
	fs := &fakeStream{shards: []*fakeShard{{id: shardID, records: [][]byte{[]byte("r0")}}}}
	store := &transientErrStore{Store: lease.NewMemoryStore()}
	store.heartbeatFails.Store(2)

	// Hold the record in consume until both failing heartbeats have fired, so
	// the blip provably happens while the poller is alive and mid-work.
	snk := &gateSink{gateOn: "r0", gateHit: make(chan struct{}), gateOpen: make(chan struct{})}
	cfg := fastCoordCfg("hb-blip", encoding.EncodingOTLPProto, encoding.CodecNone)
	cfg.HeartbeatInterval = 20 * time.Millisecond
	p := newHardeningPoller(t, cfg, fakeKinesisClient(fs), store, snk, shardID)

	done := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		defer close(done)
		p.run(ctx)
	}()

	<-snk.gateHit
	deadline := time.Now().Add(5 * time.Second)
	for store.heartbeatFails.Load() > 0 {
		if time.Now().After(deadline) {
			t.Fatal("failing heartbeats never fired")
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(snk.gateOpen)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("poller did not exit")
	}
	if cp := shardCheckpoint(t, store, shardID); cp != lease.CheckpointShardEnd {
		t.Fatalf("final checkpoint: got %q want SHARD_END (poller must survive heartbeat blips)", cp)
	}
}

// TestCheckpointRetriesTransientStoreError pins the in-place checkpoint retry:
// a transient failure is retried without abandoning the batch, so records are
// not re-read (no duplicates) and the shard still drains.
func TestCheckpointRetriesTransientStoreError(t *testing.T) {
	const shardID = "cp-blip-shard"
	fs := &fakeStream{shards: []*fakeShard{{id: shardID, records: [][]byte{[]byte("r0"), []byte("r1")}}}}
	store := &transientErrStore{Store: lease.NewMemoryStore()}
	store.checkpointFails.Store(1)

	snk := &gateSink{gateOn: "", gateHit: make(chan struct{}), gateOpen: make(chan struct{})}
	cfg := fastCoordCfg("cp-blip", encoding.EncodingOTLPProto, encoding.CodecNone)
	p := newHardeningPoller(t, cfg, fakeKinesisClient(fs), store, snk, shardID)

	runPollerToExit(t, p, 10*time.Second)

	if cp := shardCheckpoint(t, store, shardID); cp != lease.CheckpointShardEnd {
		t.Fatalf("final checkpoint: got %q want SHARD_END", cp)
	}
	seen := map[string]int{}
	for _, d := range snk.all() {
		seen[d]++
	}
	for _, want := range []string{"r0", "r1"} {
		if seen[want] != 1 {
			t.Fatalf("record %s delivered %d times; transient checkpoint retry must not re-read the batch", want, seen[want])
		}
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

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		p.run(ctx)
	}()

	// The record must land well before one PollInterval: the expired call is
	// re-opened and re-polled immediately, not slept through.
	deadline := time.Now().Add(2 * time.Second)
	for shardCheckpoint(t, store, shardID) != sequence(shardID, 0) {
		if time.Now().After(deadline) {
			t.Fatalf("record not delivered %v after start; re-open must poll immediately, not wait out PollInterval",
				time.Since(start))
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Regular pacing resumes after delivery; drain rather than waiting out the
	// 5s interval to closure detection.
	p.drain()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("poller did not exit after drain")
	}
}
