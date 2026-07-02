package awskinesisexporter

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/smithy-go/middleware"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/ptrace"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// The unit tests here drive the real *kinesis.Client end-to-end through the
// SDK's middleware stack, short-circuiting at the Finalize step with a
// fabricated typed result or error. Finalize is the last step before the
// request would be handed to the HTTP transport, so the serialization path
// (input validation, operation modeling) still runs — we just never hit the
// network. This matches the testing strategy in ADR-0004.

type fakeResult struct {
	out  *kinesis.PutRecordsOutput
	err  error
	hits *atomic.Int32
}

func injectFake(r fakeResult) func(*kinesis.Options) {
	return func(o *kinesis.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Finalize.Add(
				middleware.FinalizeMiddlewareFunc("fakeKinesis", func(_ context.Context, _ middleware.FinalizeInput, _ middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
					if r.hits != nil {
						r.hits.Add(1)
					}
					if r.err != nil {
						return middleware.FinalizeOutput{}, middleware.Metadata{}, r.err
					}
					return middleware.FinalizeOutput{Result: r.out}, middleware.Metadata{}, nil
				}),
				middleware.Before,
			)
		})
	}
}

func newTestExporter(t *testing.T, maxRecordSize int, inject func(*kinesis.Options)) *kinesisExporter {
	t.Helper()
	return newTestExporterCfg(t, &Config{
		StreamName:    "test-stream",
		Region:        "us-east-1",
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecNone,
		MaxRecordSize: maxRecordSize,
		PartitionKey:  PartitionKeyConfig{Strategy: partitionStrategyRandom, Hash: hashXXHash},
		Oversize:      OversizeConfig{Policies: []string{oversizeSplitHalf}, MaxAttempts: 8, MaxAttributeValueBytes: 4096},
	}, inject)
}

// withFastBackoff shrinks the PutRecords retry backoff to near-zero for the
// duration of a test and returns a restore func to defer.
func withFastBackoff() func() {
	prevBase, prevMax := putBackoffBase, putBackoffMax
	putBackoffBase, putBackoffMax = time.Microsecond, time.Microsecond
	return func() { putBackoffBase, putBackoffMax = prevBase, prevMax }
}

// newTestExporterCfg builds an exporter wired to a Smithy-faked Kinesis client
// for an arbitrary config, with no-op logging/metering so tests stay hermetic.
func newTestExporterCfg(t *testing.T, cfg *Config, inject func(*kinesis.Options)) *kinesisExporter {
	t.Helper()
	// Backfill the per-call limits the factory would default, so tests need not
	// set them and flush always makes progress.
	if cfg.PutRecords.MaxRecords == 0 {
		cfg.PutRecords.MaxRecords = 500
	}
	if cfg.PutRecords.MaxBytes == 0 {
		cfg.PutRecords.MaxBytes = 5 << 20
	}
	tEnc, err := encoding.NewTracesEncoder(cfg.Encoding)
	if err != nil {
		t.Fatalf("traces encoder: %v", err)
	}
	mEnc, err := encoding.NewMetricsEncoder(cfg.Encoding)
	if err != nil {
		t.Fatalf("metrics encoder: %v", err)
	}
	lEnc, err := encoding.NewLogsEncoder(cfg.Encoding)
	if err != nil {
		t.Fatalf("logs encoder: %v", err)
	}
	comp, err := encoding.NewCompressor(cfg.Compression)
	if err != nil {
		t.Fatalf("compressor: %v", err)
	}
	// Static credentials and a fixed region keep the SDK from reaching out to
	// the default credential chain or IMDS during tests.
	client := kinesis.New(kinesis.Options{
		Region:      cfg.Region,
		Credentials: aws.AnonymousCredentials{},
	}, inject)
	tel, err := newExporterTelemetry(noopmetric.NewMeterProvider())
	if err != nil {
		t.Fatalf("telemetry: %v", err)
	}
	plan, err := cfg.resolveKeyPlan()
	if err != nil {
		t.Fatalf("partition key plan: %v", err)
	}
	return &kinesisExporter{
		cfg:        cfg,
		keyPlan:    plan,
		client:     client,
		tracesEnc:  tEnc,
		metricsEnc: mEnc,
		logsEnc:    lEnc,
		comp:       comp,
		logger:     zap.NewNop(),
		tel:        tel,
	}
}

func TestConsumeTraces(t *testing.T) {
	// Keep the in-place retry backoff negligible so retryable cases stay fast.
	defer withFastBackoff()()

	tests := []struct {
		name          string
		maxRecordSize int
		result        func(hits *atomic.Int32) fakeResult
		expectErr     bool
		expectPerm    bool
		expectHits    int32
	}{
		{
			name:          "happy path",
			maxRecordSize: 1 << 20,
			result: func(hits *atomic.Int32) fakeResult {
				return fakeResult{
					hits: hits,
					out: &kinesis.PutRecordsOutput{
						FailedRecordCount: aws.Int32(0),
						Records: []types.PutRecordsResultEntry{{
							SequenceNumber: aws.String("seq-1"),
							ShardId:        aws.String("shard-1"),
						}},
					},
				}
			},
			expectHits: 1,
		},
		{
			name:          "partial failure is retryable",
			maxRecordSize: 1 << 20,
			result: func(hits *atomic.Int32) fakeResult {
				return fakeResult{
					hits: hits,
					out: &kinesis.PutRecordsOutput{
						FailedRecordCount: aws.Int32(1),
						Records: []types.PutRecordsResultEntry{{
							ErrorCode:    aws.String("ProvisionedThroughputExceededException"),
							ErrorMessage: aws.String("slow down"),
						}},
					},
				}
			},
			expectErr:  true,
			expectPerm: false,
			expectHits: maxPutAttempts, // throttled subset retried in place, then surfaced as retryable
		},
		{
			name:          "ResourceNotFoundException is permanent",
			maxRecordSize: 1 << 20,
			result: func(hits *atomic.Int32) fakeResult {
				return fakeResult{
					hits: hits,
					err:  &types.ResourceNotFoundException{Message: aws.String("stream not found")},
				}
			},
			expectErr:  true,
			expectPerm: true,
			expectHits: 1,
		},
		{
			name:          "InvalidArgumentException is permanent",
			maxRecordSize: 1 << 20,
			result: func(hits *atomic.Int32) fakeResult {
				return fakeResult{
					hits: hits,
					err:  &types.InvalidArgumentException{Message: aws.String("bad request")},
				}
			},
			expectErr:  true,
			expectPerm: true,
			expectHits: 1,
		},
		{
			name:          "ProvisionedThroughputExceededException is retryable",
			maxRecordSize: 1 << 20,
			result: func(hits *atomic.Int32) fakeResult {
				return fakeResult{
					hits: hits,
					err:  &types.ProvisionedThroughputExceededException{Message: aws.String("slow down")},
				}
			},
			expectErr:  true,
			expectPerm: false,
			expectHits: 1,
		},
		{
			// A single span cannot be split, so split_half drops the atomic
			// leaf without ever calling Kinesis.
			name:          "oversize single span is dropped without calling kinesis",
			maxRecordSize: 16,
			result: func(hits *atomic.Int32) fakeResult {
				return fakeResult{hits: hits}
			},
			expectHits: 0,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var hits atomic.Int32
			exp := newTestExporter(t, tc.maxRecordSize, injectFake(tc.result(&hits)))
			err := exp.ConsumeTraces(context.Background(), sampleTraces())
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if got := consumererror.IsPermanent(err); got != tc.expectPerm {
					t.Fatalf("IsPermanent: got %v want %v (err=%v)", got, tc.expectPerm, err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := hits.Load(); got != tc.expectHits {
				t.Fatalf("middleware hits: got %d want %d", got, tc.expectHits)
			}
		})
	}
}

// TestConcurrentConsumeNoRace builds one exporter with zstd compression and
// drives ConsumeTraces, ConsumeMetrics, and ConsumeLogs concurrently from 8
// goroutines (25 iterations each). Run under -race to confirm the shared zstd
// encoder and the full emit pipeline are goroutine-safe.
func TestConcurrentConsumeNoRace(t *testing.T) {
	capt := &capture{}
	cfg := &Config{
		StreamName:    "test-stream",
		Region:        "us-east-1",
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecZstd,
		MaxRecordSize: 1 << 20,
		PartitionKey:  PartitionKeyConfig{Strategy: partitionStrategyRandom, Hash: hashXXHash},
		Oversize:      OversizeConfig{Policies: []string{oversizeSplitHalf}, MaxAttempts: 8, MaxAttributeValueBytes: 4096},
	}
	exp := newTestExporterCfg(t, cfg, capt.injectSerialize())

	const (
		goroutines = 8
		iterations = 25
	)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()
			for i := 0; i < iterations; i++ {
				var err error
				switch i % 3 {
				case 0:
					err = exp.ConsumeTraces(ctx, sampleTraces())
				case 1:
					err = exp.ConsumeMetrics(ctx, metricsWith([][2]string{{"svc", "reg"}}))
				case 2:
					err = exp.ConsumeLogs(ctx, logsWith([][2]string{{"svc", "reg"}}))
				}
				if err != nil {
					t.Errorf("goroutine %d iter %d: %v", id, i, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

// sampleTraces returns a minimal Traces value with one span; we keep the
// helper local so test files in this package stay self-contained.
func sampleTraces() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "test-service")
	ss := rs.ScopeSpans().AppendEmpty()
	span := ss.Spans().AppendEmpty()
	span.SetName("test-span")
	span.SetTraceID([16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10})
	span.SetSpanID([8]byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18})
	return td
}

// TestSleepBackoffRespectsDeadline verifies that sleepBackoff fails fast
// instead of consuming the attempt budget. Both an already-expired context and
// a live deadline that would fire before the backoff completes must return
// immediately — sleeping into a known deadline wastes time the caller could
// spend surfacing the error to the Collector's retry policy. Nominal backoff
// for try=4 is putBackoffBase*2^4 = 1600ms.
func TestSleepBackoffRespectsDeadline(t *testing.T) {
	t.Run("expired context returns its error immediately", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cancel()
		time.Sleep(10 * time.Millisecond) // ensure deadline has elapsed

		start := time.Now()
		err := sleepBackoff(ctx, 4)
		elapsed := time.Since(start)

		if err == nil {
			t.Fatal("expected context error, got nil")
		}
		if elapsed > 100*time.Millisecond {
			t.Fatalf("sleepBackoff took %v with expired deadline; want < 100ms (nominal backoff is 1600ms)", elapsed)
		}
	})

	t.Run("live deadline nearer than backoff returns without sleeping", func(t *testing.T) {
		// Deadline 500ms out, backoff 1600ms: sleeping into the deadline would
		// block ~500ms. The fast-path must return DeadlineExceeded well before.
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		start := time.Now()
		err := sleepBackoff(ctx, 4)
		elapsed := time.Since(start)

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
		if elapsed > 100*time.Millisecond {
			t.Fatalf("sleepBackoff took %v with live 500ms deadline; want immediate return, not a sleep into the deadline", elapsed)
		}
	})
}

// TestFlushAbortsOnCancelledCtx verifies that flush() checks ctx.Err() between
// chunks and aborts immediately — without sending subsequent chunks — when the
// context is already cancelled. The SDK fake ignores the context so that without
// the inter-chunk check flush would drive all chunks regardless of cancellation.
func TestFlushAbortsOnCancelledCtx(t *testing.T) {
	var callCount int32
	inject := func(o *kinesis.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Initialize.Add(
				middleware.InitializeMiddlewareFunc("ignoreCtxCapture", func(
					_ context.Context, // deliberately ignore ctx so the fake always succeeds
					_ middleware.InitializeInput,
					_ middleware.InitializeHandler,
				) (middleware.InitializeOutput, middleware.Metadata, error) {
					atomic.AddInt32(&callCount, 1)
					return middleware.InitializeOutput{
						Result: &kinesis.PutRecordsOutput{FailedRecordCount: aws.Int32(0)},
					}, middleware.Metadata{}, nil
				}),
				middleware.Before,
			)
		})
	}

	cfg := &Config{
		StreamName:    "test-stream",
		Region:        "us-east-1",
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecNone,
		MaxRecordSize: 1 << 20,
		PartitionKey:  PartitionKeyConfig{Strategy: partitionStrategyRandom, Hash: hashXXHash},
		Oversize:      OversizeConfig{Policies: []string{oversizeSplitHalf}, MaxAttempts: 8, MaxAttributeValueBytes: 4096},
		PutRecords:    PutRecordsConfig{MaxRecords: 1, MaxBytes: 5 << 20}, // 1 record per chunk
	}
	exp := newTestExporterCfg(t, cfg, inject)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel so ctx.Err() is non-nil

	entries := []types.PutRecordsRequestEntry{
		{Data: []byte("a"), PartitionKey: aws.String("key")},
		{Data: []byte("b"), PartitionKey: aws.String("key")},
		{Data: []byte("c"), PartitionKey: aws.String("key")},
	}
	err := exp.flush(ctx, entries)
	if err == nil {
		t.Fatal("expected error for cancelled ctx, got nil")
	}
	// With the ctx.Err() guard at the top of each chunk iteration, zero
	// PutRecords calls should be made.
	if n := atomic.LoadInt32(&callCount); n != 0 {
		t.Fatalf("flush made %d PutRecords calls with pre-cancelled ctx, want 0", n)
	}
}
