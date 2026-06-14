package awskinesisexporter

import (
	"context"
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
	return &kinesisExporter{
		cfg:        cfg,
		client:     client,
		tracesEnc:  tEnc,
		metricsEnc: mEnc,
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
