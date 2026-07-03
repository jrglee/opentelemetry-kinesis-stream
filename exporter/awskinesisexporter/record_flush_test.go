package awskinesisexporter

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/smithy-go/middleware"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/ptrace"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// callCapture records the per-call record count for each PutRecords call.
// Unlike capture it does not retain payload data — per-call sizes are enough
// to characterize flush chunking behavior.
type callCapture struct {
	mu    sync.Mutex
	sizes []int
}

func (cc *callCapture) injectSerialize() func(*kinesis.Options) {
	return func(o *kinesis.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Initialize.Add(
				middleware.InitializeMiddlewareFunc("callCapture", func(_ context.Context, in middleware.InitializeInput, _ middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
					if pr, ok := in.Parameters.(*kinesis.PutRecordsInput); ok {
						cc.mu.Lock()
						cc.sizes = append(cc.sizes, len(pr.Records))
						cc.mu.Unlock()
					}
					return middleware.InitializeOutput{
						Result: &kinesis.PutRecordsOutput{FailedRecordCount: aws.Int32(0)},
					}, middleware.Metadata{}, nil
				}),
				middleware.Before,
			)
		})
	}
}

func (cc *callCapture) callSizes() []int {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	out := make([]int, len(cc.sizes))
	copy(out, cc.sizes)
	return out
}

func TestRandomStrategyDistinctKeysPerRecord(t *testing.T) {
	cfg := &Config{
		StreamName:    "test-stream",
		Region:        "us-east-1",
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecNone,
		MaxRecordSize: 120, // tiny: forces split_half into several records
		PartitionKey:  PartitionKeyConfig{Strategy: partitionStrategyRandom, Hash: hashXXHash},
		Oversize:      OversizeConfig{Policies: []string{oversizeSplitHalf}, MaxAttempts: 16, MaxAttributeValueBytes: 4096},
	}
	capt := &capture{}
	exp := newTestExporterCfg(t, cfg, capt.injectSerialize())

	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	for i := 0; i < 8; i++ {
		ss.Spans().AppendEmpty().SetName("span-name-padding-to-exceed-the-limit")
	}
	if err := exp.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("consume: %v", err)
	}
	recs := capt.all()
	if len(recs) <= 1 {
		t.Fatalf("expected split into >1 record, got %d", len(recs))
	}
	// Random promises uniform shard fan-out; a shared key would funnel every
	// record of this batch onto a single shard.
	seen := map[string]struct{}{}
	for _, r := range recs {
		seen[aws.ToString(r.PartitionKey)] = struct{}{}
	}
	if len(seen) != len(recs) {
		t.Fatalf("expected %d distinct partition keys, got %d", len(recs), len(seen))
	}
}

// TestRecordFitIncludesPartitionKeyBytes pins the record-size gate to what
// Kinesis actually enforces: data bytes plus partition-key bytes against the
// per-record limit. A payload that fits MaxRecordSize on its own but not with
// its key must be repacked, never shipped as-is.
func TestRecordFitIncludesPartitionKeyBytes(t *testing.T) {
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	for i := 0; i < 8; i++ {
		ss.Spans().AppendEmpty().SetName("span-name-padding-for-the-fit-check")
	}
	enc, err := encoding.NewTracesEncoder(encoding.EncodingOTLPProto)
	if err != nil {
		t.Fatalf("encoder: %v", err)
	}
	raw, err := enc.Marshal(td)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Fits the payload alone but not payload + the 36-byte random UUID key.
	limit := len(raw) + 10

	cfg := &Config{
		StreamName:    "test-stream",
		Region:        "us-east-1",
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecNone,
		MaxRecordSize: limit,
		PartitionKey:  PartitionKeyConfig{Strategy: partitionStrategyRandom, Hash: hashXXHash},
		Oversize:      OversizeConfig{Policies: []string{oversizeSplitHalf}, MaxAttempts: 16, MaxAttributeValueBytes: 4096},
	}
	capt := &capture{}
	exp := newTestExporterCfg(t, cfg, capt.injectSerialize())
	if err := exp.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("consume: %v", err)
	}

	recs := capt.all()
	dec, _ := encoding.NewTracesDecoder(encoding.EncodingOTLPProto)
	total := 0
	for _, r := range recs {
		if n := len(r.Data) + len(aws.ToString(r.PartitionKey)); n > limit {
			t.Fatalf("record exceeds Kinesis size accounting: data+key=%d > max_record_size=%d", n, limit)
		}
		d, _ := dec.Unmarshal(r.Data)
		total += d.SpanCount()
	}
	if total != 8 {
		t.Fatalf("spans preserved: got %d want 8", total)
	}
}

// TestFlushByteChunkingIncludesKeyBytes pins the PutRecords request-size gate
// to data + partition-key bytes. Three entries whose data alone fits MaxBytes
// but whose data+keys does not must split across two calls.
func TestFlushByteChunkingIncludesKeyBytes(t *testing.T) {
	var cc callCapture
	cfg := &Config{
		StreamName:    "test-stream",
		Region:        "us-east-1",
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecNone,
		MaxRecordSize: 1 << 20,
		PutRecords:    PutRecordsConfig{MaxRecords: 500, MaxBytes: 200},
		PartitionKey:  PartitionKeyConfig{Strategy: partitionStrategyRandom, Hash: hashXXHash},
		Oversize:      OversizeConfig{Policies: []string{oversizeSplitHalf}, MaxAttempts: 8, MaxAttributeValueBytes: 4096},
	}
	exp := newTestExporterCfg(t, cfg, cc.injectSerialize())

	// 60 data bytes + a 36-byte key = 96 per entry: two fit under 200 (192),
	// three would only fit if key bytes were ignored (data alone is 180).
	key := "0123456789abcdef0123456789abcdef0123" // 36 bytes, UUID-length
	entries := make([]types.PutRecordsRequestEntry, 3)
	for i := range entries {
		entries[i] = types.PutRecordsRequestEntry{
			Data:         make([]byte, 60),
			PartitionKey: aws.String(key),
		}
	}
	if err := exp.flush(context.Background(), entries); err != nil {
		t.Fatalf("flush: %v", err)
	}

	sizes := cc.callSizes()
	if len(sizes) != 2 || sizes[0] != 2 || sizes[1] != 1 {
		t.Fatalf("PutRecords calls: got %v want [2 1] (key bytes must count toward max_bytes)", sizes)
	}
}

// TestFlushChunksAtMaxRecords locks the flush-chunking behavior at the
// MaxRecords boundary: 501 records with MaxRecords=500 must produce exactly two
// PutRecords calls (500+1).
func TestFlushChunksAtMaxRecords(t *testing.T) {
	var cc callCapture
	cfg := &Config{
		StreamName:    "test-stream",
		Region:        "us-east-1",
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecNone,
		MaxRecordSize: 1 << 20,
		PutRecords:    PutRecordsConfig{MaxRecords: 500, MaxBytes: 100 << 20},
		PartitionKey:  PartitionKeyConfig{Strategy: partitionStrategyRandom, Hash: hashXXHash},
		Oversize:      OversizeConfig{Policies: []string{oversizeSplitHalf}, MaxAttempts: 8, MaxAttributeValueBytes: 4096},
	}
	exp := newTestExporterCfg(t, cfg, cc.injectSerialize())

	entries := make([]types.PutRecordsRequestEntry, 501)
	for i := range entries {
		entries[i] = types.PutRecordsRequestEntry{Data: []byte("x"), PartitionKey: aws.String("k")}
	}
	if err := exp.flush(context.Background(), entries); err != nil {
		t.Fatalf("flush: %v", err)
	}

	sizes := cc.callSizes()
	if len(sizes) != 2 {
		t.Fatalf("PutRecords calls: got %d want 2 (500+1), sizes=%v", len(sizes), sizes)
	}
	if sizes[0] != 500 {
		t.Fatalf("first call: got %d records want 500", sizes[0])
	}
	if sizes[1] != 1 {
		t.Fatalf("second call: got %d records want 1", sizes[1])
	}
}

// TestFlushChunksAtMaxBytes locks the flush-chunking behavior at the MaxBytes
// boundary. Three 60-byte entries with MaxBytes=150 must split 2+1 across two
// PutRecords calls: the first two entries total 120 bytes (fits); adding the
// third would reach 180, which exceeds MaxBytes, so it opens a new call.
func TestFlushChunksAtMaxBytes(t *testing.T) {
	var cc callCapture
	cfg := &Config{
		StreamName:    "test-stream",
		Region:        "us-east-1",
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecNone,
		MaxRecordSize: 1 << 20,
		PutRecords:    PutRecordsConfig{MaxRecords: 500, MaxBytes: 150},
		PartitionKey:  PartitionKeyConfig{Strategy: partitionStrategyRandom, Hash: hashXXHash},
		Oversize:      OversizeConfig{Policies: []string{oversizeSplitHalf}, MaxAttempts: 8, MaxAttributeValueBytes: 4096},
	}
	exp := newTestExporterCfg(t, cfg, cc.injectSerialize())

	data := make([]byte, 60) // 60+60=120 ≤ 150; adding a third: 180 > 150 → chunk boundary
	entries := []types.PutRecordsRequestEntry{
		{Data: data, PartitionKey: aws.String("k")},
		{Data: data, PartitionKey: aws.String("k")},
		{Data: data, PartitionKey: aws.String("k")},
	}
	if err := exp.flush(context.Background(), entries); err != nil {
		t.Fatalf("flush: %v", err)
	}

	sizes := cc.callSizes()
	if len(sizes) != 2 {
		t.Fatalf("PutRecords calls: got %d want 2 (2+1), sizes=%v", len(sizes), sizes)
	}
	if sizes[0] != 2 {
		t.Fatalf("first call: got %d records want 2", sizes[0])
	}
	if sizes[1] != 1 {
		t.Fatalf("second call: got %d records want 1", sizes[1])
	}
}

// TestFlushSingleRecordOverMaxBytes locks the "always include at least one
// record" guard in the flush loop (record.go). A single entry whose
// len(Data) > PutRecords.MaxBytes must still be dispatched in its own call —
// the loop's `end > start` guard prevents an infinite-skip that would stall
// progress when every record individually exceeds the byte ceiling.
func TestFlushSingleRecordOverMaxBytes(t *testing.T) {
	var cc callCapture
	cfg := &Config{
		StreamName:    "test-stream",
		Region:        "us-east-1",
		Encoding:      encoding.EncodingOTLPProto,
		Compression:   encoding.CodecNone,
		MaxRecordSize: 1 << 20,
		PutRecords:    PutRecordsConfig{MaxRecords: 500, MaxBytes: 50},
		PartitionKey:  PartitionKeyConfig{Strategy: partitionStrategyRandom, Hash: hashXXHash},
		Oversize:      OversizeConfig{Policies: []string{oversizeSplitHalf}, MaxAttempts: 8, MaxAttributeValueBytes: 4096},
	}
	exp := newTestExporterCfg(t, cfg, cc.injectSerialize())

	// One entry whose Data is twice MaxBytes — must still be dispatched alone.
	entries := []types.PutRecordsRequestEntry{
		{Data: make([]byte, 100), PartitionKey: aws.String("k")},
	}
	if err := exp.flush(context.Background(), entries); err != nil {
		t.Fatalf("flush: %v", err)
	}

	sizes := cc.callSizes()
	if len(sizes) != 1 {
		t.Fatalf("PutRecords calls: got %d want 1, sizes=%v", len(sizes), sizes)
	}
	if sizes[0] != 1 {
		t.Fatalf("call record count: got %d want 1", sizes[0])
	}
}

// TestPutRecordsPartialFailureRetriesAllErrorCodes verifies the partial-failure
// contract: succeeded records are never re-sent, and every record with any
// error code — including unrecognised codes — is retried in place. No per-record
// code is permanently dropped; exhausted subsets surface as retryable errors.
func TestPutRecordsPartialFailureRetriesAllErrorCodes(t *testing.T) {
	defer withFastBackoff()()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	var mu sync.Mutex
	var callSizes []int

	inject := func(o *kinesis.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Initialize.Add(
				middleware.InitializeMiddlewareFunc("programmedKinesis", func(_ context.Context, in middleware.InitializeInput, _ middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
					pr := in.Parameters.(*kinesis.PutRecordsInput)
					n := len(pr.Records)
					mu.Lock()
					callSizes = append(callSizes, n)
					first := len(callSizes) == 1
					mu.Unlock()

					recs := make([]types.PutRecordsResultEntry, n)
					var failed int32
					for i := range recs {
						code := ""
						if first {
							switch i {
							case 1:
								code = "ProvisionedThroughputExceededException" // transient
							case 2:
								code = "InternalFailure" // transient
							case 3:
								code = "ValidationException" // permanent
							}
						}
						if code == "" {
							recs[i] = types.PutRecordsResultEntry{SequenceNumber: aws.String("ok")}
						} else {
							recs[i] = types.PutRecordsResultEntry{ErrorCode: aws.String(code), ErrorMessage: aws.String(code)}
							failed++
						}
					}
					return middleware.InitializeOutput{
						Result: &kinesis.PutRecordsOutput{FailedRecordCount: aws.Int32(failed), Records: recs},
					}, middleware.Metadata{}, nil
				}),
				middleware.Before,
			)
		})
	}

	exp := newTestExporterCfg(t, tagHashCfg(), inject)
	tel, err := newExporterTelemetry(mp)
	if err != nil {
		t.Fatal(err)
	}
	exp.tel = tel

	if err := exp.ConsumeTraces(context.Background(), tracesWith(tuples())); err != nil {
		t.Fatalf("consume: %v", err) // all error codes retried to success → no error
	}

	mu.Lock()
	defer mu.Unlock()
	if len(callSizes) != 2 {
		t.Fatalf("PutRecords calls: got %d (%v) want 2 (initial + retry of all error records)", len(callSizes), callSizes)
	}
	if callSizes[0] != len(tuples()) {
		t.Fatalf("first call records: got %d want %d", callSizes[0], len(tuples()))
	}
	// All 3 error records (ProvisionedThroughputExceededException, InternalFailure,
	// ValidationException) are retried — no per-record code is permanently dropped.
	if callSizes[1] != 3 {
		t.Fatalf("retry call records: got %d want 3 (all error records, including unknown codes)", callSizes[1])
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	// No per-record code is dropped; drop counter must be zero.
	if got := sumDropped(t, &rm); got != 0 {
		t.Fatalf("dropped counter: got %d want 0 (no per-record code is permanently dropped)", got)
	}
}

// TestPutRecordsRetryExhaustionErrorShape programs every record to fail with
// ProvisionedThroughputExceededException on every attempt. After maxPutAttempts
// the in-place retry loop exhausts and returns a retryable (non-permanent)
// error — the Collector's at-least-once outer retry owns the backstop, and the
// exporter must not escalate this to permanent.
func TestPutRecordsRetryExhaustionErrorShape(t *testing.T) {
	defer withFastBackoff()()

	inject := func(o *kinesis.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Initialize.Add(
				middleware.InitializeMiddlewareFunc("alwaysThrottle", func(_ context.Context, in middleware.InitializeInput, _ middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
					pr := in.Parameters.(*kinesis.PutRecordsInput)
					n := len(pr.Records)
					recs := make([]types.PutRecordsResultEntry, n)
					for i := range recs {
						recs[i] = types.PutRecordsResultEntry{
							ErrorCode:    aws.String("ProvisionedThroughputExceededException"),
							ErrorMessage: aws.String("throttled"),
						}
					}
					return middleware.InitializeOutput{
						Result: &kinesis.PutRecordsOutput{
							FailedRecordCount: aws.Int32(int32(n)),
							Records:           recs,
						},
					}, middleware.Metadata{}, nil
				}),
				middleware.Before,
			)
		})
	}

	exp := newTestExporter(t, 1<<20, inject)
	err := exp.ConsumeTraces(context.Background(), sampleTraces())
	if err == nil {
		t.Fatal("expected error after retry exhaustion, got nil")
	}
	if consumererror.IsPermanent(err) {
		t.Fatalf("error must not be permanent (Collector owns the outer retry backstop): %v", err)
	}
}

// TestUnknownErrorCodeIsRetried pins finding 3: an unrecognised per-record
// ErrorCode must be retried (transient bias) rather than silently dropped.
// AWS currently documents exactly two per-record codes; any future code must
// not be lost before it is classified. The test programs the first call to
// fail with "SomeFutureException" and asserts the record is retried on a
// second call with no drop counter increment.
func TestUnknownErrorCodeIsRetried(t *testing.T) {
	defer withFastBackoff()()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	var callCount int32
	inject := func(o *kinesis.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Initialize.Add(
				middleware.InitializeMiddlewareFunc("unknownCodeKinesis", func(_ context.Context, in middleware.InitializeInput, _ middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
					pr := in.Parameters.(*kinesis.PutRecordsInput)
					n := len(pr.Records)
					call := atomic.AddInt32(&callCount, 1)

					recs := make([]types.PutRecordsResultEntry, n)
					var failed int32
					if call == 1 {
						// First call: return an unknown error code on the first record.
						recs[0] = types.PutRecordsResultEntry{
							ErrorCode:    aws.String("SomeFutureException"),
							ErrorMessage: aws.String("new aws error"),
						}
						failed = 1
						for i := 1; i < n; i++ {
							recs[i] = types.PutRecordsResultEntry{SequenceNumber: aws.String("ok")}
						}
					} else {
						// Subsequent calls: all succeed.
						for i := range recs {
							recs[i] = types.PutRecordsResultEntry{SequenceNumber: aws.String("ok")}
						}
					}
					return middleware.InitializeOutput{
						Result: &kinesis.PutRecordsOutput{FailedRecordCount: aws.Int32(failed), Records: recs},
					}, middleware.Metadata{}, nil
				}),
				middleware.Before,
			)
		})
	}

	exp := newTestExporterCfg(t, tagHashCfg(), inject)
	tel, err := newExporterTelemetry(mp)
	if err != nil {
		t.Fatal(err)
	}
	exp.tel = tel

	if err := exp.ConsumeTraces(context.Background(), sampleTraces()); err != nil {
		t.Fatalf("expected success after retry of unknown code: %v", err)
	}

	if n := atomic.LoadInt32(&callCount); n < 2 {
		t.Fatalf("expected at least 2 PutRecords calls (initial + retry), got %d", n)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	if total := sumDropped(t, &rm); total != 0 {
		dropped := sumByReason(t, &rm, "kinesis.exporter.records_dropped")
		t.Fatalf("drop counter: got %d want 0 (unknown code must be retried, not dropped); reasons=%v", total, dropped)
	}
}

func TestRandomDefaultKey(t *testing.T) {
	capt := &capture{}
	exp := newTestExporter(t, 1<<20, capt.injectSerialize())
	if err := exp.ConsumeTraces(context.Background(), sampleTraces()); err != nil {
		t.Fatalf("consume 1: %v", err)
	}
	if err := exp.ConsumeTraces(context.Background(), sampleTraces()); err != nil {
		t.Fatalf("consume 2: %v", err)
	}
	recs := capt.all()
	if len(recs) != 2 {
		t.Fatalf("records: got %d want 2 (one per call)", len(recs))
	}
	k1, k2 := aws.ToString(recs[0].PartitionKey), aws.ToString(recs[1].PartitionKey)
	if k1 == "" || k2 == "" {
		t.Fatalf("empty random key")
	}
	if k1 == k2 {
		t.Fatalf("random keys should differ across calls: %s == %s", k1, k2)
	}
}
