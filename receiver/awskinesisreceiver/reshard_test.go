package awskinesisreceiver

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/smithy-go/middleware"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// TestReshardParentDrainsBeforeChild verifies the parent-drains-before-child
// invariant against a synthetic post-split topology. MiniStack does not model
// resharding faithfully (it appends shards without parent lineage or closing
// the originals), so this drives the real coordinator and poller against a
// fake Kinesis served via Smithy middleware: a closed parent shard with
// records, plus a child shard that references it. The receiver must consume
// every parent record and mark the parent SHARD_END before it reads a single
// child record.
func TestReshardParentDrainsBeforeChild(t *testing.T) {
	enc, err := encoding.NewTracesEncoder(encoding.EncodingOTLPProto)
	if err != nil {
		t.Fatal(err)
	}

	parentSeqs := spanData(t, enc, "P", 5)
	childSeqs := spanData(t, enc, "C", 5)

	fs := &fakeStream{
		shards: []*fakeShard{
			// Parent: closed (its records end with SHARD_END), no parent.
			{id: "shard-parent", records: parentSeqs},
			// Child: references the parent; must not be read until the parent
			// reaches SHARD_END.
			{id: "shard-child", parent: "shard-parent", records: childSeqs},
		},
	}

	rec := &recorder{}
	consumeFn, err := consumer.NewTraces(rec.consume)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := encoding.NewTracesDecoder(encoding.EncodingOTLPProto)
	if err != nil {
		t.Fatal(err)
	}

	cfg := fastCoordCfg("rs", encoding.EncodingOTLPProto, encoding.CodecNone)
	c := newTestCoordinator(t, cfg, fs, tracesSink{decoder: dec, consumer: consumeFn}, "w1")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.start(ctx); err != nil {
		t.Fatalf("coordinator start: %v", err)
	}

	// Wait until all 10 spans are consumed.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rec.len() >= 10 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	c.wait()

	names := rec.snapshot()
	if len(names) != 10 {
		t.Fatalf("consumed %d spans, want 10: %v", len(names), names)
	}

	// Invariant: every parent span precedes every child span.
	seenChild := false
	for _, n := range names {
		switch {
		case strings.HasPrefix(n, "C-"):
			seenChild = true
		case strings.HasPrefix(n, "P-"):
			if seenChild {
				t.Fatalf("parent span %q delivered after a child span; order=%v", n, names)
			}
		}
	}

	// Completeness: all five parent and five child spans arrived exactly once.
	counts := map[string]int{}
	for _, n := range names {
		counts[n]++
	}
	for i := 0; i < 5; i++ {
		for _, p := range []string{"P", "C"} {
			k := fmt.Sprintf("%s-%d", p, i)
			if counts[k] != 1 {
				t.Errorf("span %q delivered %d times, want 1", k, counts[k])
			}
		}
	}
}

// recorder captures the order spans are delivered to the pipeline.
type recorder struct {
	mu    sync.Mutex
	names []string
}

func (r *recorder) consume(_ context.Context, td ptrace.Traces) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				r.names = append(r.names, spans.At(k).Name())
			}
		}
	}
	return nil
}

func (r *recorder) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.names)
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.names...)
}

// spanData marshals n single-span Traces, each named "<prefix>-<i>".
func spanData(t *testing.T, enc encoding.TracesEncoder, prefix string, n int) [][]byte {
	t.Helper()
	out := make([][]byte, n)
	for i := 0; i < n; i++ {
		td := ptrace.NewTraces()
		s := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		s.SetName(fmt.Sprintf("%s-%d", prefix, i))
		b, err := enc.Marshal(td)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		out[i] = b
	}
	return out
}

// fakeShard is one shard in the synthetic stream. Every shard is finite: once
// its records are exhausted, GetRecords returns a nil iterator (SHARD_END).
type fakeShard struct {
	id      string
	parent  string
	records [][]byte
}

type fakeStream struct {
	mu     sync.Mutex
	shards []*fakeShard
}

func (fs *fakeStream) shard(id string) *fakeShard {
	for _, s := range fs.shards {
		if s.id == id {
			return s
		}
	}
	return nil
}

// handle short-circuits the Kinesis API calls the receiver makes, serving them
// from the synthetic stream. Installed on the Initialize step where the typed
// input is still available.
func (fs *fakeStream) handle(
	ctx context.Context,
	in middleware.InitializeInput,
	next middleware.InitializeHandler,
) (middleware.InitializeOutput, middleware.Metadata, error) {
	var out middleware.InitializeOutput
	fs.mu.Lock()
	defer fs.mu.Unlock()

	switch params := in.Parameters.(type) {
	case *kinesis.ListShardsInput:
		out.Result = fs.listShards()
	case *kinesis.GetShardIteratorInput:
		res, err := fs.getShardIterator(params)
		if err != nil {
			return out, middleware.Metadata{}, err
		}
		out.Result = res
	case *kinesis.GetRecordsInput:
		out.Result = fs.getRecords(params)
	default:
		return next.HandleInitialize(ctx, in)
	}
	return out, middleware.Metadata{}, nil
}

func (fs *fakeStream) listShards() *kinesis.ListShardsOutput {
	shards := make([]ktypes.Shard, 0, len(fs.shards))
	for _, s := range fs.shards {
		shard := ktypes.Shard{ShardId: aws.String(s.id)}
		if s.parent != "" {
			shard.ParentShardId = aws.String(s.parent)
		}
		shards = append(shards, shard)
	}
	return &kinesis.ListShardsOutput{Shards: shards}
}

// getShardIterator encodes the position as "shardID@index". TRIM_HORIZON starts
// at 0; AFTER_SEQUENCE_NUMBER resumes just past the given record.
func (fs *fakeStream) getShardIterator(in *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	shardID := aws.ToString(in.ShardId)
	if fs.shard(shardID) == nil {
		return nil, fmt.Errorf("unknown shard %q", shardID)
	}
	pos := 0
	if in.ShardIteratorType == ktypes.ShardIteratorTypeAfterSequenceNumber {
		idx, err := seqIndex(aws.ToString(in.StartingSequenceNumber))
		if err != nil {
			return nil, err
		}
		pos = idx + 1
	}
	return &kinesis.GetShardIteratorOutput{ShardIterator: aws.String(iterator(shardID, pos))}, nil
}

func (fs *fakeStream) getRecords(in *kinesis.GetRecordsInput) *kinesis.GetRecordsOutput {
	shardID, pos := parseIterator(aws.ToString(in.ShardIterator))
	s := fs.shard(shardID)
	if s == nil {
		return &kinesis.GetRecordsOutput{}
	}
	if pos >= len(s.records) {
		// Drained: a finite (closed) shard returns a nil iterator => SHARD_END.
		return &kinesis.GetRecordsOutput{NextShardIterator: nil}
	}
	rec := ktypes.Record{
		Data:           s.records[pos],
		SequenceNumber: aws.String(sequence(shardID, pos)),
		PartitionKey:   aws.String("pk"),
	}
	return &kinesis.GetRecordsOutput{
		Records:           []ktypes.Record{rec},
		NextShardIterator: aws.String(iterator(shardID, pos+1)),
	}
}

func fakeKinesisClient(fs *fakeStream) *kinesis.Client {
	return kinesis.New(kinesis.Options{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
		APIOptions: []func(*middleware.Stack) error{
			func(stack *middleware.Stack) error {
				return stack.Initialize.Add(
					middleware.InitializeMiddlewareFunc("fakeKinesis", fs.handle),
					middleware.Before,
				)
			},
		},
	})
}

func iterator(shardID string, pos int) string { return fmt.Sprintf("%s@%d", shardID, pos) }

func parseIterator(it string) (string, int) {
	at := strings.LastIndex(it, "@")
	if at < 0 {
		return it, 0
	}
	pos, _ := strconv.Atoi(it[at+1:])
	return it[:at], pos
}

func sequence(shardID string, idx int) string { return fmt.Sprintf("%s#%04d", shardID, idx) }

func seqIndex(seq string) (int, error) {
	h := strings.LastIndex(seq, "#")
	if h < 0 {
		return 0, fmt.Errorf("bad sequence %q", seq)
	}
	return strconv.Atoi(seq[h+1:])
}
