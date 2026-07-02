package awskinesisexporter

// Transport mechanism for the Kinesis exporter.
//
// This file owns the grouping seam (taggedBatch, signalCodec), the shared
// emit entry point, partition-key derivation, and the chunked PutRecords loop
// with bounded in-place retry and backoff. It does not decide what to do when
// a payload is too large — that policy lives in oversize.go. The separation
// means a policy change (e.g. a new oversize strategy) touches oversize.go
// only, and a transport change (e.g. a new AWS API call) touches record.go
// only.

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// PutRecords retry bounds. A partial PutRecords failure is retried in place
// for the failed subset with capped exponential backoff, so already-succeeded
// records are not duplicated. After maxPutAttempts the still-failing subset
// surfaces as a retryable error to the exporterhelper retry sender
// (retry_on_failure), which the factory wires around this exporter. The
// budget here stays deliberately short so the operator-tunable helper policy
// dominates.
const maxPutAttempts = 5

// Backoff bounds for the in-place transient retry. Vars (not consts) so tests
// can shrink them; production never mutates them.
var (
	putBackoffBase = 100 * time.Millisecond
	putBackoffMax  = 2 * time.Second
)

// taggedBatch pairs a signal batch with the joined tag value used to derive
// its partition key. key is empty for the random strategy.
type taggedBatch[T any] struct {
	key   string
	batch T
}

// signalCodec is the per-signal adapter that lets the otherwise identical
// group/compress/oversize/PutRecords pipeline stay signal-agnostic. Only the
// operations here touch pdata; everything else is generic.
type signalCodec[T any] struct {
	// groupByTags partitions a batch by the compiled key plan. With an empty
	// plan it returns a single batch keyed "" (random strategy).
	groupByTags func(b T, plan keyPlan) []taggedBatch[T]
	// splitHalf divides a batch's children roughly in half. ok is false when
	// the batch is a single indivisible leaf item.
	splitHalf func(b T) (T, T, bool)
	// truncateAttributes returns a clone of b with every string-valued
	// attribute clamped to maxBytes, and the count of values touched. The
	// caller's batch is never mutated.
	truncateAttributes func(b T, maxBytes int) (T, int)
	marshal            func(b T) ([]byte, error)
	// itemCount is the span/datapoint count, used for drop accounting and logs.
	itemCount func(b T) int
}

// emit is the shared export path for any signal: group by key plan, repack
// each group into fitting payloads, and flush them as bounded PutRecords calls.
func emit[T any](ctx context.Context, e *kinesisExporter, b T, sc signalCodec[T]) error {
	groups := sc.groupByTags(b, e.keyPlan)

	strategy := partitionStrategyRandom
	if e.cfg.tagHash() {
		strategy = partitionStrategyTagHash
	}
	e.logger.Debug("emit", zap.Int("groups", len(groups)), zap.String("strategy", strategy))

	entries := make([]types.PutRecordsRequestEntry, 0, len(groups))
	for _, g := range groups {
		payloads, ds := packChain(ctx, e, g.batch, sc)
		for _, d := range ds {
			e.tel.recordDrop(ctx, d.count, d.reason)
			e.logger.Warn(
				"dropped items during oversize recovery",
				zap.Int("dropped", d.count),
				zap.String("reason", d.reason),
				zap.Strings("policies", e.cfg.Oversize.Policies),
			)
		}
		for _, p := range payloads {
			entries = append(entries, types.PutRecordsRequestEntry{Data: p, PartitionKey: aws.String(e.partitionKey(g.key))})
		}
	}
	return e.flush(ctx, entries)
}

// partitionKey resolves the per-record key. tag_hash maps a tag tuple to a
// stable 16-hex key so equal tuples always land on the same key (and shard);
// random returns a fresh UUID per record for uniform fan-out — a key shared
// across a call's records would funnel them all onto one shard.
func (e *kinesisExporter) partitionKey(tagValue string) string {
	if e.cfg.tagHash() {
		return fmt.Sprintf("%016x", xxhash.Sum64String(tagValue))
	}
	return uuid.NewString()
}

// flush sends entries via PutRecords, chunking so each call stays within the
// operator-configured per-call record-count and byte limits (put_records.*).
// At least one record is always included per call even if it alone exceeds the
// byte limit — a single record's size is bounded by max_record_size instead.
// The context is checked between chunks so an attempt-deadline cancellation
// aborts the flush promptly instead of continuing into chunks that will also
// fail — the exporterhelper retry re-sends the whole request, so every chunk
// written past the deadline is a duplicate on the next attempt.
func (e *kinesisExporter) flush(ctx context.Context, entries []types.PutRecordsRequestEntry) error {
	maxRecords := e.cfg.PutRecords.MaxRecords
	maxBytes := e.cfg.PutRecords.MaxBytes
	for start := 0; start < len(entries); {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := start
		bytes := 0
		for end < len(entries) && end-start < maxRecords {
			// Key bytes count toward the request-level limit, same as the
			// record-level fit check in tryEncode.
			n := len(entries[end].Data) + len(aws.ToString(entries[end].PartitionKey))
			if end > start && bytes+n > maxBytes {
				break
			}
			bytes += n
			end++
		}
		if err := e.putRecords(ctx, entries[start:end]); err != nil {
			return err
		}
		start = end
	}
	return nil
}

// putRecords issues PutRecords for one chunk and resolves partial failures by
// retrying the failed subset in place. PutRecords returns a per-record result
// array in request order, so a record that succeeded is never re-sent (no
// duplication). Every per-record error code is retried — AWS documents only
// throttling and InternalFailure here, and an unrecognized future code must
// not be silently dropped before it can be classified. If records still fail
// after maxPutAttempts, the remaining subset is surfaced as a retryable error
// for the exporterhelper retry sender (the at-least-once backstop). Note a
// helper-level retry re-sends the whole request, so records that succeeded
// before the residual failure can be duplicated — accepted at-least-once
// behavior.
func (e *kinesisExporter) putRecords(ctx context.Context, records []types.PutRecordsRequestEntry) error {
	attempt := records
	for try := 0; ; try++ {
		if len(attempt) == 0 {
			return nil
		}
		bytes := 0
		for i := range attempt {
			bytes += len(attempt[i].Data) + len(aws.ToString(attempt[i].PartitionKey))
		}
		start := time.Now()
		out, err := e.client.PutRecords(ctx, &kinesis.PutRecordsInput{
			StreamName: aws.String(e.cfg.StreamName),
			Records:    attempt,
		})
		durationMs := float64(time.Since(start).Microseconds()) / 1000
		e.tel.recordPut(ctx, len(attempt), bytes, durationMs)
		if err != nil {
			return classifyPutRecordsError(err)
		}
		if out.FailedRecordCount == nil || *out.FailedRecordCount == 0 {
			e.logger.Debug(
				"put_records",
				zap.Int("records", len(attempt)),
				zap.Int("bytes", bytes),
				zap.Float64("duration_ms", durationMs),
			)
			return nil
		}

		var retry []types.PutRecordsRequestEntry
		for i, r := range out.Records {
			if r.ErrorCode == nil {
				continue // succeeded
			}
			// Every per-record ErrorCode is retried. AWS documents exactly two:
			// "ProvisionedThroughputExceededException" (shard throttled; backoff
			// clears the capacity debt) and "InternalFailure" (transient service
			// error). Anything else is deliberately retry-biased — a future AWS
			// code must not be silently dropped before it can be classified. After
			// maxPutAttempts the still-failing subset surfaces as a retryable
			// error for the Collector's retry policy, the at-least-once backstop.
			code := aws.ToString(r.ErrorCode)
			switch code {
			case "ProvisionedThroughputExceededException", "InternalFailure":
				// Known transient codes — retry silently.
			default:
				// Unknown code: log at Warn so operators see it.
				e.logger.Warn(
					"kinesis record: unrecognised error code (will retry)",
					zap.Int("index", i),
					zap.String("code", code),
					zap.String("message", aws.ToString(r.ErrorMessage)),
				)
			}
			retry = append(retry, attempt[i])
		}
		if len(retry) == 0 {
			return nil
		}
		if try+1 >= maxPutAttempts {
			return fmt.Errorf("put_records: %d records still failing after %d attempts", len(retry), try+1)
		}
		if err := sleepBackoff(ctx, try); err != nil {
			return err
		}
		attempt = retry
	}
}

// sleepBackoff waits putBackoffBase*2^try (capped at putBackoffMax) or until
// the context is cancelled, whichever comes first. If the context deadline is
// nearer than the backoff, it returns immediately — sleeping into a known
// deadline burns the attempt's remaining budget for nothing, and the retry
// this backoff precedes could never run anyway.
func sleepBackoff(ctx context.Context, try int) error {
	d := putBackoffBase << try
	if d > putBackoffMax || d <= 0 {
		d = putBackoffMax
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if dl, ok := ctx.Deadline(); ok && time.Until(dl) < d {
		return context.DeadlineExceeded
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
