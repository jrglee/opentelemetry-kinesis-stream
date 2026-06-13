package awskinesisexporter

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

// PutRecords retry bounds. A partial PutRecords failure is retried in place for
// only the transient (throttled / InternalFailure) subset, with capped
// exponential backoff, so already-succeeded records are not duplicated and
// permanently-rejected records do not head-of-line-block. After maxPutAttempts
// the still-failing subset is handed to the Collector's retry policy.
const maxPutAttempts = 5

// Backoff bounds for the in-place transient retry. Vars (not consts) so tests
// can shrink them; production never mutates them.
var (
	putBackoffBase = 100 * time.Millisecond
	putBackoffMax  = 2 * time.Second
)

// transientPutError reports whether a per-record PutRecords ErrorCode is worth
// retrying. Throttling clears once the shard has capacity, and InternalFailure
// is a transient service error; any other code (e.g. a validation error) fails
// identically on every retry and is treated as permanent.
func transientPutError(code string) bool {
	switch code {
	case "ProvisionedThroughputExceededException", "InternalFailure":
		return true
	default:
		return false
	}
}

// taggedBatch pairs a signal batch with the joined tag value used to derive
// its partition key. key is empty for the random strategy.
type taggedBatch[T any] struct {
	key   string
	batch T
}

// signalCodec is the per-signal adapter that lets the otherwise identical
// group/compress/oversize/PutRecords pipeline stay signal-agnostic. Only the
// four operations here touch pdata; everything else is generic.
type signalCodec[T any] struct {
	// groupByTags partitions a batch by the ordered tag values. With no tags
	// it returns a single batch keyed "" (random strategy).
	groupByTags func(b T, tags []string) []taggedBatch[T]
	// splitHalf divides a batch's children roughly in half. ok is false when
	// the batch is a single indivisible leaf item.
	splitHalf func(b T) (T, T, bool)
	marshal   func(b T) ([]byte, error)
	// itemCount is the span/datapoint count, used for drop accounting and logs.
	itemCount func(b T) int
}

// emit is the shared export path for any signal: group by tags, repack each
// group into fitting payloads, and flush them as bounded PutRecords calls.
func emit[T any](ctx context.Context, e *kinesisExporter, b T, sc signalCodec[T]) error {
	var tags []string
	if e.cfg.tagHash() {
		tags = e.cfg.PartitionKey.Tags
	}
	groups := sc.groupByTags(b, tags)

	strategy := partitionStrategyRandom
	if e.cfg.tagHash() {
		strategy = partitionStrategyTagHash
	}
	e.logger.Debug("emit", zap.Int("groups", len(groups)), zap.String("strategy", strategy))

	entries := make([]types.PutRecordsRequestEntry, 0, len(groups))
	for _, g := range groups {
		key := e.partitionKey(g.key)
		payloads, dropped := pack(g.batch, sc, e.cfg, e.comp, 0)
		if dropped > 0 {
			e.tel.recordDrop(ctx, dropped, "oversize")
			e.logger.Warn("dropped oversize items during repack", zap.Int("dropped", dropped))
		}
		for _, p := range payloads {
			entries = append(entries, types.PutRecordsRequestEntry{Data: p, PartitionKey: aws.String(key)})
		}
	}
	return e.flush(ctx, entries)
}

// partitionKey resolves the per-record key. tag_hash maps a tag tuple to a
// stable 16-hex key so equal tuples always land on the same key (and shard);
// random returns a fresh UUID per group for uniform fan-out.
func (e *kinesisExporter) partitionKey(tagValue string) string {
	if e.cfg.tagHash() {
		return fmt.Sprintf("%016x", xxhash.Sum64String(tagValue))
	}
	return uuid.NewString()
}

// pack marshals a batch, compresses it, and — if the result exceeds
// MaxRecordSize — repacks it into fitting payloads per the oversize policy.
// depth bounds recursion at cfg.Oversize.MaxAttempts; items past the bound or
// that cannot be reduced further are dropped and counted.
func pack[T any](
	batch T,
	sc signalCodec[T],
	cfg *Config,
	comp interface {
		Compress([]byte) ([]byte, error)
	},
	depth int,
) (payloads [][]byte, dropped int) {
	raw, err := sc.marshal(batch)
	if err != nil {
		// Marshal of a sub-batch built by CopyTo should not fail; drop and count.
		return nil, sc.itemCount(batch)
	}
	payload, err := comp.Compress(raw)
	if err != nil {
		return nil, sc.itemCount(batch)
	}
	if len(payload) <= cfg.MaxRecordSize {
		return [][]byte{payload}, 0
	}

	// Over the ceiling. reject drops the whole batch outright.
	if cfg.Oversize.Policy == oversizeReject {
		return nil, sc.itemCount(batch)
	}
	// Recursion bound: give up and drop what remains.
	if depth >= cfg.Oversize.MaxAttempts {
		return nil, sc.itemCount(batch)
	}

	a, b, ok := sc.splitHalf(batch)
	if !ok {
		// A single leaf item still too large cannot be reduced further. Both
		// split_half and drop_largest converge here: drop this leaf. (Real
		// drop_largest would remove only the largest top-level resource of a
		// multi-resource batch; that case is already handled by the split
		// recursion below, so the only remaining drop is this atomic leaf.)
		return nil, sc.itemCount(batch)
	}
	pa, da := pack(a, sc, cfg, comp, depth+1)
	pb, db := pack(b, sc, cfg, comp, depth+1)
	return append(pa, pb...), da + db
}

// flush sends entries via PutRecords, chunking so each call stays within the
// operator-configured per-call record-count and byte limits (put_records.*).
// At least one record is always included per call even if it alone exceeds the
// byte limit — a single record's size is bounded by max_record_size instead.
func (e *kinesisExporter) flush(ctx context.Context, entries []types.PutRecordsRequestEntry) error {
	maxRecords := e.cfg.PutRecords.MaxRecords
	maxBytes := e.cfg.PutRecords.MaxBytes
	for start := 0; start < len(entries); {
		end := start
		bytes := 0
		for end < len(entries) && end-start < maxRecords {
			n := len(entries[end].Data)
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
// retrying only the transient subset in place. PutRecords returns a per-record
// result array in request order, so a record that succeeded is never re-sent
// (no duplication) and a permanently-rejected record is dropped-and-counted
// once rather than riding a whole-batch retry. Throttled / InternalFailure
// records are retried with capped backoff; if they still fail after
// maxPutAttempts, the remaining subset is surfaced as a retryable error for the
// Collector's retry policy (the at-least-once backstop).
func (e *kinesisExporter) putRecords(ctx context.Context, records []types.PutRecordsRequestEntry) error {
	attempt := records
	for try := 0; ; try++ {
		if len(attempt) == 0 {
			return nil
		}
		bytes := 0
		for i := range attempt {
			bytes += len(attempt[i].Data)
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
		var rejected int
		for i, r := range out.Records {
			if r.ErrorCode == nil {
				continue // succeeded
			}
			if transientPutError(aws.ToString(r.ErrorCode)) {
				retry = append(retry, attempt[i])
				continue
			}
			e.logger.Warn(
				"kinesis record rejected",
				zap.Int("index", i),
				zap.String("code", aws.ToString(r.ErrorCode)),
				zap.String("message", aws.ToString(r.ErrorMessage)),
			)
			rejected++
		}
		if rejected > 0 {
			e.tel.recordDrop(ctx, rejected, "rejected")
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

// sleepBackoff waits putBackoffBase*2^try (capped at putBackoffMax) or until the
// context is cancelled, whichever comes first.
func sleepBackoff(ctx context.Context, try int) error {
	d := putBackoffBase << try
	if d > putBackoffMax || d <= 0 {
		d = putBackoffMax
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
