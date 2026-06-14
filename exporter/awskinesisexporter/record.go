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
// operations here touch pdata; everything else is generic.
type signalCodec[T any] struct {
	// groupByTags partitions a batch by the ordered tag values. With no tags
	// it returns a single batch keyed "" (random strategy).
	groupByTags func(b T, tags []string) []taggedBatch[T]
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

// dropOutcome carries a count of items not exported and the reason label that
// names the cause.
type dropOutcome struct {
	count  int
	reason string
}

// drops is a coalesced list of per-reason drop counts. The split recursion can
// produce a mix of terminal reasons (e.g. one branch hits an irreducible leaf
// while another hits max_attempts) and we want each to land on the metric
// under its own label, not collapse to a single "chain_exhausted" bucket that
// would hide the operator's real lever (raising MaxAttempts).
type drops []dropOutcome

func (d drops) total() int {
	n := 0
	for _, x := range d {
		n += x.count
	}
	return n
}

// addReason coalesces by reason: a second drop with the same reason bumps the
// existing entry rather than appending a duplicate.
func (d drops) addReason(count int, reason string) drops {
	if count == 0 {
		return d
	}
	for i := range d {
		if d[i].reason == reason {
			d[i].count += count
			return d
		}
	}
	return append(d, dropOutcome{count: count, reason: reason})
}

func (d drops) merge(o drops) drops {
	for _, x := range o {
		d = d.addReason(x.count, x.reason)
	}
	return d
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

// packChain marshals a batch, compresses it, and — if the result exceeds
// MaxRecordSize — runs cfg.Oversize.Policies in order against the remainder
// until something fits or the chain is exhausted. It returns the payloads
// that fit and the per-reason drops accumulated along the way. The
// attributes_truncated counter is emitted at the truncate policy site so it
// records every mutation regardless of which policy ultimately shipped the
// data — there's no need to thread a "repaired" count up the call stack.
//
// Validation rejects policy lists with split_half or reject anywhere but the
// last position (see config.go), so this dispatcher does not need to handle
// "what if split's drops leak into a downstream policy?" — they cannot, by
// construction.
func packChain[T any](ctx context.Context, e *kinesisExporter, batch T, sc signalCodec[T]) ([][]byte, drops) {
	cfg := e.cfg
	// First try the unmodified batch. If it already fits, no policy runs.
	payload, ok, terminal := tryEncode(e, batch, sc)
	if terminal.count > 0 {
		return nil, drops{}.addReason(terminal.count, terminal.reason)
	}
	if ok {
		return [][]byte{payload}, nil
	}

	current := batch
	for _, policy := range cfg.Oversize.Policies {
		switch policy {
		case oversizeTruncateAttrs:
			if sc.truncateAttributes == nil {
				continue
			}
			trimmed, n := sc.truncateAttributes(current, cfg.Oversize.MaxAttributeValueBytes)
			e.logger.Debug(
				"oversize policy: truncate_attribute_values",
				zap.Int("attributes_clamped", n),
				zap.Int("max_attribute_value_bytes", cfg.Oversize.MaxAttributeValueBytes),
				zap.Int("item_count", sc.itemCount(current)),
			)
			if n == 0 {
				// Nothing to truncate — fall through to the next policy
				// without crediting the metric or mutating `current`.
				continue
			}
			// Credit every clamp the moment it happens, not when the chain
			// eventually decides whether truncation alone fit the payload.
			// Even if split_half ultimately ships these items, the data was
			// mutated and the operator needs to see that signal.
			e.tel.recordTruncated(ctx, n)
			p, fit, d := tryEncode(e, trimmed, sc)
			if d.count > 0 {
				// Encoding the trimmed clone failed (rare: a codec that
				// rejects the post-clamp content). Fall through to let the
				// next policy try the still-pristine original — do not let a
				// per-policy hiccup poison the chain.
				e.logger.Debug(
					"truncate produced an unencodable batch; falling through",
					zap.String("reason", d.reason),
				)
				continue
			}
			if fit {
				return [][]byte{p}, nil
			}
			// Truncation didn't fit; carry the trimmed batch into the next
			// policy so its work is preserved (split_half operates on the
			// slightly smaller payload).
			current = trimmed

		case oversizeSplitHalf:
			payloads, splitDrops := packSplit(ctx, e, current, sc, 0)
			return payloads, splitDrops

		case oversizeReject:
			return nil, drops{}.addReason(sc.itemCount(current), "reject_policy")
		}
	}

	// Chain exhausted without anything fitting.
	e.logger.Warn(
		"oversize recovery chain exhausted",
		zap.Strings("policies", cfg.Oversize.Policies),
		zap.Int("item_count", sc.itemCount(current)),
	)
	return nil, drops{}.addReason(sc.itemCount(current), "chain_exhausted")
}

// tryEncode marshals + compresses one batch and reports whether it fits the
// MaxRecordSize ceiling. A marshal or compress error returns a drop with the
// appropriate reason; this isolates terminal failures from policy decisions.
func tryEncode[T any](e *kinesisExporter, batch T, sc signalCodec[T]) ([]byte, bool, dropOutcome) {
	raw, err := sc.marshal(batch)
	if err != nil {
		e.logger.Warn("marshal failed", zap.Error(err), zap.Int("item_count", sc.itemCount(batch)))
		return nil, false, dropOutcome{count: sc.itemCount(batch), reason: "marshal_error"}
	}
	payload, err := e.comp.Compress(raw)
	if err != nil {
		e.logger.Warn("compress failed", zap.Error(err), zap.Int("item_count", sc.itemCount(batch)))
		return nil, false, dropOutcome{count: sc.itemCount(batch), reason: "compress_error"}
	}
	fit := len(payload) <= e.cfg.MaxRecordSize
	e.logger.Debug(
		"encode attempt",
		zap.Int("raw_bytes", len(raw)),
		zap.Int("compressed_bytes", len(payload)),
		zap.Int("max_record_size", e.cfg.MaxRecordSize),
		zap.Bool("fit", fit),
		zap.Int("item_count", sc.itemCount(batch)),
	)
	if fit {
		return payload, true, dropOutcome{}
	}
	return nil, false, dropOutcome{}
}

// packSplit is split_half's recursive worker. It halves a batch until each
// piece fits or the MaxAttempts bound is hit, returning the fitting payloads
// and a per-reason drop list. Distinct terminal reasons (irreducible vs
// max_attempts) are kept separate so the operator's telemetry shows which
// lever to pull — collapsing both into a single label would hide max_attempts
// behind "irreducible" or vice versa.
func packSplit[T any](ctx context.Context, e *kinesisExporter, batch T, sc signalCodec[T], depth int) ([][]byte, drops) {
	payload, ok, terminal := tryEncode(e, batch, sc)
	if terminal.count > 0 {
		return nil, drops{}.addReason(terminal.count, terminal.reason)
	}
	if ok {
		return [][]byte{payload}, nil
	}

	if depth >= e.cfg.Oversize.MaxAttempts {
		return nil, drops{}.addReason(sc.itemCount(batch), "max_attempts")
	}

	a, b, ok := sc.splitHalf(batch)
	if !ok {
		return nil, drops{}.addReason(sc.itemCount(batch), "irreducible")
	}
	e.logger.Debug(
		"oversize policy: split_half",
		zap.Int("depth", depth),
		zap.Int("left_items", sc.itemCount(a)),
		zap.Int("right_items", sc.itemCount(b)),
	)
	pa, da := packSplit(ctx, e, a, sc, depth+1)
	pb, db := packSplit(ctx, e, b, sc, depth+1)
	return append(pa, pb...), da.merge(db)
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
