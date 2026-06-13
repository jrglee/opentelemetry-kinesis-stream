package awskinesisexporter

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// Kinesis PutRecords ceilings: at most 500 records and 5 MiB of aggregate
// record data per call. Flushing honors both so a single ConsumeX never
// overruns the API regardless of how many groups/splits it produced.
const (
	maxRecordsPerPut = 500
	maxBytesPerPut   = 5 << 20
)

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

	entries := make([]types.PutRecordsRequestEntry, 0, len(groups))
	for _, g := range groups {
		key := e.partitionKey(g.key)
		payloads, dropped := pack(g.batch, sc, e.cfg, e.comp, 0)
		if dropped > 0 {
			e.recordsDropped.Add(ctx, int64(dropped), metric.WithAttributes(attribute.String("reason", "oversize")))
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
// 500-record and 5-MiB ceilings. Per-record and transport errors reuse the
// traces path's classification so the Collector's retry policy behaves the same.
func (e *kinesisExporter) flush(ctx context.Context, entries []types.PutRecordsRequestEntry) error {
	for start := 0; start < len(entries); {
		end := start
		bytes := 0
		for end < len(entries) && end-start < maxRecordsPerPut {
			n := len(entries[end].Data)
			if end > start && bytes+n > maxBytesPerPut {
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

// putRecords issues one PutRecords call and surfaces per-record failures the
// same way the original single-record path did.
func (e *kinesisExporter) putRecords(ctx context.Context, records []types.PutRecordsRequestEntry) error {
	if len(records) == 0 {
		return nil
	}
	out, err := e.client.PutRecords(ctx, &kinesis.PutRecordsInput{
		StreamName: aws.String(e.cfg.StreamName),
		Records:    records,
	})
	if err != nil {
		return classifyPutRecordsError(err)
	}
	if out.FailedRecordCount != nil && *out.FailedRecordCount > 0 {
		var throttled, rejected int
		for i, r := range out.Records {
			if r.ErrorCode == nil {
				continue
			}
			e.logger.Warn(
				"kinesis record rejected",
				zap.Int("index", i),
				zap.String("code", aws.ToString(r.ErrorCode)),
				zap.String("message", aws.ToString(r.ErrorMessage)),
			)
			if aws.ToString(r.ErrorCode) == "ProvisionedThroughputExceededException" {
				throttled++
			} else {
				rejected++
			}
		}
		// A record rejected for a non-throttling reason (e.g. a validation
		// error) will fail identically on every retry. Drop and count those so
		// they cannot head-of-line-block the pipeline with an infinite retry.
		if rejected > 0 {
			e.recordsDropped.Add(ctx, int64(rejected), metric.WithAttributes(attribute.String("reason", "rejected")))
		}
		// Retry only if some failures were throttling — those succeed once the
		// shard has capacity. Returning an error re-sends the whole batch (the
		// succeeded records included), which is the accepted at-least-once cost.
		if throttled > 0 {
			return fmt.Errorf("put_records: %d records throttled", throttled)
		}
	}
	return nil
}
