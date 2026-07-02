package awskinesisexporter

// Oversize-record recovery policy.
//
// This file is pure policy: given a batch that won't fit in one Kinesis
// record, run the operator-configured policy chain (truncate_attribute_values
// → split_half → reject) until something fits or the chain is exhausted.
// The ordering matters and is user-visible config; config.go validates that
// split_half and reject appear only as terminators.
//
// record.go is the mechanism layer: emit, flush, putRecords, and the
// backoff retry loop. The split is intentional — changing how we handle an
// oversize batch (policy) must not require touching how we chunk and send
// fitting payloads (mechanism), and vice versa.

import (
	"context"

	"go.uber.org/zap"
)

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
	// Kinesis meters data + partition-key bytes against the record limit, so
	// the fit check budgets for the key this payload will be shipped under.
	fit := len(payload)+e.cfg.keyOverhead() <= e.cfg.MaxRecordSize
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
