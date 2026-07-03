// poller_records.go: per-record decode and dead-letter routing.
//
// handleRecord decompresses and delivers one Kinesis record to the downstream
// sink; maybeDeadLetter wraps unprocessable bytes for observability when
// dead-lettering is configured. The recordResult contract drives the poll
// loop's checkpoint-advance decision: only recordRetry withholds the
// checkpoint, ensuring a transient downstream rejection does not silently
// drop valid telemetry. recordSkip (permanently unprocessable, dead-lettered
// or disabled) lets the checkpoint advance past the undeliverable record.

package awskinesisreceiver

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"go.uber.org/zap"
)

// recordResult tells the poll loop whether the checkpoint may advance past a
// record. recordRetry means a valid record was transiently rejected and must
// be re-read; recordSkip and recordOK both let the checkpoint advance.
type recordResult int

const (
	recordOK    recordResult = iota // delivered downstream
	recordSkip                      // permanently unprocessable — safe to skip
	recordRetry                     // transient downstream failure — must re-read
)

func (p *shardPoller) handleRecord(ctx context.Context, rec types.Record) recordResult {
	raw, err := p.comp.Decompress(rec.Data)
	if err != nil {
		p.logger.Warn(
			"decompress failed; skipping record",
			zap.String("shard", p.shardID()),
			zap.String("seq", aws.ToString(rec.SequenceNumber)),
			zap.Error(err),
		)
		if !p.maybeDeadLetter(ctx, rec, "decompress") {
			return recordRetry
		}
		return recordSkip
	}
	// The sink performs the only signal-specific work: decode + deliver. It
	// reports a decode failure so the unprocessable bytes can be dead-lettered;
	// a transient consume failure becomes recordRetry so the checkpoint does
	// not advance past valid telemetry the downstream merely rejected.
	result, decodeFailed := p.sink.consume(ctx, raw)
	switch {
	case decodeFailed:
		p.logger.Warn(
			"decode failed; skipping record",
			zap.String("shard", p.shardID()),
			zap.String("seq", aws.ToString(rec.SequenceNumber)),
		)
		if !p.maybeDeadLetter(ctx, rec, "decode") {
			return recordRetry
		}
	case result == recordRetry:
		p.logger.Warn(
			"consume failed; will retry record",
			zap.String("shard", p.shardID()),
			zap.String("seq", aws.ToString(rec.SequenceNumber)),
		)
	case result == recordSkip:
		p.logger.Warn(
			"consume permanently rejected; skipping record",
			zap.String("shard", p.shardID()),
			zap.String("seq", aws.ToString(rec.SequenceNumber)),
		)
	}
	return result
}

// maybeDeadLetter re-emits an unprocessable raw record into the pipeline when
// dead-lettering is enabled, so the bytes are observable rather than silently
// dropped. It reports whether the checkpoint may advance past the record: true
// when dead-lettering is disabled (skipping is the configured behavior) or the
// wrapper was accepted downstream; false when the emit failed — advancing then
// would lose the bytes entirely, so the caller must re-read the record and
// re-attempt the dead-letter. A persistently failing dead-letter pipeline
// therefore wedges the shard exactly like a persistently rejecting downstream:
// bounded by the stuck backoff and visible through its warning and the
// dead_letter counter, rather than a silent drop.
func (p *shardPoller) maybeDeadLetter(ctx context.Context, rec types.Record, failureClass string) bool {
	if !p.cfg.DeadLetter.Enabled {
		return true
	}
	if err := p.sink.deadLetter(ctx, rec, failureClass, string(p.cfg.Encoding), string(p.cfg.Compression)); err != nil {
		p.tel.recordDeadLetter(ctx, resultError)
		p.logger.Warn(
			"dead-letter emit failed; record will be re-read",
			zap.String("shard", p.shardID()),
			zap.String("seq", aws.ToString(rec.SequenceNumber)),
			zap.Error(err),
		)
		return false
	}
	p.tel.recordDeadLetter(ctx, resultSuccess)
	return true
}
