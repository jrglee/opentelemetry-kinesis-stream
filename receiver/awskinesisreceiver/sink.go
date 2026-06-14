package awskinesisreceiver

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/encoding"
)

// sink is the only signal-specific seam in the receiver. The lease store,
// coordinator, and pollers all operate on raw bytes; a sink decodes a
// decompressed payload into its signal and delivers it downstream, and wraps
// an unprocessable raw record back into the pipeline as a dead-letter. One
// sink instance is shared across all shard pollers (it is stateless).
type sink interface {
	// consume decodes payload and delivers it. The second return is true when
	// the failure was a decode failure, so the poller can dead-letter the
	// unprocessable bytes; the recordResult drives checkpoint advancement.
	consume(ctx context.Context, payload []byte) (recordResult, bool)
	// deadLetter re-emits a failed raw record into this signal's pipeline.
	deadLetter(ctx context.Context, rec types.Record, failureClass, encName, codecName string) error
}

// tracesSink delivers decoded traces and dead-letters failures as spans.
type tracesSink struct {
	decoder  encoding.TracesDecoder
	consumer consumer.Traces
}

func (s tracesSink) consume(ctx context.Context, payload []byte) (recordResult, bool) {
	td, err := s.decoder.Unmarshal(payload)
	if err != nil {
		return recordSkip, true
	}
	if err := s.consumer.ConsumeTraces(ctx, td); err != nil {
		if consumererror.IsPermanent(err) {
			return recordSkip, false
		}
		return recordRetry, false
	}
	return recordOK, false
}

func (s tracesSink) deadLetter(ctx context.Context, rec types.Record, failureClass, encName, codecName string) error {
	return s.consumer.ConsumeTraces(ctx, deadLetterTraces(rec, failureClass, encName, codecName))
}

// metricsSink delivers decoded metrics and dead-letters failures as a gauge.
type metricsSink struct {
	decoder  encoding.MetricsDecoder
	consumer consumer.Metrics
}

func (s metricsSink) consume(ctx context.Context, payload []byte) (recordResult, bool) {
	md, err := s.decoder.Unmarshal(payload)
	if err != nil {
		return recordSkip, true
	}
	if err := s.consumer.ConsumeMetrics(ctx, md); err != nil {
		if consumererror.IsPermanent(err) {
			return recordSkip, false
		}
		return recordRetry, false
	}
	return recordOK, false
}

func (s metricsSink) deadLetter(ctx context.Context, rec types.Record, failureClass, encName, codecName string) error {
	return s.consumer.ConsumeMetrics(ctx, deadLetterMetrics(rec, failureClass, encName, codecName))
}

// logsSink delivers decoded logs and dead-letters failures as a log record.
type logsSink struct {
	decoder  encoding.LogsDecoder
	consumer consumer.Logs
}

func (s logsSink) consume(ctx context.Context, payload []byte) (recordResult, bool) {
	ld, err := s.decoder.Unmarshal(payload)
	if err != nil {
		return recordSkip, true
	}
	if err := s.consumer.ConsumeLogs(ctx, ld); err != nil {
		if consumererror.IsPermanent(err) {
			return recordSkip, false
		}
		return recordRetry, false
	}
	return recordOK, false
}

func (s logsSink) deadLetter(ctx context.Context, rec types.Record, failureClass, encName, codecName string) error {
	return s.consumer.ConsumeLogs(ctx, deadLetterLogs(rec, failureClass, encName, codecName))
}
