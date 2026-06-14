package encoding

import (
	"errors"
	"fmt"

	arrowpb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1" //nolint:revive // imported for protobuf type only
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/arrow_record"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/protobuf/proto"
)

// ErrArrowPanic wraps a panic recovered from the upstream `arrow_record`
// Producer or Consumer. The upstream library raises panics for certain inputs
// it cannot encode/decode (most notably "Too many consecutive schema updates"
// on extreme cardinality). Leaving that panic to propagate would crash the
// whole collector process — on the receiver side it would also cause a
// crashloop because the poisoned record never advances the checkpoint.
// Callers can `errors.Is(err, ErrArrowPanic)` to route the failure (drop the
// record, dead-letter it, count it as an encoder failure) instead of letting
// the panic kill the binary.
var ErrArrowPanic = errors.New("arrow runtime panic")

// otlpArrowTraces implements both encoder and decoder for the OpenTelemetry
// Arrow wire format. Because Kinesis is a store-and-forward transport where
// each record is delivered independently (at-least-once, with dead-letter
// drops), each Marshal call uses a fresh Producer and each Unmarshal call uses
// a fresh Consumer — every Kinesis record carries a fully self-contained
// Arrow batch with its own schema and dictionaries. The cross-batch
// dictionary-delta compression Arrow uses in streaming mode is forfeited; in
// exchange, any single record can be decoded in isolation.
type otlpArrowTraces struct{}

func (otlpArrowTraces) Marshal(td ptrace.Traces) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			out, err = nil, fmt.Errorf("%w: encode traces: %v", ErrArrowPanic, r)
		}
	}()
	p := arrow_record.NewProducer()
	defer func() { _ = p.Close() }()
	bar, perr := p.BatchArrowRecordsFromTraces(td)
	if perr != nil {
		return nil, fmt.Errorf("arrow encode traces: %w", perr)
	}
	return proto.Marshal(bar)
}

func (otlpArrowTraces) Unmarshal(buf []byte) (out ptrace.Traces, err error) {
	defer func() {
		if r := recover(); r != nil {
			out, err = ptrace.Traces{}, fmt.Errorf("%w: decode traces: %v", ErrArrowPanic, r)
		}
	}()
	var bar arrowpb.BatchArrowRecords
	if perr := proto.Unmarshal(buf, &bar); perr != nil {
		return ptrace.Traces{}, fmt.Errorf("arrow proto unmarshal: %w", perr)
	}
	c := arrow_record.NewConsumer()
	defer func() { _ = c.Close() }()
	in, derr := c.TracesFrom(&bar)
	if derr != nil {
		return ptrace.Traces{}, fmt.Errorf("arrow decode traces: %w", derr)
	}
	if len(in) != 1 {
		return ptrace.Traces{}, fmt.Errorf("arrow decode traces: expected 1 batch, got %d", len(in))
	}
	return in[0], nil
}

// otlpArrowMetrics is the metrics counterpart of otlpArrowTraces. It uses the
// same fresh-producer-per-record model, so every record is self-decodable.
type otlpArrowMetrics struct{}

func (otlpArrowMetrics) Marshal(md pmetric.Metrics) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			out, err = nil, fmt.Errorf("%w: encode metrics: %v", ErrArrowPanic, r)
		}
	}()
	p := arrow_record.NewProducer()
	defer func() { _ = p.Close() }()
	bar, perr := p.BatchArrowRecordsFromMetrics(md)
	if perr != nil {
		return nil, fmt.Errorf("arrow encode metrics: %w", perr)
	}
	return proto.Marshal(bar)
}

func (otlpArrowMetrics) Unmarshal(buf []byte) (out pmetric.Metrics, err error) {
	defer func() {
		if r := recover(); r != nil {
			out, err = pmetric.Metrics{}, fmt.Errorf("%w: decode metrics: %v", ErrArrowPanic, r)
		}
	}()
	var bar arrowpb.BatchArrowRecords
	if perr := proto.Unmarshal(buf, &bar); perr != nil {
		return pmetric.Metrics{}, fmt.Errorf("arrow proto unmarshal: %w", perr)
	}
	c := arrow_record.NewConsumer()
	defer func() { _ = c.Close() }()
	in, derr := c.MetricsFrom(&bar)
	if derr != nil {
		return pmetric.Metrics{}, fmt.Errorf("arrow decode metrics: %w", derr)
	}
	if len(in) != 1 {
		return pmetric.Metrics{}, fmt.Errorf("arrow decode metrics: expected 1 batch, got %d", len(in))
	}
	return in[0], nil
}

// otlpArrowLogs is the logs counterpart of otlpArrowTraces. It uses the same
// fresh-producer-per-record model, so every record is self-decodable.
type otlpArrowLogs struct{}

func (otlpArrowLogs) Marshal(ld plog.Logs) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			out, err = nil, fmt.Errorf("%w: encode logs: %v", ErrArrowPanic, r)
		}
	}()
	p := arrow_record.NewProducer()
	defer func() { _ = p.Close() }()
	bar, perr := p.BatchArrowRecordsFromLogs(ld)
	if perr != nil {
		return nil, fmt.Errorf("arrow encode logs: %w", perr)
	}
	return proto.Marshal(bar)
}

func (otlpArrowLogs) Unmarshal(buf []byte) (out plog.Logs, err error) {
	defer func() {
		if r := recover(); r != nil {
			out, err = plog.Logs{}, fmt.Errorf("%w: decode logs: %v", ErrArrowPanic, r)
		}
	}()
	var bar arrowpb.BatchArrowRecords
	if perr := proto.Unmarshal(buf, &bar); perr != nil {
		return plog.Logs{}, fmt.Errorf("arrow proto unmarshal: %w", perr)
	}
	c := arrow_record.NewConsumer()
	defer func() { _ = c.Close() }()
	in, derr := c.LogsFrom(&bar)
	if derr != nil {
		return plog.Logs{}, fmt.Errorf("arrow decode logs: %w", derr)
	}
	if len(in) != 1 {
		return plog.Logs{}, fmt.Errorf("arrow decode logs: expected 1 batch, got %d", len(in))
	}
	return in[0], nil
}
