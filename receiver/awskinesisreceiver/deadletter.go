package awskinesisreceiver

import (
	"encoding/base64"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// deadLetterName is the span/metric name carrying an unprocessable record back
// into the pipeline. Operators route on it with standard components (filter,
// routing connector) to send failures wherever they want.
const deadLetterName = "kinesis.dead_letter"

// deadLetterAttrs sets the wrapper attributes shared by both signals: the raw
// (still-compressed) record bytes plus enough context to investigate.
func deadLetterAttrs(set func(k, v string), rec types.Record, failureClass, encName, codecName string) {
	set("kinesis.raw", base64.StdEncoding.EncodeToString(rec.Data))
	set("kinesis.sequence", aws.ToString(rec.SequenceNumber))
	set("kinesis.partition_key", aws.ToString(rec.PartitionKey))
	set("kinesis.failure_class", failureClass)
	set("kinesis.encoding", encName)
	set("kinesis.codec", codecName)
}

// deadLetterTraces wraps a failed raw record as a single span so a traces
// pipeline can carry it.
func deadLetterTraces(rec types.Record, failureClass, encName, codecName string) ptrace.Traces {
	td := ptrace.NewTraces()
	span := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName(deadLetterName)
	deadLetterAttrs(func(k, v string) { span.Attributes().PutStr(k, v) }, rec, failureClass, encName, codecName)
	return td
}

// deadLetterMetrics wraps a failed raw record as a gauge data point (value 1)
// so a metrics pipeline can carry it.
func deadLetterMetrics(rec types.Record, failureClass, encName, codecName string) pmetric.Metrics {
	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName(deadLetterName)
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetIntValue(1)
	deadLetterAttrs(func(k, v string) { dp.Attributes().PutStr(k, v) }, rec, failureClass, encName, codecName)
	return md
}
