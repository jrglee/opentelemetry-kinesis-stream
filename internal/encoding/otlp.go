package encoding

import (
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

// otlpProtoTraces implements both encoder and decoder for the OTLP protobuf
// wire format. The contrib awskinesisexporter uses the same layout, which is
// what makes wire compatibility with contrib free at this encoding.
type otlpProtoTraces struct{}

func (otlpProtoTraces) Marshal(td ptrace.Traces) ([]byte, error) {
	return ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()
}

func (otlpProtoTraces) Unmarshal(buf []byte) (ptrace.Traces, error) {
	req := ptraceotlp.NewExportRequest()
	if err := req.UnmarshalProto(buf); err != nil {
		return ptrace.Traces{}, err
	}
	return req.Traces(), nil
}

// otlpProtoMetrics is the metrics counterpart of otlpProtoTraces, using the
// same OTLP protobuf layout so the wire stays consistent across signals.
type otlpProtoMetrics struct{}

func (otlpProtoMetrics) Marshal(md pmetric.Metrics) ([]byte, error) {
	return pmetricotlp.NewExportRequestFromMetrics(md).MarshalProto()
}

func (otlpProtoMetrics) Unmarshal(buf []byte) (pmetric.Metrics, error) {
	req := pmetricotlp.NewExportRequest()
	if err := req.UnmarshalProto(buf); err != nil {
		return pmetric.Metrics{}, err
	}
	return req.Metrics(), nil
}

// otlpProtoLogs is the logs counterpart of otlpProtoTraces, using the same
// OTLP protobuf layout so the wire stays consistent across signals.
type otlpProtoLogs struct{}

func (otlpProtoLogs) Marshal(ld plog.Logs) ([]byte, error) {
	return plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
}

func (otlpProtoLogs) Unmarshal(buf []byte) (plog.Logs, error) {
	req := plogotlp.NewExportRequest()
	if err := req.UnmarshalProto(buf); err != nil {
		return plog.Logs{}, err
	}
	return req.Logs(), nil
}
