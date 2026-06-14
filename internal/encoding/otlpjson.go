package encoding

import (
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

// otlpJSONTraces implements both encoder and decoder for the OTLP JSON wire
// format. JSON is human-readable and broadly interoperable; it is more verbose
// than the proto encoding, so pairing it with a codec (e.g. zstd) is advised.
type otlpJSONTraces struct{}

func (otlpJSONTraces) Marshal(td ptrace.Traces) ([]byte, error) {
	return ptraceotlp.NewExportRequestFromTraces(td).MarshalJSON()
}

func (otlpJSONTraces) Unmarshal(buf []byte) (ptrace.Traces, error) {
	req := ptraceotlp.NewExportRequest()
	if err := req.UnmarshalJSON(buf); err != nil {
		return ptrace.Traces{}, err
	}
	return req.Traces(), nil
}

// otlpJSONMetrics is the metrics counterpart of otlpJSONTraces, using the same
// OTLP JSON layout so the wire stays consistent across signals.
type otlpJSONMetrics struct{}

func (otlpJSONMetrics) Marshal(md pmetric.Metrics) ([]byte, error) {
	return pmetricotlp.NewExportRequestFromMetrics(md).MarshalJSON()
}

func (otlpJSONMetrics) Unmarshal(buf []byte) (pmetric.Metrics, error) {
	req := pmetricotlp.NewExportRequest()
	if err := req.UnmarshalJSON(buf); err != nil {
		return pmetric.Metrics{}, err
	}
	return req.Metrics(), nil
}

// otlpJSONLogs is the logs counterpart of otlpJSONTraces, using the same
// OTLP JSON layout so the wire stays consistent across signals.
type otlpJSONLogs struct{}

func (otlpJSONLogs) Marshal(ld plog.Logs) ([]byte, error) {
	return plogotlp.NewExportRequestFromLogs(ld).MarshalJSON()
}

func (otlpJSONLogs) Unmarshal(buf []byte) (plog.Logs, error) {
	req := plogotlp.NewExportRequest()
	if err := req.UnmarshalJSON(buf); err != nil {
		return plog.Logs{}, err
	}
	return req.Logs(), nil
}
