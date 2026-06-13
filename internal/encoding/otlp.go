package encoding

import (
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
