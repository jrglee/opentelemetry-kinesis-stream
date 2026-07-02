package awskinesisexporter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

// These tests drive the factory-built exporter — exporterhelper wrap included —
// against a scripted HTTP server standing in for Kinesis, reached via the
// config's endpoint override. They prove the queue/retry/timeout wiring is
// live, which the middleware-fake unit tests (which bypass the factory) cannot.

// scriptedKinesis serves canned Kinesis JSON responses in order, repeating the
// last one once the script is exhausted.
type scriptedKinesis struct {
	hits      atomic.Int32
	responses []scriptedResponse
}

type scriptedResponse struct {
	status int
	body   string
}

func (s *scriptedKinesis) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		i := int(s.hits.Add(1)) - 1
		if i >= len(s.responses) {
			i = len(s.responses) - 1
		}
		r := s.responses[i]
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		w.WriteHeader(r.status)
		_, _ = w.Write([]byte(r.body))
	}
}

// factoryTestConfig builds a config pointed at the scripted server with a
// blocking queue (wait_for_result) so Consume returns the request's real
// outcome, and a near-zero retry backoff to keep tests fast.
func factoryTestConfig(t *testing.T, endpoint string) *Config {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	cfg.StreamName = "test-stream"
	cfg.Region = "us-east-1"
	cfg.Endpoint = endpoint

	queue := exporterhelper.NewDefaultQueueConfig()
	queue.WaitForResult = true
	cfg.QueueConfig = configoptional.Some(queue)

	cfg.RetryConfig.InitialInterval = time.Millisecond
	cfg.RetryConfig.MaxInterval = 5 * time.Millisecond
	cfg.RetryConfig.MaxElapsedTime = 5 * time.Second
	return cfg
}

func setFakeAWSCreds(t *testing.T) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_SESSION_TOKEN", "test")
}

// TestFactoryExporterRetriesWholeRequest proves the exporterhelper retry
// sender is wired: a whole-request failure the SDK does not retry (HTTP 400
// with an unrecognized type) must be retried by retry_on_failure and succeed
// on the second attempt, surfacing no error to the caller.
func TestFactoryExporterRetriesWholeRequest(t *testing.T) {
	setFakeAWSCreds(t)
	server := &scriptedKinesis{responses: []scriptedResponse{
		{status: http.StatusBadRequest, body: `{"__type":"SomethingTransient","message":"try again"}`},
		{status: http.StatusOK, body: `{"FailedRecordCount":0,"Records":[]}`},
	}}
	ts := httptest.NewServer(server.handler())
	defer ts.Close()

	cfg := factoryTestConfig(t, ts.URL)
	exp, err := NewFactory().CreateTraces(context.Background(), exportertest.NewNopSettings(componentType), cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := exp.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		if err := exp.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	}()

	if err := exp.ConsumeTraces(context.Background(), sampleTraces()); err != nil {
		t.Fatalf("consume should succeed after retry, got: %v", err)
	}
	if got := server.hits.Load(); got != 2 {
		t.Fatalf("PutRecords attempts: got %d want 2 (fail once, retry succeeds)", got)
	}
}

// TestFactoryPermanentErrorNotRetried proves the permanent classification has
// a consumer: ResourceNotFoundException must surface immediately with no
// helper-level retry.
func TestFactoryPermanentErrorNotRetried(t *testing.T) {
	setFakeAWSCreds(t)
	server := &scriptedKinesis{responses: []scriptedResponse{
		{status: http.StatusBadRequest, body: `{"__type":"ResourceNotFoundException","message":"no such stream"}`},
	}}
	ts := httptest.NewServer(server.handler())
	defer ts.Close()

	cfg := factoryTestConfig(t, ts.URL)
	exp, err := NewFactory().CreateTraces(context.Background(), exportertest.NewNopSettings(componentType), cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := exp.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		if err := exp.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	}()

	if err := exp.ConsumeTraces(context.Background(), sampleTraces()); err == nil {
		t.Fatal("consume should surface the permanent error")
	}
	if got := server.hits.Load(); got != 1 {
		t.Fatalf("PutRecords attempts: got %d want 1 (permanent errors are not retried)", got)
	}
}
