package awskinesisreceiver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveWorkerIDStatic(t *testing.T) {
	t.Run("explicit worker_id is used verbatim", func(t *testing.T) {
		cfg := &Config{WorkerResolutionStrategy: WorkerStrategyStatic, WorkerID: "replica-a"}
		got, err := resolveWorkerID(context.Background(), cfg)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if got != "replica-a" {
			t.Fatalf("got %q want replica-a", got)
		}
	})

	t.Run("empty worker_id mints a distinct random id", func(t *testing.T) {
		cfg := &Config{WorkerResolutionStrategy: WorkerStrategyStatic}
		first, err := resolveWorkerID(context.Background(), cfg)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if !strings.HasPrefix(first, "otelcol-") {
			t.Fatalf("got %q want otelcol- prefix", first)
		}
		second, err := resolveWorkerID(context.Background(), cfg)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if first == second {
			t.Fatalf("expected distinct random ids, got %q twice", first)
		}
	})
}

func TestResolveWorkerIDHostname(t *testing.T) {
	want, err := os.Hostname()
	if err != nil {
		t.Skipf("os.Hostname unavailable: %v", err)
	}
	cfg := &Config{WorkerResolutionStrategy: WorkerStrategyHostname}
	got, err := resolveWorkerID(context.Background(), cfg)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestResolveWorkerIDFile(t *testing.T) {
	t.Run("trims surrounding whitespace", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "id")
		if err := os.WriteFile(path, []byte("  task-123\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg := &Config{WorkerResolutionStrategy: WorkerStrategyFile, WorkerIDFile: path}
		got, err := resolveWorkerID(context.Background(), cfg)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if got != "task-123" {
			t.Fatalf("got %q want task-123", got)
		}
	})

	t.Run("missing file errors", func(t *testing.T) {
		cfg := &Config{WorkerResolutionStrategy: WorkerStrategyFile, WorkerIDFile: filepath.Join(t.TempDir(), "absent")}
		if _, err := resolveWorkerID(context.Background(), cfg); err == nil {
			t.Fatal("expected error for a missing file")
		}
	})

	t.Run("empty file errors", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "id")
		if err := os.WriteFile(path, []byte("   \n"), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg := &Config{WorkerResolutionStrategy: WorkerStrategyFile, WorkerIDFile: path}
		if _, err := resolveWorkerID(context.Background(), cfg); err == nil {
			t.Fatal("expected error for a whitespace-only file")
		}
	})

	t.Run("invalid UTF-8 content errors", func(t *testing.T) {
		// A non-empty but invalid-UTF-8 id would pass the empty check yet be
		// rejected by DynamoDB on every Acquire; the choke-point must fail Start.
		path := filepath.Join(t.TempDir(), "id")
		if err := os.WriteFile(path, []byte{0xff, 0xfe, 0xfd}, 0o600); err != nil {
			t.Fatal(err)
		}
		cfg := &Config{WorkerResolutionStrategy: WorkerStrategyFile, WorkerIDFile: path}
		if _, err := resolveWorkerID(context.Background(), cfg); err == nil {
			t.Fatal("expected error for invalid UTF-8 file content")
		}
	})
}

func TestResolveWorkerIDECS(t *testing.T) {
	cfg := &Config{WorkerResolutionStrategy: WorkerStrategyECS}

	t.Run("extracts the task id from the task ARN", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/task" {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write([]byte(`{"TaskARN":"arn:aws:ecs:us-east-1:111122223333:task/my-cluster/abc123def456"}`))
		}))
		defer srv.Close()
		t.Setenv(ecsMetadataURIEnvV4, srv.URL)

		got, err := resolveWorkerID(context.Background(), cfg)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if got != "abc123def456" {
			t.Fatalf("got %q want abc123def456", got)
		}
	})

	t.Run("falls back to the v3 metadata env var", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"TaskARN":"arn:aws:ecs:us-east-1:111122223333:task/v3task"}`))
		}))
		defer srv.Close()
		t.Setenv(ecsMetadataURIEnvV4, "")
		t.Setenv(ecsMetadataURIEnvV3, srv.URL)

		got, err := resolveWorkerID(context.Background(), cfg)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if got != "v3task" {
			t.Fatalf("got %q want v3task", got)
		}
	})

	t.Run("missing metadata env errors", func(t *testing.T) {
		t.Setenv(ecsMetadataURIEnvV4, "")
		t.Setenv(ecsMetadataURIEnvV3, "")
		if _, err := resolveWorkerID(context.Background(), cfg); err == nil {
			t.Fatal("expected error when no metadata endpoint is set")
		}
	})

	t.Run("non-200 response errors", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "boom", http.StatusInternalServerError)
		}))
		defer srv.Close()
		t.Setenv(ecsMetadataURIEnvV4, srv.URL)
		if _, err := resolveWorkerID(context.Background(), cfg); err == nil {
			t.Fatal("expected error on a non-200 response")
		}
	})

	t.Run("malformed json errors", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`not json`))
		}))
		defer srv.Close()
		t.Setenv(ecsMetadataURIEnvV4, srv.URL)
		if _, err := resolveWorkerID(context.Background(), cfg); err == nil {
			t.Fatal("expected error on malformed metadata")
		}
	})

	t.Run("empty task ARN errors", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"TaskARN":""}`))
		}))
		defer srv.Close()
		t.Setenv(ecsMetadataURIEnvV4, srv.URL)
		if _, err := resolveWorkerID(context.Background(), cfg); err == nil {
			t.Fatal("expected error on an empty TaskARN")
		}
	})
}
