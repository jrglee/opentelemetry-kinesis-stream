package awskinesisreceiver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

// ECS container metadata endpoint. The ECS agent injects the base URI as an
// environment variable; the endpoint is a link-local address that needs no IAM
// permissions. v4 is preferred; v3 is the older fallback.
const (
	ecsMetadataURIEnvV4 = "ECS_CONTAINER_METADATA_URI_V4"
	ecsMetadataURIEnvV3 = "ECS_CONTAINER_METADATA_URI"
	ecsMetadataTimeout  = 2 * time.Second
)

// resolveWorkerID computes the lease-owner identity per the configured
// strategy. Non-static strategies fail fast rather than falling back, so a
// misconfigured deployment is caught at Start instead of silently minting a
// throwaway identity that cannot reclaim its leases. Config.Validate has
// already rejected unknown strategies and mismatched companion fields; the
// default branch is a belt-and-braces guard.
//
// The empty-check is the choke-point for a load-bearing invariant: an empty
// leaseOwner reads as "unowned" everywhere in the lease store, so every replica
// would claim the shard and deliver it — silent duplicate delivery. Each
// strategy already guards this; the check here stops a future strategy from
// regressing it.
func resolveWorkerID(ctx context.Context, cfg *Config) (string, error) {
	id, err := resolveByStrategy(ctx, cfg)
	if err != nil {
		return "", err
	}
	if id == "" {
		return "", fmt.Errorf("worker_resolution_strategy %q resolved an empty worker id", cfg.WorkerResolutionStrategy)
	}
	// The identity is written to the DynamoDB leaseOwner string attribute, which
	// must be valid UTF-8. The file strategy can read arbitrary bytes; reject an
	// invalid id at Start rather than let every Acquire fail with a validation
	// error, which would silently leave all shards unclaimed.
	if !utf8.ValidString(id) {
		return "", fmt.Errorf("worker_resolution_strategy %q resolved a worker id that is not valid UTF-8", cfg.WorkerResolutionStrategy)
	}
	return id, nil
}

func resolveByStrategy(ctx context.Context, cfg *Config) (string, error) {
	switch cfg.WorkerResolutionStrategy {
	case WorkerStrategyStatic:
		if cfg.WorkerID != "" {
			return cfg.WorkerID, nil
		}
		return "otelcol-" + uuid.NewString(), nil
	case WorkerStrategyHostname:
		host, err := os.Hostname()
		if err != nil {
			return "", fmt.Errorf("hostname strategy: %w", err)
		}
		if host == "" {
			return "", errors.New("hostname strategy: os reported an empty hostname")
		}
		return host, nil
	case WorkerStrategyECS:
		return ecsTaskID(ctx)
	case WorkerStrategyFile:
		return workerIDFromFile(cfg.WorkerIDFile)
	default:
		return "", fmt.Errorf("unknown worker_resolution_strategy %q", cfg.WorkerResolutionStrategy)
	}
}

// ecsTaskID fetches the task ID from the ECS container metadata endpoint. The
// task ARN's final path segment is the task ID — globally unique and stable
// for the life of the task, so an in-place restart reclaims its own leases.
func ecsTaskID(ctx context.Context) (string, error) {
	base := os.Getenv(ecsMetadataURIEnvV4)
	if base == "" {
		base = os.Getenv(ecsMetadataURIEnvV3)
	}
	if base == "" {
		return "", fmt.Errorf("ecs strategy: neither %s nor %s is set (not running under ECS, or the agent predates metadata v3)",
			ecsMetadataURIEnvV4, ecsMetadataURIEnvV3)
	}

	ctx, cancel := context.WithTimeout(ctx, ecsMetadataTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/task", nil)
	if err != nil {
		return "", fmt.Errorf("ecs strategy: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("ecs strategy: metadata request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("ecs strategy: metadata endpoint returned %s", resp.Status)
	}

	var meta struct {
		TaskARN string `json:"TaskARN"`
	}
	// Bound the read: the metadata document is small, but this decodes an
	// external HTTP response, so cap it rather than trust the sender's length.
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&meta); err != nil {
		return "", fmt.Errorf("ecs strategy: decode task metadata: %w", err)
	}
	id := meta.TaskARN[strings.LastIndex(meta.TaskARN, "/")+1:]
	if id == "" {
		return "", fmt.Errorf("ecs strategy: could not extract task id from ARN %q", meta.TaskARN)
	}
	return id, nil
}

// workerIDFromFile reads a stable identity that a platform projects onto a
// file rather than an environment variable.
func workerIDFromFile(path string) (string, error) {
	if path == "" {
		return "", errors.New("file strategy: worker_id_file is empty")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("file strategy: %w", err)
	}
	id := strings.TrimSpace(string(raw))
	if id == "" {
		return "", fmt.Errorf("file strategy: %s is empty", path)
	}
	return id, nil
}
