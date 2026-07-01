//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// requireDocker skips the test if the docker CLI is absent. The E2E is opt-in
// via the `e2e` build tag, but a missing daemon should skip, not hard-fail.
func requireDocker(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}
}

// composeEnv builds the environment for compose invocations, including a
// docker credential-helper workaround for machines whose ~/.docker/config.json
// points at an unavailable helper (the /tmp/dockercfg shim, if present, is
// used when DOCKER_CONFIG is unset).
func composeEnv() []string {
	env := os.Environ()
	if os.Getenv("DOCKER_CONFIG") == "" {
		if _, err := os.Stat("/tmp/dockercfg/config.json"); err == nil {
			env = append(env, "DOCKER_CONFIG=/tmp/dockercfg")
		}
	}
	return env
}

// copyShared pulls the consumer output files off the shared named volume into
// a host directory via `docker compose cp`. A host bind mount would be simpler
// but colima does not reliably surface container writes back to the macOS
// host, so the volume + cp path is used instead. Both consumers mount the same
// volume, so copying from consumer-a captures both out-a and out-b. Errors are
// non-fatal: the files may not exist on early polls.
func copyShared(t *testing.T, env []string, dest string) {
	t.Helper()
	if out, err := compose(t, env, 30*time.Second, "cp", "consumer-a:/shared/.", dest); err != nil {
		t.Logf("cp shared (not ready yet?): %v\n%s", err, out)
	}
}

func composeDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve caller for compose dir")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "compose")
}

func compose(t *testing.T, env []string, timeout time.Duration, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	full := append([]string{"compose", "-f", "docker-compose.yaml"}, args...)
	cmd := exec.CommandContext(ctx, "docker", full...)
	cmd.Dir = composeDir(t)
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// waitForBalancedOwnership polls the DynamoDB lease table (via MiniStack on
// localhost:4566) until shard ownership has rebalanced to an even split across
// distinct workers and that assignment is stable across consecutive reads.
// "Even" means every shard is owned and the busiest and idlest worker differ
// by at most one shard. Stability (two consecutive identical snapshots) ensures
// rebalancing has converged so no steal happens during the subsequent emission.
func waitForBalancedOwnership(t *testing.T) {
	t.Helper()
	client := dynamoClient(t)
	deadline := time.Now().Add(settleDeadline)
	stable := 0
	last := ""
	for time.Now().Before(deadline) {
		owners, owned, total := scanOwners(t, client)
		if total >= 2 && owned >= total && len(owners) >= 2 && balanced(owners) {
			sig := ownersSignature(owners)
			if sig == last {
				stable++
				if stable >= 2 {
					t.Logf("balanced ownership converged: %s", sig)
					return
				}
			} else {
				stable, last = 0, sig
			}
		} else {
			stable, last = 0, ""
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatal("timed out waiting for balanced shard ownership across replicas")
}

// balanced reports whether the busiest and idlest owner differ by at most one
// shard (an even-as-possible fair-share split).
func balanced(owners map[string]int) bool {
	if len(owners) == 0 {
		return false
	}
	minN, maxN := math.MaxInt, 0
	for _, n := range owners {
		if n < minN {
			minN = n
		}
		if n > maxN {
			maxN = n
		}
	}
	return maxN-minN <= 1
}

// ownersSignature is a stable string of the owner->count map for comparing
// snapshots across polls.
func ownersSignature(owners map[string]int) string {
	ks := keys(owners)
	sort.Strings(ks)
	var b strings.Builder
	for _, k := range ks {
		fmt.Fprintf(&b, "%s=%d;", k, owners[k])
	}
	return b.String()
}

// waitForProducer blocks until the producer's OTLP gRPC port (published to the
// host) accepts a TCP connection, so telemetrygen does not blast spans before
// the listener is bound.
func waitForProducer(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(settleDeadline)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", "localhost:4317", 2*time.Second)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatal("timed out waiting for producer OTLP listener on localhost:4317")
}

// scanOwners returns the distinct non-empty owners, the count of leases with
// an owner, and the total lease count.
func scanOwners(t *testing.T, client *dynamodb.Client) (owners map[string]int, owned, total int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String("otel-leases")})
	if err != nil {
		return nil, 0, 0 // table may not exist yet
	}
	owners = make(map[string]int)
	for _, item := range out.Items {
		if v, ok := item["leaseOwner"].(*ddbtypes.AttributeValueMemberS); ok && v.Value != "" {
			owners[v.Value]++
			owned++
		}
	}
	return owners, owned, len(out.Items)
}

func dynamoClient(t *testing.T) *dynamodb.Client {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion("us-east-1"))
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://localhost:4566")
	})
}

func keys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// atLeastOnceDupTolerance bounds the duplicate deliveries a multi-replica test
// accepts. The receiver is at-least-once: a bootstrap steal during fair-share
// rebalance re-delivers the stolen shard's in-flight window (records since its
// last checkpoint), so a small overage is correct, not a bug. The bound stays
// well under a whole shard's worth (~half the records here) so gross double
// delivery — a shard delivered to two replicas for its whole life — still fails.
func atLeastOnceDupTolerance(expected int) int {
	if t := expected / 10; t > 5 {
		return t
	}
	return 5
}
