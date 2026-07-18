//go:build ministack

// This integration test validates the `ecs` worker-resolution strategy against
// the MiniStack emulator. MiniStack emulates Kinesis/DynamoDB, not the ECS
// container-metadata endpoint (that is served by the ECS agent), so the test
// stubs `/task` in-process, runs the real resolver, and then proves the
// resolved task id round-trips through MiniStack's DynamoDB as a leaseOwner —
// i.e. the id is valid for the lease store (no ValidationException) and the
// ARN extraction is correct.
//
// Requires MiniStack reachable at MINISTACK_ENDPOINT (default
// http://localhost:4566):
//
//	docker compose -f compose/docker-compose.yaml up -d ministack
//	go test -tags ministack ./receiver/awskinesisreceiver/ -run TestECSStrategyMiniStack -v

package awskinesisreceiver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/lease"
)

func TestECSStrategyMiniStackLeaseOwner(t *testing.T) {
	const taskID = "e2eTASKID0123456789abcdef01234567"

	// Stub the ECS task-metadata endpoint the real resolver will GET.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/task" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"TaskARN":"arn:aws:ecs:us-east-1:000000000000:task/e2e-cluster/` + taskID + `"}`))
	}))
	defer srv.Close()
	t.Setenv(ecsMetadataURIEnvV4, srv.URL)

	ctx := context.Background()

	// 1) The real ecs strategy extracts the task id from the ARN.
	id, err := resolveWorkerID(ctx, &Config{WorkerResolutionStrategy: WorkerStrategyECS})
	if err != nil {
		t.Fatalf("resolveWorkerID(ecs): %v", err)
	}
	if id != taskID {
		t.Fatalf("resolved worker id = %q, want %q", id, taskID)
	}

	// 2) That id must be a valid DynamoDB leaseOwner in MiniStack.
	endpoint := os.Getenv("MINISTACK_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	client, err := newDynamoDBClient(ctx, "us-east-1", endpoint)
	if err != nil {
		t.Fatalf("dynamodb client: %v", err)
	}
	const table = "otel-leases-ecs-validation"
	createLeaseTable(t, ctx, client, table)

	store := lease.NewDynamoDBStore(client, table)
	const shard = "shardId-000000000001"
	if err := store.Ensure(ctx, shard, nil); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	got, err := store.Acquire(ctx, shard, id, 0)
	if err != nil {
		t.Fatalf("Acquire with ecs-resolved id: %v", err)
	}
	if got.Owner != taskID {
		t.Fatalf("Acquire returned leaseOwner %q, want %q", got.Owner, taskID)
	}

	// 3) Read it back from MiniStack to confirm it persisted unchanged.
	leases, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var persisted string
	for _, l := range leases {
		if l.ShardID == shard {
			persisted = l.Owner
		}
	}
	if persisted != taskID {
		t.Fatalf("persisted leaseOwner = %q, want %q", persisted, taskID)
	}
	t.Logf("ecs strategy validated against MiniStack: shard %q leaseOwner=%q", shard, persisted)
}

func createLeaseTable(t *testing.T, ctx context.Context, client *dynamodb.Client, table string) {
	t.Helper()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(table),
		BillingMode: ddbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("leaseKey"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("leaseKey"), KeyType: ddbtypes.KeyTypeHash},
		},
	})
	var inUse *ddbtypes.ResourceInUseException
	if err != nil && !errors.As(err, &inUse) {
		t.Fatalf("create lease table (is MiniStack up at the endpoint?): %v", err)
	}
	if err := dynamodb.NewTableExistsWaiter(client).Wait(ctx,
		&dynamodb.DescribeTableInput{TableName: aws.String(table)}, 30*time.Second); err != nil {
		t.Fatalf("wait for lease table: %v", err)
	}
}
