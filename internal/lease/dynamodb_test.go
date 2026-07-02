package lease

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
)

// TestMarshalUnmarshalRoundTrip pins the KCL attribute layout: a round trip
// through marshalLease/unmarshalLease must preserve every field, including the
// comma-joined multi-parent encoding that KCL itself reads.
func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		in   Lease
	}{
		{"owned single parent", Lease{ShardID: "s-1", Owner: "w-1", Counter: 7, Checkpoint: "49625-abc", ParentIDs: []string{"p-0"}}},
		{"owned multi parent", Lease{ShardID: "m-2", Owner: "w-9", Counter: 3, Checkpoint: CheckpointShardEnd, ParentIDs: []string{"p-0", "p-1"}}},
		{"unowned no parents", Lease{ShardID: "s-0", Counter: 0, Checkpoint: CheckpointTrimHorizon}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := unmarshalLease(marshalLease(tc.in))
			if err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if !reflect.DeepEqual(got, tc.in) {
				t.Fatalf("round trip = %+v want %+v", got, tc.in)
			}
		})
	}
}

// TestMarshalParentsAsStringSet pins the wire type: KCL's
// DynamoDBLeaseSerializer stores parentShardId as a DynamoDB string set, and
// a stock KCL reading any other type deserializes zero parents — silently
// dropping its parent-before-child reshard barrier.
func TestMarshalParentsAsStringSet(t *testing.T) {
	item := marshalLease(Lease{ShardID: "c-1", Checkpoint: CheckpointTrimHorizon, ParentIDs: []string{"p-0", "p-1"}})
	ss, ok := item[attrParentShardID].(*types.AttributeValueMemberSS)
	if !ok {
		t.Fatalf("parentShardId must be a string set (SS), got %T", item[attrParentShardID])
	}
	if !reflect.DeepEqual(ss.Value, []string{"p-0", "p-1"}) {
		t.Fatalf("parents = %v want [p-0 p-1]", ss.Value)
	}
}

// TestUnmarshalReadsLegacyCommaJoinedParents keeps rows written before the
// string-set change readable: the table migrates in place as rows rewrite.
func TestUnmarshalReadsLegacyCommaJoinedParents(t *testing.T) {
	legacy := map[string]types.AttributeValue{
		attrLeaseKey:      &types.AttributeValueMemberS{Value: "c-1"},
		attrParentShardID: &types.AttributeValueMemberS{Value: "p-0,p-1"},
	}
	got, err := unmarshalLease(legacy)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(got.ParentIDs, []string{"p-0", "p-1"}) {
		t.Fatalf("parents = %v want [p-0 p-1]", got.ParentIDs)
	}
}

// TestMarshalOmitsEmptyOwnerAndParents matches KCL's "unowned" row shape: an
// empty owner and an empty parent list must omit the attributes entirely
// rather than writing empty strings, so a KCL consumer reads the row as
// genuinely unowned with no lineage.
func TestMarshalOmitsEmptyOwnerAndParents(t *testing.T) {
	item := marshalLease(Lease{ShardID: "s-0", Checkpoint: CheckpointTrimHorizon})
	if _, ok := item[attrLeaseOwner]; ok {
		t.Errorf("empty owner should be omitted, got %v", item[attrLeaseOwner])
	}
	if _, ok := item[attrParentShardID]; ok {
		t.Errorf("empty parents should be omitted, got %v", item[attrParentShardID])
	}
}

// TestUnmarshalDefaultsMissingColumns covers KCL rows that omit columns we
// treat as optional: only leaseKey is mandatory; everything else has a defined
// zero so a partial row is still readable.
func TestUnmarshalDefaultsMissingColumns(t *testing.T) {
	bare := map[string]types.AttributeValue{
		attrLeaseKey: &types.AttributeValueMemberS{Value: "s-0"},
	}
	got, err := unmarshalLease(bare)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	want := Lease{ShardID: "s-0"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

func TestUnmarshalRejectsMissingLeaseKey(t *testing.T) {
	if _, err := unmarshalLease(map[string]types.AttributeValue{}); err == nil {
		t.Fatal("expected error for row missing leaseKey")
	}
}

func TestUnmarshalRejectsNonNumericCounter(t *testing.T) {
	item := map[string]types.AttributeValue{
		attrLeaseKey:     &types.AttributeValueMemberS{Value: "s-0"},
		attrLeaseCounter: &types.AttributeValueMemberN{Value: "not-a-number"},
	}
	if _, err := unmarshalLease(item); err == nil {
		t.Fatal("expected error for non-numeric leaseCounter")
	}
}

func TestIsConditionalCheckFailed(t *testing.T) {
	if isConditionalCheckFailed(nil) {
		t.Error("nil error reported as conditional check failure")
	}
	if isConditionalCheckFailed(errors.New("throttled")) {
		t.Error("generic error reported as conditional check failure")
	}
	ccf := &types.ConditionalCheckFailedException{}
	if !isConditionalCheckFailed(ccf) {
		t.Error("ConditionalCheckFailedException not recognized")
	}
}

// --- DynamoDB client-path tests (smithy Finalize middleware fake) ----------
//
// The tests below drive the real *dynamodb.Client through the full SDK
// serialization path but short-circuit at the Finalize layer, so no HTTP
// transport is ever reached. This mirrors the pattern used by the kinesis
// exporter tests (ADR-0004): per-operation response queues are populated
// before the call, and the middleware pops one entry per DynamoDB API call.

// ddbResponse is a single scripted outcome for one DynamoDB API call.
type ddbResponse struct {
	out interface{} // typed DynamoDB output (*dynamodb.GetItemOutput, etc.)
	err error
}

// fakeDynamo holds per-operation queues (keyed by DynamoDB operation name:
// "GetItem", "UpdateItem", "PutItem", "DeleteItem", "Scan"). Results are
// consumed in FIFO order. A call with an empty queue panics immediately so
// test ordering bugs surface loudly.
type fakeDynamo struct {
	queues map[string][]ddbResponse
}

func newFakeDynamo() *fakeDynamo {
	return &fakeDynamo{queues: make(map[string][]ddbResponse)}
}

func (f *fakeDynamo) enqueue(op string, out interface{}, err error) {
	f.queues[op] = append(f.queues[op], ddbResponse{out: out, err: err})
}

// inject returns a func(*dynamodb.Options) that registers a Finalize
// middleware that short-circuits every DynamoDB call. middleware.GetOperationName
// pulls the operation name set by the SDK via middleware.WithOperationName in
// api_client.go, which gives us "GetItem", "UpdateItem", etc.
func (f *fakeDynamo) inject() func(*dynamodb.Options) {
	return func(o *dynamodb.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Finalize.Add(
				middleware.FinalizeMiddlewareFunc("fakeDynamo",
					func(ctx context.Context, _ middleware.FinalizeInput, _ middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
						op := middleware.GetOperationName(ctx)
						q := f.queues[op]
						if len(q) == 0 {
							panic("fakeDynamo: unexpected call to " + op)
						}
						r := q[0]
						f.queues[op] = q[1:]
						if r.err != nil {
							return middleware.FinalizeOutput{}, middleware.Metadata{}, r.err
						}
						return middleware.FinalizeOutput{Result: r.out}, middleware.Metadata{}, nil
					}),
				middleware.Before,
			)
		})
	}
}

// newFakeStore wires a DynamoDBStore to the given fake. Static credentials
// and a fixed region prevent the SDK from touching the default credential
// chain or IMDS during tests.
func newFakeStore(t *testing.T, f *fakeDynamo) *DynamoDBStore {
	t.Helper()
	client := dynamodb.New(dynamodb.Options{
		Region:      "us-east-1",
		Credentials: aws.AnonymousCredentials{},
	}, f.inject())
	return NewDynamoDBStore(client, "test-table")
}

// fakeUpdateOutput builds an UpdateItemOutput whose Attributes field contains
// the marshaled form of l. update() calls unmarshalLease(resp.Attributes), so
// this is the correct shape for a successful conditional update.
func fakeUpdateOutput(l Lease) *dynamodb.UpdateItemOutput {
	return &dynamodb.UpdateItemOutput{Attributes: marshalLease(l)}
}

func fakeGetItemFound(l Lease) *dynamodb.GetItemOutput {
	return &dynamodb.GetItemOutput{Item: marshalLease(l)}
}

func fakeGetItemMissing() *dynamodb.GetItemOutput {
	return &dynamodb.GetItemOutput{} // empty Item → rowExists returns false
}

// TestUpdateDisambiguatesConflictVsNotFound verifies that a
// ConditionalCheckFailedException from UpdateItem is classified as
// ErrLeaseConflict when the row still exists, or ErrLeaseNotFound when the
// row is gone. The follow-up GetItem (in rowExists) is what makes the
// distinction — DynamoDB itself does not tell us which case it was.
func TestUpdateDisambiguatesConflictVsNotFound(t *testing.T) {
	t.Run("row exists → ErrLeaseConflict", func(t *testing.T) {
		f := newFakeDynamo()
		f.enqueue("UpdateItem", nil, &types.ConditionalCheckFailedException{})
		f.enqueue("GetItem", fakeGetItemFound(Lease{
			ShardID:    "s-1",
			Owner:      "other",
			Counter:    5,
			Checkpoint: CheckpointTrimHorizon,
		}), nil)

		_, err := newFakeStore(t, f).Acquire(context.Background(), "s-1", "me", 4)
		if !errors.Is(err, ErrLeaseConflict) {
			t.Fatalf("want ErrLeaseConflict, got %v", err)
		}
	})

	t.Run("row gone → ErrLeaseNotFound", func(t *testing.T) {
		f := newFakeDynamo()
		f.enqueue("UpdateItem", nil, &types.ConditionalCheckFailedException{})
		f.enqueue("GetItem", fakeGetItemMissing(), nil)

		_, err := newFakeStore(t, f).Acquire(context.Background(), "s-1", "me", 0)
		if !errors.Is(err, ErrLeaseNotFound) {
			t.Fatalf("want ErrLeaseNotFound, got %v", err)
		}
	})
}

// TestAcquire covers the two Acquire outcomes: a stale counter is rejected
// (counter raced ahead between List and Acquire → ErrLeaseConflict), and a
// matching counter succeeds and returns the lease with a bumped counter and
// the caller as the new owner.
func TestAcquire(t *testing.T) {
	t.Run("successful acquire returns bumped counter", func(t *testing.T) {
		f := newFakeDynamo()
		want := Lease{ShardID: "s-1", Owner: "me", Counter: 3, Checkpoint: CheckpointTrimHorizon}
		f.enqueue("UpdateItem", fakeUpdateOutput(want), nil)

		got, err := newFakeStore(t, f).Acquire(context.Background(), "s-1", "me", 2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	})

	t.Run("stale counter → ErrLeaseConflict", func(t *testing.T) {
		f := newFakeDynamo()
		f.enqueue("UpdateItem", nil, &types.ConditionalCheckFailedException{})
		f.enqueue("GetItem", fakeGetItemFound(Lease{
			ShardID:    "s-1",
			Owner:      "other",
			Counter:    3,
			Checkpoint: CheckpointTrimHorizon,
		}), nil)

		_, err := newFakeStore(t, f).Acquire(context.Background(), "s-1", "me", 2)
		if !errors.Is(err, ErrLeaseConflict) {
			t.Fatalf("want ErrLeaseConflict, got %v", err)
		}
	})
}

// TestDynamoDBDelete covers the three Delete outcomes defined by the Store contract:
//   - correct counter  → nil (clean delete)
//   - stale counter    → ErrLeaseConflict (active lease must not be GC'd)
//   - missing row      → nil (idempotent; cleanup is safe to retry)
func TestDynamoDBDelete(t *testing.T) {
	t.Run("correct counter → nil", func(t *testing.T) {
		f := newFakeDynamo()
		f.enqueue("DeleteItem", &dynamodb.DeleteItemOutput{}, nil)

		if err := newFakeStore(t, f).Delete(context.Background(), "s-1", 5); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("stale counter → ErrLeaseConflict", func(t *testing.T) {
		f := newFakeDynamo()
		f.enqueue("DeleteItem", nil, &types.ConditionalCheckFailedException{})
		f.enqueue("GetItem", fakeGetItemFound(Lease{
			ShardID:    "s-1",
			Owner:      "other",
			Counter:    6,
			Checkpoint: CheckpointTrimHorizon,
		}), nil)

		err := newFakeStore(t, f).Delete(context.Background(), "s-1", 5)
		if !errors.Is(err, ErrLeaseConflict) {
			t.Fatalf("want ErrLeaseConflict, got %v", err)
		}
	})

	t.Run("missing row → nil (idempotent)", func(t *testing.T) {
		f := newFakeDynamo()
		f.enqueue("DeleteItem", nil, &types.ConditionalCheckFailedException{})
		f.enqueue("GetItem", fakeGetItemMissing(), nil)

		if err := newFakeStore(t, f).Delete(context.Background(), "s-1", 5); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

// TestHeartbeat covers the happy path through the shared update() code path.
// The conflict/not-found sub-cases are shared with Acquire and are covered by
// TestUpdateDisambiguatesConflictVsNotFound.
func TestHeartbeat(t *testing.T) {
	f := newFakeDynamo()
	in := Lease{ShardID: "s-1", Owner: "me", Counter: 7, Checkpoint: "seq-42"}
	want := Lease{ShardID: "s-1", Owner: "me", Counter: 8, Checkpoint: "seq-42"}
	f.enqueue("UpdateItem", fakeUpdateOutput(want), nil)

	got, err := newFakeStore(t, f).Heartbeat(context.Background(), in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// TestCheckpoint covers the happy path: the new sequence number and a bumped
// counter are reflected in the returned lease.
func TestCheckpoint(t *testing.T) {
	f := newFakeDynamo()
	in := Lease{ShardID: "s-1", Owner: "me", Counter: 7, Checkpoint: "seq-42"}
	want := Lease{ShardID: "s-1", Owner: "me", Counter: 8, Checkpoint: "seq-99"}
	f.enqueue("UpdateItem", fakeUpdateOutput(want), nil)

	got, err := newFakeStore(t, f).Checkpoint(context.Background(), in, "seq-99")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// TestRelease covers the Release happy path. Release reuses update() and
// clears leaseOwner atomically with the counter bump; a successful call
// returns nil.
func TestRelease(t *testing.T) {
	f := newFakeDynamo()
	// After Release the row has no owner. marshalLease omits leaseOwner when
	// Owner == "", so unmarshal returns Owner == "" — matching the zero value.
	want := Lease{ShardID: "s-1", Counter: 8, Checkpoint: "seq-42"}
	f.enqueue("UpdateItem", fakeUpdateOutput(want), nil)

	in := Lease{ShardID: "s-1", Owner: "me", Counter: 7, Checkpoint: "seq-42"}
	if err := newFakeStore(t, f).Release(context.Background(), in); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestEnsure covers Ensure's two outcomes: PutItem succeeds for a new row,
// and ConditionalCheckFailed is silently accepted (row already exists, which
// is the postcondition Ensure promises — idempotent).
func TestEnsure(t *testing.T) {
	t.Run("new row → nil", func(t *testing.T) {
		f := newFakeDynamo()
		f.enqueue("PutItem", &dynamodb.PutItemOutput{}, nil)

		if err := newFakeStore(t, f).Ensure(context.Background(), "s-1", nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("row already exists → nil (idempotent)", func(t *testing.T) {
		f := newFakeDynamo()
		f.enqueue("PutItem", nil, &types.ConditionalCheckFailedException{})

		if err := newFakeStore(t, f).Ensure(context.Background(), "s-1", nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

// TestList covers paginated Scan: a single-page response and a two-page
// response where the first Scan carries a non-empty LastEvaluatedKey that
// triggers a second Scan with ExclusiveStartKey set.
func TestList(t *testing.T) {
	lease1 := Lease{ShardID: "s-1", Owner: "w-1", Counter: 3, Checkpoint: "seq-1"}
	lease2 := Lease{ShardID: "s-2", Counter: 0, Checkpoint: CheckpointTrimHorizon}

	t.Run("single page", func(t *testing.T) {
		f := newFakeDynamo()
		f.enqueue("Scan", &dynamodb.ScanOutput{
			Items: []map[string]types.AttributeValue{marshalLease(lease1)},
		}, nil)

		got, err := newFakeStore(t, f).List(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(got, []Lease{lease1}) {
			t.Fatalf("got %+v, want [%+v]", got, lease1)
		}
	})

	t.Run("paginated two pages", func(t *testing.T) {
		f := newFakeDynamo()
		// First page carries LastEvaluatedKey, which triggers a second Scan.
		f.enqueue("Scan", &dynamodb.ScanOutput{
			Items: []map[string]types.AttributeValue{marshalLease(lease1)},
			LastEvaluatedKey: map[string]types.AttributeValue{
				attrLeaseKey: &types.AttributeValueMemberS{Value: "s-1"},
			},
		}, nil)
		f.enqueue("Scan", &dynamodb.ScanOutput{
			Items: []map[string]types.AttributeValue{marshalLease(lease2)},
		}, nil)

		got, err := newFakeStore(t, f).List(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(got, []Lease{lease1, lease2}) {
			t.Fatalf("got %+v, want [%+v %+v]", got, lease1, lease2)
		}
	})
}
