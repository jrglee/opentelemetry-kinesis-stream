package lease

import (
	"errors"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
