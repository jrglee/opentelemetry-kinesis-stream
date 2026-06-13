package lease

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// KCL lease table attribute names. These are fixed by KCL v2; we use the
// exact strings so a real KCL consumer could share the table without
// re-ingesting from TRIM_HORIZON. We only write the subset of columns we
// need — KCL tolerates the rest being absent and fills its own defaults.
const (
	attrLeaseKey      = "leaseKey"
	attrLeaseOwner    = "leaseOwner"
	attrLeaseCounter  = "leaseCounter"
	attrCheckpoint    = "checkpoint"
	attrParentShardID = "parentShardId"

	// parentSeparator matches KCL's join character for multi-parent rows.
	// A single parent is stored as the bare shard ID.
	parentSeparator = ","
)

// DynamoDBStore is a Store backed by a KCL-compatible DynamoDB lease table.
//
// The table must have leaseKey as its HASH partition key (string). All
// mutating operations are conditional writes keyed on leaseCounter (and
// leaseOwner where ownership must be proved), which is what gives the
// fencing semantics promised by Store.
type DynamoDBStore struct {
	client *dynamodb.Client
	table  string
}

// NewDynamoDBStore wraps a dynamodb.Client and a table name. The caller owns
// table provisioning — this constructor does not create the table.
func NewDynamoDBStore(client *dynamodb.Client, table string) *DynamoDBStore {
	return &DynamoDBStore{client: client, table: table}
}

// List paginates a Scan over the lease table. The Store contract permits
// eventually consistent reads; conditional writes are what make stale data
// safe to act on, not read consistency.
func (s *DynamoDBStore) List(ctx context.Context) ([]Lease, error) {
	var (
		out      []Lease
		startKey map[string]types.AttributeValue
	)
	for {
		resp, err := s.client.Scan(ctx, &dynamodb.ScanInput{
			TableName:         aws.String(s.table),
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range resp.Items {
			l, err := unmarshalLease(item)
			if err != nil {
				return nil, err
			}
			out = append(out, l)
		}
		if len(resp.LastEvaluatedKey) == 0 {
			return out, nil
		}
		startKey = resp.LastEvaluatedKey
	}
}

// Ensure inserts a fresh lease row if one does not already exist. The
// attribute_not_exists(leaseKey) condition makes the PutItem idempotent:
// a second caller racing the first sees ConditionalCheckFailed, which we
// translate to nil — the row exists, which is the postcondition.
func (s *DynamoDBStore) Ensure(ctx context.Context, shardID string, parentIDs []string) error {
	cond, err := expression.NewBuilder().
		WithCondition(expression.Name(attrLeaseKey).AttributeNotExists()).
		Build()
	if err != nil {
		return err
	}
	item := marshalLease(Lease{
		ShardID:    shardID,
		Counter:    0,
		Checkpoint: CheckpointTrimHorizon,
		ParentIDs:  parentIDs,
	})
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                 aws.String(s.table),
		Item:                      item,
		ConditionExpression:       cond.Condition(),
		ExpressionAttributeNames:  cond.Names(),
		ExpressionAttributeValues: cond.Values(),
	})
	if isConditionalCheckFailed(err) {
		return nil
	}
	return err
}

// Acquire claims an unowned (or stale) lease. The condition matches on the
// caller-observed Counter only — Acquire is the one operation where the
// new owner is allowed to overwrite whatever owner string is currently
// stored, because Counter alone is the fencing token at takeover time.
func (s *DynamoDBStore) Acquire(ctx context.Context, shardID, owner string, expectedCounter int64) (Lease, error) {
	upd := expression.
		Set(expression.Name(attrLeaseOwner), expression.Value(owner)).
		Set(expression.Name(attrLeaseCounter), expression.Value(expectedCounter+1))
	cond := expression.Name(attrLeaseCounter).Equal(expression.Value(expectedCounter))
	return s.update(ctx, shardID, upd, cond)
}

// Heartbeat bumps Counter while proving the caller still owns the row.
// Both leaseOwner and leaseCounter must match the caller's observation;
// either mismatch yields ErrLeaseConflict.
func (s *DynamoDBStore) Heartbeat(ctx context.Context, lease Lease) (Lease, error) {
	upd := expression.Set(expression.Name(attrLeaseCounter), expression.Value(lease.Counter+1))
	cond := ownerCounterCondition(lease)
	return s.update(ctx, lease.ShardID, upd, cond)
}

// Checkpoint persists a sequence number against an owned lease, bumping
// Counter atomically with the checkpoint write so callers can resume from
// exactly the recorded position after a takeover.
func (s *DynamoDBStore) Checkpoint(ctx context.Context, lease Lease, seq string) (Lease, error) {
	upd := expression.
		Set(expression.Name(attrCheckpoint), expression.Value(seq)).
		Set(expression.Name(attrLeaseCounter), expression.Value(lease.Counter+1))
	cond := ownerCounterCondition(lease)
	return s.update(ctx, lease.ShardID, upd, cond)
}

// Release voluntarily clears ownership. We REMOVE leaseOwner rather than
// writing an empty string so the row matches KCL's "unowned" shape, and
// SET the bumped counter in the same UpdateItem so the transition is
// atomic and visible to the next Acquire.
func (s *DynamoDBStore) Release(ctx context.Context, lease Lease) error {
	upd := expression.
		Set(expression.Name(attrLeaseCounter), expression.Value(lease.Counter+1)).
		Remove(expression.Name(attrLeaseOwner))
	cond := ownerCounterCondition(lease)
	_, err := s.update(ctx, lease.ShardID, upd, cond)
	return err
}

// ownerCounterCondition is the fencing predicate shared by Heartbeat,
// Checkpoint, and Release: the caller must still be the owner *and* hold
// the latest Counter.
func ownerCounterCondition(l Lease) expression.ConditionBuilder {
	return expression.Name(attrLeaseOwner).Equal(expression.Value(l.Owner)).
		And(expression.Name(attrLeaseCounter).Equal(expression.Value(l.Counter)))
}

// update is the shared UpdateItem path. It builds the expression, issues
// the call with ReturnValues=ALL_NEW so we can hand the freshly persisted
// state back to the caller, and normalizes the two error cases the Store
// contract names: a failed condition is ErrLeaseConflict; a missing row
// is ErrLeaseNotFound.
//
// DynamoDB does not distinguish "row missing" from "condition failed" in
// a vanilla conditional UpdateItem — both surface as
// ConditionalCheckFailedException. To return the right error we issue a
// follow-up GetItem on conflict to disambiguate. This costs one extra read
// only on the (rare) conflict path.
func (s *DynamoDBStore) update(
	ctx context.Context,
	shardID string,
	upd expression.UpdateBuilder,
	cond expression.ConditionBuilder,
) (Lease, error) {
	expr, err := expression.NewBuilder().WithUpdate(upd).WithCondition(cond).Build()
	if err != nil {
		return Lease{}, err
	}
	resp, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(s.table),
		Key:                       leaseKey(shardID),
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ReturnValues:              types.ReturnValueAllNew,
	})
	if isConditionalCheckFailed(err) {
		exists, gerr := s.rowExists(ctx, shardID)
		if gerr != nil {
			return Lease{}, gerr
		}
		if !exists {
			return Lease{}, ErrLeaseNotFound
		}
		return Lease{}, ErrLeaseConflict
	}
	if err != nil {
		return Lease{}, err
	}
	return unmarshalLease(resp.Attributes)
}

func (s *DynamoDBStore) rowExists(ctx context.Context, shardID string) (bool, error) {
	resp, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.table),
		Key:            leaseKey(shardID),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return false, err
	}
	return len(resp.Item) > 0, nil
}

func leaseKey(shardID string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		attrLeaseKey: &types.AttributeValueMemberS{Value: shardID},
	}
}

// marshalLease serializes a Lease into the KCL attribute layout. Owner is
// omitted entirely when empty so the row matches KCL's "unowned" shape.
// Parents are joined with ',' (KCL's separator); a missing/empty list
// omits the attribute, again matching KCL.
func marshalLease(l Lease) map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{
		attrLeaseKey:     &types.AttributeValueMemberS{Value: l.ShardID},
		attrLeaseCounter: &types.AttributeValueMemberN{Value: strconv.FormatInt(l.Counter, 10)},
		attrCheckpoint:   &types.AttributeValueMemberS{Value: l.Checkpoint},
	}
	if l.Owner != "" {
		item[attrLeaseOwner] = &types.AttributeValueMemberS{Value: l.Owner}
	}
	if len(l.ParentIDs) > 0 {
		item[attrParentShardID] = &types.AttributeValueMemberS{
			Value: strings.Join(l.ParentIDs, parentSeparator),
		}
	}
	return item
}

// unmarshalLease is the inverse. leaseKey is mandatory; everything else
// has a defined zero (missing owner → "", missing parents → nil, missing
// counter → 0, missing checkpoint → "") because KCL may write rows where
// some of those columns are absent.
func unmarshalLease(m map[string]types.AttributeValue) (Lease, error) {
	shardID, ok := stringAttr(m, attrLeaseKey)
	if !ok {
		return Lease{}, errors.New("lease row missing leaseKey")
	}
	l := Lease{ShardID: shardID}
	if owner, ok := stringAttr(m, attrLeaseOwner); ok {
		l.Owner = owner
	}
	if cp, ok := stringAttr(m, attrCheckpoint); ok {
		l.Checkpoint = cp
	}
	if n, ok := m[attrLeaseCounter].(*types.AttributeValueMemberN); ok {
		v, err := strconv.ParseInt(n.Value, 10, 64)
		if err != nil {
			return Lease{}, err
		}
		l.Counter = v
	}
	if parents, ok := stringAttr(m, attrParentShardID); ok && parents != "" {
		l.ParentIDs = strings.Split(parents, parentSeparator)
	}
	return l, nil
}

func stringAttr(m map[string]types.AttributeValue, name string) (string, bool) {
	v, ok := m[name].(*types.AttributeValueMemberS)
	if !ok {
		return "", false
	}
	return v.Value, true
}

func isConditionalCheckFailed(err error) bool {
	if err == nil {
		return false
	}
	var ccf *types.ConditionalCheckFailedException
	return errors.As(err, &ccf)
}
