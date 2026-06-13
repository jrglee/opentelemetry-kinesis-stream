package awskinesisreceiver

import (
	"testing"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/lease"
)

func TestParentsDrained(t *testing.T) {
	tests := []struct {
		name    string
		target  lease.Lease
		all     []lease.Lease
		drained bool
	}{
		{
			name:    "original shard with no parents always drained",
			target:  lease.Lease{ShardID: "s-0"},
			drained: true,
		},
		{
			name:   "child blocked while parent still active",
			target: lease.Lease{ShardID: "s-1", ParentIDs: []string{"s-0"}},
			all: []lease.Lease{
				{ShardID: "s-0", Checkpoint: "49625-abc"},
			},
			drained: false,
		},
		{
			name:   "child unblocked once parent hits SHARD_END",
			target: lease.Lease{ShardID: "s-1", ParentIDs: []string{"s-0"}},
			all: []lease.Lease{
				{ShardID: "s-0", Checkpoint: lease.CheckpointShardEnd},
			},
			drained: true,
		},
		{
			name:   "child blocked when one of two parents not drained (merge)",
			target: lease.Lease{ShardID: "m-2", ParentIDs: []string{"s-0", "s-1"}},
			all: []lease.Lease{
				{ShardID: "s-0", Checkpoint: lease.CheckpointShardEnd},
				{ShardID: "s-1", Checkpoint: "49625-xyz"},
			},
			drained: false,
		},
		{
			name:    "parent trimmed past retention treated as drained",
			target:  lease.Lease{ShardID: "s-1", ParentIDs: []string{"gone"}},
			all:     nil,
			drained: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := parentsDrained(tc.target, indexCheckpoints(tc.all)); got != tc.drained {
				t.Fatalf("parentsDrained=%v want %v", got, tc.drained)
			}
		})
	}
}
