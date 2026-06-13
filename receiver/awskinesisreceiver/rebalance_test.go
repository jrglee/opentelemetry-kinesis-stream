package awskinesisreceiver

import (
	"reflect"
	"sort"
	"testing"

	"github.com/jrglee/opentelemetry-kinesis-stream/internal/lease"
)

func TestFairShareTarget(t *testing.T) {
	cases := []struct{ shards, workers, want int }{
		{0, 1, 0},
		{2, 1, 2},
		{2, 2, 1},
		{3, 2, 2},
		{4, 2, 2},
		{5, 2, 3},
		{1, 3, 1},
		{2, 0, 2}, // defensive: zero workers treated as one
	}
	for _, c := range cases {
		if got := fairShareTarget(c.shards, c.workers); got != c.want {
			t.Errorf("fairShareTarget(%d,%d)=%d want %d", c.shards, c.workers, got, c.want)
		}
	}
}

// lease builds a lease row; fresh/drained/owned are supplied per-test.
func ls(shard, owner, checkpoint string, parents ...string) lease.Lease {
	if checkpoint == "" {
		checkpoint = lease.CheckpointTrimHorizon
	}
	return lease.Lease{ShardID: shard, Owner: owner, Checkpoint: checkpoint, ParentIDs: parents}
}

func allTrue(shards ...string) map[string]bool {
	m := map[string]bool{}
	for _, s := range shards {
		m[s] = true
	}
	return m
}

func TestPlanRebalance(t *testing.T) {
	tests := []struct {
		name        string
		in          rebalanceInput
		wantAcquire []string
		wantRelease []string
		wantSteal   string
	}{
		{
			name: "lone worker acquires all shards",
			in: rebalanceInput{
				leases:  []lease.Lease{ls("s0", "", ""), ls("s1", "", "")},
				self:    "a",
				fresh:   map[string]bool{},
				drained: allTrue("s0", "s1"),
				owned:   map[string]bool{},
			},
			wantAcquire: []string{"s0", "s1"},
		},
		{
			name: "two workers, I own both, release one",
			in: rebalanceInput{
				leases:  []lease.Lease{ls("s0", "a", ""), ls("s1", "a", "")},
				self:    "a",
				fresh:   allTrue("s0", "s1"),
				drained: allTrue("s0", "s1"),
				owned:   allTrue("s0", "s1"),
			},
			// only 'a' is fresh -> 1 worker -> target 2 -> no release.
			// Make a second worker visible below; this case is lone-owner.
			wantRelease: nil,
		},
		{
			name: "two fresh workers, I own both, release surplus",
			in: rebalanceInput{
				leases:  []lease.Lease{ls("s0", "a", ""), ls("s1", "a", ""), ls("s2", "b", "")},
				self:    "a",
				fresh:   allTrue("s0", "s1", "s2"),
				drained: allTrue("s0", "s1", "s2"),
				owned:   allTrue("s0", "s1"),
			},
			// 3 shards, workers {a,b} = 2, target 2. I own 2 == target -> nothing.
			wantRelease: nil,
		},
		{
			name: "over target releases highest shard ids",
			in: rebalanceInput{
				leases: []lease.Lease{
					ls("s0", "a", ""), ls("s1", "a", ""), ls("s2", "a", ""), ls("s3", "b", ""),
				},
				self:    "a",
				fresh:   allTrue("s0", "s1", "s2", "s3"),
				drained: allTrue("s0", "s1", "s2", "s3"),
				owned:   allTrue("s0", "s1", "s2"),
			},
			// 4 shards, 2 workers, target 2. I own 3 -> release 1 (highest: s2).
			wantRelease: []string{"s2"},
		},
		{
			name: "under target with all owned-fresh steals from overloaded peer",
			in: rebalanceInput{
				leases:  []lease.Lease{ls("s0", "b", ""), ls("s1", "b", "")},
				self:    "a",
				fresh:   allTrue("s0", "s1"),
				drained: allTrue("s0", "s1"),
				owned:   map[string]bool{},
			},
			// 2 shards, workers {a,b}=2, target 1. b owns 2 > 1 -> steal s0.
			wantSteal: "s0",
		},
		{
			name: "stable uneven split does not steal",
			in: rebalanceInput{
				leases:  []lease.Lease{ls("s0", "a", ""), ls("s1", "b", ""), ls("s2", "b", "")},
				self:    "a",
				fresh:   allTrue("s0", "s1", "s2"),
				drained: allTrue("s0", "s1", "s2"),
				owned:   allTrue("s0"),
			},
			// 3 shards, 2 workers, target 2. I own 1 < 2, but b owns 2 == target
			// (not strictly over) -> no steal, nothing unowned -> no-op.
		},
		{
			name: "prefers acquiring unowned over stealing",
			in: rebalanceInput{
				leases:  []lease.Lease{ls("s0", "b", ""), ls("s1", "", "")},
				self:    "a",
				fresh:   allTrue("s0"), // s1 unowned
				drained: allTrue("s0", "s1"),
				owned:   map[string]bool{},
			},
			// 2 shards, workers {a,b}=2, target 1. Acquire s1 (unowned) up to
			// target; no steal needed.
			wantAcquire: []string{"s1"},
		},
		{
			name: "stale owner's shard is acquirable not stolen",
			in: rebalanceInput{
				leases:  []lease.Lease{ls("s0", "dead", "")},
				self:    "a",
				fresh:   map[string]bool{}, // s0 owner not fresh
				drained: allTrue("s0"),
				owned:   map[string]bool{},
			},
			// dead owner not counted -> workers {a}=1 -> target 1 -> acquire s0.
			wantAcquire: []string{"s0"},
		},
		{
			name: "child shard with undrained parent is skipped",
			in: rebalanceInput{
				leases:  []lease.Lease{ls("child", "", "", "parent")},
				self:    "a",
				fresh:   map[string]bool{},
				drained: map[string]bool{}, // child not drained
				owned:   map[string]bool{},
			},
			// nothing acquirable (parent not drained) -> no-op even though under target.
		},
		{
			name: "SHARD_END leases excluded from target and candidates",
			in: rebalanceInput{
				leases: []lease.Lease{
					ls("s0", "a", lease.CheckpointShardEnd),
					ls("s1", "", ""),
				},
				self:    "a",
				fresh:   map[string]bool{},
				drained: allTrue("s0", "s1"),
				owned:   map[string]bool{},
			},
			// active shards = {s1}; target ceil(1/1)=1 -> acquire s1.
			wantAcquire: []string{"s1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := planRebalance(tc.in)
			assertSet(t, "acquire", got.acquire, tc.wantAcquire)
			assertSet(t, "release", got.release, tc.wantRelease)
			if got.steal != tc.wantSteal {
				t.Errorf("steal=%q want %q", got.steal, tc.wantSteal)
			}
		})
	}
}

func assertSet(t *testing.T, label string, got, want []string) {
	t.Helper()
	g := append([]string(nil), got...)
	w := append([]string(nil), want...)
	sort.Strings(g)
	sort.Strings(w)
	if len(g) == 0 && len(w) == 0 {
		return
	}
	if !reflect.DeepEqual(g, w) {
		t.Errorf("%s = %v want %v", label, g, w)
	}
}
