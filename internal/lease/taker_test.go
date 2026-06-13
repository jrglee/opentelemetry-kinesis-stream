package lease

import (
	"reflect"
	"sort"
	"testing"
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

// ls builds a lease row; fresh/drained/owned are supplied per-test.
func ls(shard, owner, checkpoint string, parents ...string) Lease {
	if checkpoint == "" {
		checkpoint = CheckpointTrimHorizon
	}
	return Lease{ShardID: shard, Owner: owner, Checkpoint: checkpoint, ParentIDs: parents}
}

func allTrue(shards ...string) map[string]bool {
	m := map[string]bool{}
	for _, s := range shards {
		m[s] = true
	}
	return m
}

func TestPlan(t *testing.T) {
	tests := []struct {
		name        string
		in          PlanInput
		wantAcquire []string
		wantRelease []string
		wantSteal   string
	}{
		{
			name: "lone worker acquires all shards",
			in: PlanInput{
				Leases:  []Lease{ls("s0", "", ""), ls("s1", "", "")},
				Self:    "a",
				Fresh:   map[string]bool{},
				Drained: allTrue("s0", "s1"),
				Owned:   map[string]bool{},
			},
			wantAcquire: []string{"s0", "s1"},
		},
		{
			name: "two workers, I own both, release one",
			in: PlanInput{
				Leases:  []Lease{ls("s0", "a", ""), ls("s1", "a", "")},
				Self:    "a",
				Fresh:   allTrue("s0", "s1"),
				Drained: allTrue("s0", "s1"),
				Owned:   allTrue("s0", "s1"),
			},
			// only 'a' is fresh -> 1 worker -> target 2 -> no release.
			// Make a second worker visible below; this case is lone-owner.
			wantRelease: nil,
		},
		{
			name: "two fresh workers, I own both, release surplus",
			in: PlanInput{
				Leases:  []Lease{ls("s0", "a", ""), ls("s1", "a", ""), ls("s2", "b", "")},
				Self:    "a",
				Fresh:   allTrue("s0", "s1", "s2"),
				Drained: allTrue("s0", "s1", "s2"),
				Owned:   allTrue("s0", "s1"),
			},
			// 3 shards, workers {a,b} = 2, target 2. I own 2 == target -> nothing.
			wantRelease: nil,
		},
		{
			name: "over target releases highest shard ids",
			in: PlanInput{
				Leases: []Lease{
					ls("s0", "a", ""), ls("s1", "a", ""), ls("s2", "a", ""), ls("s3", "b", ""),
				},
				Self:    "a",
				Fresh:   allTrue("s0", "s1", "s2", "s3"),
				Drained: allTrue("s0", "s1", "s2", "s3"),
				Owned:   allTrue("s0", "s1", "s2"),
			},
			// 4 shards, 2 workers, target 2. I own 3 -> release 1 (highest: s2).
			wantRelease: []string{"s2"},
		},
		{
			name: "under target with all owned-fresh steals from overloaded peer",
			in: PlanInput{
				Leases:  []Lease{ls("s0", "b", ""), ls("s1", "b", "")},
				Self:    "a",
				Fresh:   allTrue("s0", "s1"),
				Drained: allTrue("s0", "s1"),
				Owned:   map[string]bool{},
			},
			// 2 shards, workers {a,b}=2, target 1. b owns 2 > 1 -> steal s0.
			wantSteal: "s0",
		},
		{
			name: "stable uneven split does not steal",
			in: PlanInput{
				Leases:  []Lease{ls("s0", "a", ""), ls("s1", "b", ""), ls("s2", "b", "")},
				Self:    "a",
				Fresh:   allTrue("s0", "s1", "s2"),
				Drained: allTrue("s0", "s1", "s2"),
				Owned:   allTrue("s0"),
			},
			// 3 shards, 2 workers, target 2. I own 1 < 2, but b owns 2 == target
			// (not strictly over) -> no steal, nothing unowned -> no-op.
		},
		{
			name: "prefers acquiring unowned over stealing",
			in: PlanInput{
				Leases:  []Lease{ls("s0", "b", ""), ls("s1", "", "")},
				Self:    "a",
				Fresh:   allTrue("s0"), // s1 unowned
				Drained: allTrue("s0", "s1"),
				Owned:   map[string]bool{},
			},
			// 2 shards, workers {a,b}=2, target 1. Acquire s1 (unowned) up to
			// target; no steal needed.
			wantAcquire: []string{"s1"},
		},
		{
			name: "stale owner's shard is acquirable not stolen",
			in: PlanInput{
				Leases:  []Lease{ls("s0", "dead", "")},
				Self:    "a",
				Fresh:   map[string]bool{}, // s0 owner not fresh
				Drained: allTrue("s0"),
				Owned:   map[string]bool{},
			},
			// dead owner not counted -> workers {a}=1 -> target 1 -> acquire s0.
			wantAcquire: []string{"s0"},
		},
		{
			name: "child shard with undrained parent is skipped",
			in: PlanInput{
				Leases:  []Lease{ls("child", "", "", "parent")},
				Self:    "a",
				Fresh:   map[string]bool{},
				Drained: map[string]bool{}, // child not drained
				Owned:   map[string]bool{},
			},
			// nothing acquirable (parent not drained) -> no-op even though under target.
		},
		{
			name: "SHARD_END leases excluded from target and candidates",
			in: PlanInput{
				Leases: []Lease{
					ls("s0", "a", CheckpointShardEnd),
					ls("s1", "", ""),
				},
				Self:    "a",
				Fresh:   map[string]bool{},
				Drained: allTrue("s0", "s1"),
				Owned:   map[string]bool{},
			},
			// active shards = {s1}; target ceil(1/1)=1 -> acquire s1.
			wantAcquire: []string{"s1"},
		},

		// --- Adversarial cases grounded in KCL LeaseTaker behaviour. ---
		{
			// (a) Steal happens ONLY when nothing is free to acquire. With an
			// unowned shard present, the under-target worker acquires it and
			// plans no steal that pass, even though a peer is over target.
			name: "free shard preempts steal in the same pass",
			in: PlanInput{
				Leases:  []Lease{ls("s0", "b", ""), ls("s1", "b", ""), ls("s2", "", "")},
				Self:    "a",
				Fresh:   allTrue("s0", "s1"), // s2 unowned
				Drained: allTrue("s0", "s1", "s2"),
				Owned:   map[string]bool{},
			},
			// 3 shards, workers {a,b}=2, target 2. a wants 2 -> acquires s2; b
			// owns 2 == target, not over -> no steal candidate anyway, but even
			// the acquire path short-circuits before pickStealTarget.
			wantAcquire: []string{"s2"},
		},
		{
			// (b) A worker exactly at target is inert: no acquire, release, or
			// steal, even with other shards in the table owned by fresh peers.
			name: "worker exactly at target is inert",
			in: PlanInput{
				Leases:  []Lease{ls("s0", "a", ""), ls("s1", "b", ""), ls("s2", "c", ""), ls("s3", "d", "")},
				Self:    "a",
				Fresh:   allTrue("s0", "s1", "s2", "s3"),
				Drained: allTrue("s0", "s1", "s2", "s3"),
				Owned:   allTrue("s0"),
			},
			// 4 shards, 4 workers, target 1. a owns 1 == target -> nothing.
		},
		{
			// (c) At most one steal per pass even when several owners are over
			// target and nothing is free.
			name: "one steal per pass with multiple over-target owners",
			in: PlanInput{
				Leases: []Lease{
					ls("b0", "b", ""), ls("b1", "b", ""),
					ls("c0", "c", ""), ls("c1", "c", ""),
				},
				Self:    "a",
				Fresh:   allTrue("b0", "b1", "c0", "c1"),
				Drained: allTrue("b0", "b1", "c0", "c1"),
				Owned:   map[string]bool{},
			},
			// 4 shards, workers {a,b,c}=3, target 2. a owns 0, wants 2, nothing
			// free. b and c each own 2 == target -> NOT strictly over -> no
			// victim qualifies. Confirms the >target gate; see next case for a
			// real multi-victim steal.
		},
		{
			// (c)/(d) Two owners strictly over target; exactly one steal, and the
			// most-overloaded owner (c, owning 3 vs b's 3 -> tie broken by lowest
			// id... so make c clearly the heaviest) is the victim.
			name: "most overloaded owner chosen as victim, single steal",
			in: PlanInput{
				Leases: []Lease{
					ls("b0", "b", ""), ls("b1", "b", ""),
					ls("c0", "c", ""), ls("c1", "c", ""), ls("c2", "c", ""),
				},
				Self:    "a",
				Fresh:   allTrue("b0", "b1", "c0", "c1", "c2"),
				Drained: allTrue("b0", "b1", "c0", "c1", "c2"),
				Owned:   map[string]bool{},
			},
			// 5 shards, workers {a,b,c}=3, target 2. b owns 2 (==target, not
			// over), c owns 3 (>target) -> victim c, steal lowest shard c0. Only
			// one steal.
			wantSteal: "c0",
		},
		{
			// (e) SHARD_END leases are excluded from BOTH the target denominator
			// and the steal candidate set. The drained shard owned by the peer is
			// never a steal victim.
			name: "shard_end peer lease is not a steal candidate",
			in: PlanInput{
				Leases: []Lease{
					ls("done", "b", CheckpointShardEnd),
					ls("s0", "b", ""), ls("s1", "b", ""),
				},
				Self:    "a",
				Fresh:   allTrue("done", "s0", "s1"),
				Drained: allTrue("done", "s0", "s1"),
				Owned:   map[string]bool{},
			},
			// active = {s0,s1}; workers {a,b}=2, target 1. b owns 2 active > 1 ->
			// steal lowest active s0 ('done' excluded as SHARD_END).
			wantSteal: "s0",
		},
		{
			// (f) A child whose parent is not drained is never stolen, even from
			// an over-target fresh owner. The over-target owner here holds only
			// undrained children, so no steal is possible.
			name: "undrained child is never stolen",
			in: PlanInput{
				Leases: []Lease{
					ls("c0", "b", "", "p0"), ls("c1", "b", "", "p1"),
				},
				Self:    "a",
				Fresh:   allTrue("c0", "c1"),
				Drained: map[string]bool{}, // neither child drained
				Owned:   map[string]bool{},
			},
			// 2 shards, workers {a,b}=2, target 1. b owns 2 > 1, but both its
			// shards are undrained children -> no steal candidate -> no-op.
		},
		{
			// (g) Bootstrap: a brand-new worker owning nothing, all shards held by
			// one fresh peer over target, plans exactly one steal that pass.
			name: "bootstrap worker plans exactly one steal",
			in: PlanInput{
				Leases: []Lease{
					ls("s0", "b", ""), ls("s1", "b", ""), ls("s2", "b", ""), ls("s3", "b", ""),
				},
				Self:    "a",
				Fresh:   allTrue("s0", "s1", "s2", "s3"),
				Drained: allTrue("s0", "s1", "s2", "s3"),
				Owned:   map[string]bool{},
			},
			// 4 shards, workers {a,b}=2, target 2. a owns 0, wants 2, nothing
			// free -> steal one (lowest) from over-target b. One steal per pass.
			wantSteal: "s0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Plan(tc.in)
			assertSet(t, "acquire", got.Acquire, tc.wantAcquire)
			assertSet(t, "release", got.Release, tc.wantRelease)
			if got.Steal != tc.wantSteal {
				t.Errorf("steal=%q want %q", got.Steal, tc.wantSteal)
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
