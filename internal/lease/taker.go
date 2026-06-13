package lease

import "sort"

// PlanInput is the snapshot a single rebalance pass plans against. All fields
// are read-only; Plan is pure so it can be table-tested without AWS.
type PlanInput struct {
	Leases  []Lease         // current lease-table snapshot
	Self    string          // this worker's id
	Fresh   map[string]bool // shardID -> owner heartbeated within lease_duration
	Drained map[string]bool // shardID -> parents are drained (acquirable now)
	Owned   map[string]bool // shardID -> this worker is actively polling it
}

// PlanResult is the set of actions a worker should take this pass to move
// toward its fair share. At most one steal per pass bounds churn.
type PlanResult struct {
	Acquire []string // unowned or stale shards to claim
	Release []string // surplus shards this worker should give up
	Steal   string   // one shard to take from an overloaded fresh owner ("" = none)
}

// Plan is the KCL LeaseTaker step: it computes the fair-share actions for one
// worker. Every replica runs the same computation against the same lease
// snapshot, so they converge without a leader: target = ceil(activeShards /
// activeWorkers), where an active worker is a distinct fresh owner (plus self).
// A worker over target releases the surplus; a worker under target acquires
// unowned/stale shards and, failing that, steals one shard from the
// most-overloaded fresh owner.
//
// Parent-drains-before-child is respected: a shard is only an acquire or steal
// candidate once its parents are drained.
func Plan(in PlanInput) PlanResult {
	active := activeShards(in.Leases)
	target := fairShareTarget(len(active), activeWorkers(active, in.Fresh, in.Self))

	myShards := ownedActive(active, in.Owned)
	sort.Strings(myShards)

	// Over target: release the surplus (deterministic: highest shard IDs first
	// so two workers don't both release the same one if they raced).
	if len(myShards) > target {
		return PlanResult{Release: myShards[target:]}
	}

	want := target - len(myShards)
	if want <= 0 {
		return PlanResult{}
	}

	// Acquire unowned or stale (non-fresh-owner) shards whose parents are drained.
	var acquire []string
	for _, l := range active {
		if len(acquire) >= want {
			break
		}
		if in.Owned[l.ShardID] || !in.Drained[l.ShardID] {
			continue
		}
		if l.Owner == "" || l.Owner == in.Self || !in.Fresh[l.ShardID] {
			acquire = append(acquire, l.ShardID)
		}
	}
	sort.Strings(acquire)
	if len(acquire) >= want {
		return PlanResult{Acquire: acquire}
	}

	// Still short and nothing free to take: steal one shard from the most
	// overloaded fresh owner (an owner holding strictly more than target).
	steal := pickStealTarget(active, in, target)
	return PlanResult{Acquire: acquire, Steal: steal}
}

// activeShards returns the leases that are not drained (SHARD_END).
func activeShards(leases []Lease) []Lease {
	out := make([]Lease, 0, len(leases))
	for _, l := range leases {
		if l.Checkpoint != CheckpointShardEnd {
			out = append(out, l)
		}
	}
	return out
}

// activeWorkers counts the distinct owners currently heartbeating, plus self.
func activeWorkers(active []Lease, fresh map[string]bool, self string) int {
	owners := map[string]struct{}{self: {}}
	for _, l := range active {
		if l.Owner != "" && fresh[l.ShardID] {
			owners[l.Owner] = struct{}{}
		}
	}
	return len(owners)
}

// fairShareTarget is ceil(shards / workers), with sane behaviour at the edges.
func fairShareTarget(shards, workers int) int {
	if shards == 0 {
		return 0
	}
	if workers < 1 {
		workers = 1
	}
	return (shards + workers - 1) / workers
}

func ownedActive(active []Lease, owned map[string]bool) []string {
	var out []string
	for _, l := range active {
		if owned[l.ShardID] {
			out = append(out, l.ShardID)
		}
	}
	return out
}

// pickStealTarget returns one shard to steal: a drained shard owned by the
// most-overloaded fresh peer (one holding strictly more than target). Returns
// "" if no peer is overloaded. Deterministic on ties (lowest owner id, then
// lowest shard id) so concurrent workers don't pile onto the same victim
// unpredictably.
func pickStealTarget(active []Lease, in PlanInput, target int) string {
	counts := map[string]int{}
	for _, l := range active {
		if l.Owner != "" && l.Owner != in.Self && in.Fresh[l.ShardID] {
			counts[l.Owner]++
		}
	}

	bestOwner := ""
	bestCount := target // must be strictly greater than target to qualify
	owners := make([]string, 0, len(counts))
	for o := range counts {
		owners = append(owners, o)
	}
	sort.Strings(owners)
	for _, o := range owners {
		if counts[o] > bestCount {
			bestCount = counts[o]
			bestOwner = o
		}
	}
	if bestOwner == "" {
		return ""
	}

	var candidates []string
	for _, l := range active {
		if l.Owner == bestOwner && in.Fresh[l.ShardID] && in.Drained[l.ShardID] {
			candidates = append(candidates, l.ShardID)
		}
	}
	if len(candidates) == 0 {
		return ""
	}
	sort.Strings(candidates)
	return candidates[0]
}
