# 0024. Worker identity resolution strategies

- **Status:** Accepted
- **Date:** 2026-07-17

## Context

Each shard lease is owned by a single worker identity — the DynamoDB
`leaseOwner` column, keyed to the receiver replica. Ownership correctness is a
safety property: every `Heartbeat`, `Checkpoint`, and `Release` is fenced on
`leaseOwner` matching the holder ([0006](0006-shard-lease-coordination.md)). So
the identity must be **unique** per replica (two replicas sharing one fight over
leases) and ideally **stable** across restarts (a restarting replica that keeps
its identity reclaims its own leases immediately instead of waiting out
`lease_duration`).

The identity was a single config string, `worker_id`, defaulting to a random
`otelcol-<uuid>` when unset. External injection already works — the distro
wires the confmap `envprovider`, so `worker_id: ${env:POD_NAME}` expands at load
time. That covers platforms that expose a stable identity as an environment
variable (Kubernetes downward API).

It does **not** cover ECS. The stable per-replica identity there is the task
ID, and ECS exposes it only behind the container metadata endpoint
(`$ECS_CONTAINER_METADATA_URI_V4/task`) — never as an environment variable a
task definition can reference. An operator can only wire it by adding an
entrypoint that curls the endpoint and re-exports a variable. Meanwhile the
random-UUID default is a quiet footgun: a bouncing task mints a fresh identity
every restart and cannot reclaim its own leases, stalling for a full
`lease_duration` on every deploy.

DESIGN.md §2 holds that "every configurable knob is a long-lived compatibility
commitment" and that a new knob "require[s] a stated use case that cannot be
served by an existing one." `worker_id` plus env-expansion cannot serve the ECS
task ID; that is the use case a new knob must exist to serve.

## Decision

Add a `worker_resolution_strategy` enum that declaratively selects how the
identity is resolved at startup. The receiver does the resolution; the operator
sets one value.

- `static` (default) — use `worker_id`; if empty, a random `otelcol-<uuid>`.
  This is **exactly** the prior behavior, so existing configuration — with or
  without `worker_id` — is unchanged, and the zero-config memory-backend path
  keeps working.
- `hostname` — the OS hostname.
- `ecs` — the task ID from the ECS container metadata endpoint (v4, with a v3
  fallback). The endpoint is link-local and needs no IAM permission; the task
  ID is the task ARN's final path segment.
- `file` — the trimmed contents of `worker_id_file`, for platforms that project
  an identity onto a file rather than a variable.

Two rules keep the model unambiguous, enforced in `Validate`: `worker_id` is
read **only** by `static` (setting it under another strategy is an error), and
non-static strategies **fail fast** at `Start` if they cannot resolve rather
than silently falling back to a throwaway identity — the determinism is the
point of choosing a strategy explicitly.

Every strategy stays within DESIGN.md §2's "no external control plane"
constraint: the ECS endpoint is AWS-native and link-local, `file` is
filesystem-local, `hostname` is local.

## Consequences

- ECS deployments become correct by default with one config line
  (`worker_resolution_strategy: ecs`), no entrypoint shims, no task-role
  changes. A restarting task reclaims its own leases because the task ID is
  stable for the task's life.
- The enum is the single point of extension for future identity sources (e.g.
  an EC2 instance ID via IMDS) — a new strategy, not a new top-level knob.
- The `ecs` strategy couples one code path to the ECS metadata contract (the
  `TaskARN` field and the ARN's `/`-delimited task ID). If AWS changes that
  shape the strategy breaks; it is isolated to one resolver and unit-tested
  against a stubbed endpoint. It cannot be exercised in the docker-compose E2E
  (no metadata endpoint there), so the E2E proves strategy resolution via the
  `file` strategy instead, and `ecs`/`hostname` are unit-only.
- `static`'s empty-`worker_id` fallback still mints an unstable random UUID.
  That is retained for backward compatibility, but it is now one explicit
  choice among four rather than the only behavior — operators who need
  stability have a named path to it.
- Identity **uniqueness across live replicas is a correctness invariant**, and
  `hostname`/`file` widen the door to accidental collision. The fast-restart
  reclaim path (a replica reclaims a lease whose owner equals its own identity,
  `internal/lease/taker.go`) means two live replicas that resolve the *same*
  identity will each keep reclaiming the other's shards and deliver records
  twice — the counter fence does not prevent it. This is enforced/mitigated at
  three layers: the resolver rejects an empty or non-UTF-8 identity at Start;
  `Store.Acquire` rejects an empty owner at the seam (an empty owner reads as
  "unowned" and would let every replica claim the shard); and the strategy
  godoc/user-guide state the uniqueness requirement per strategy. A *non-empty
  but duplicated* live identity cannot be detected locally and remains an
  operator responsibility. Making collisions structurally impossible (e.g.
  gating reclaim-self on staleness, trading fast reclaim for collision-safety)
  is a change to the ADR-0009 handoff semantics and is deferred to its own ADR.
