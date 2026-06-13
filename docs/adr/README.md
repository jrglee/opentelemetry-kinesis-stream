# Architectural Decision Records

These records capture cross-cutting decisions made during implementation
that future contributors need to understand in order to read the code as
intentional rather than accidental. They are deliberately lightweight: a
short context, the decision stated as a positive imperative, and the
consequences worth revisiting.

The top-level [`DESIGN.md`](../../DESIGN.md) covers the overall architecture.
The ADRs here cover the smaller, sharper decisions that fall out of
implementing it.

## Index

- [0001 — Standalone repo, single Go module](0001-standalone-repo-single-module.md)
- [0002 — Component naming mirrors contrib](0002-component-naming-mirrors-contrib.md)
- [0003 — mise pins the toolchain; Makefile owns task running](0003-mise-and-makefile-for-build-tooling.md)
- [0004 — Testing strategy: middleware fakes, MiniStack, real AWS](0004-testing-strategy-middleware-ministack-real-aws.md)
- [0005 — PoC milestone scope cuts](0005-poc-milestone-scope-cuts.md)
- [0006 — Shard lease coordination via a KCL-shaped lease store](0006-shard-lease-coordination.md)
- [0007 — Hand-written collector binary and docker-compose E2E stack](0007-custom-collector-binary-and-e2e-stack.md)
- [0008 — Leaderless fair-share shard rebalancing](0008-leaderless-fair-share-rebalancing.md)
- [0009 — Graceful lease handoff and shutdown drain](0009-graceful-lease-handoff-and-shutdown.md)
- [0010 — zstd/snappy codecs; OTel-Arrow deferred](0010-codecs-and-deferred-arrow.md)
- [0011 — Metrics signal via a single signal-agnostic seam](0011-metrics-signal-via-sink-seam.md)
- [0012 — Tag-hash partition keys, group-by-tag microbatching, oversize repack](0012-tag-grouping-and-oversize-repack.md)
- [0013 — Dead-letter via pipeline re-emit; exporter observability](0013-dead-letter-via-pipeline-reemit.md)
- [0014 — Model shard coordination on KCL](0014-model-shard-coordination-on-kcl.md)
- [0015 — Delegate observability to the collector's built-in telemetry](0015-delegate-observability-to-collector-telemetry.md)

## How to add an ADR

1. Copy [`template.md`](template.md) to `NNNN-kebab-title.md` using the next
   sequential number.
2. Fill in Context, Decision, Consequences. Keep each section short — if a
   section grows past a few paragraphs, the decision is probably more than
   one ADR.
3. Add a one-line entry to the index above.
4. Open a PR.

ADRs are immutable once accepted. To change a decision, write a new ADR
that supersedes the old one and update the old one's `Status` to
`Superseded by NNNN`.
