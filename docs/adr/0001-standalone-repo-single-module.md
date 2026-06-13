# 0001. Standalone repo, single Go module

- **Status:** Accepted
- **Date:** 2026-06-12

## Context

The exporter and receiver could live in three plausible places: forked into
`opentelemetry-collector-contrib`, in this standalone repo as one Go module
per component (the contrib pattern), or in this standalone repo as a single
module covering both components and the shared internal encoding package.

Forking contrib is rejected in `DESIGN.md` §3 for reasons that don't need
restating here. The remaining question is one module versus two.

Per-component modules are the contrib convention. They let each component
version independently and be consumed without its sibling. The cost is
`replace` directives during local development, duplicated dependency lists,
and an internal-package boundary that no longer crosses freely between the
two components.

This repo is a proof of concept. The wire contract and configuration surface
are not stable. Independent versioning has no value until something is
stable enough to version.

## Decision

Use a single Go module at the repo root: `github.com/jrglee/opentelemetry-kinesis-stream`.
The exporter, receiver, and shared internal encoding package live as
sub-packages.

## Consequences

- The shared `internal/encoding` package works as Go's module-level
  `internal/` restriction intends: both components can import it, nothing
  outside the repo can.
- Local development needs no `replace` directives.
- The two components must move in lockstep at the module version level.
  This is fine for the PoC and would force the split if and when independent
  versioning matters.
- Revisit when the components stabilize, or when one needs to be consumed
  without the other.
