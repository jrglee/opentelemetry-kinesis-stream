# 0003. mise pins the toolchain; Makefile owns task running

- **Status:** Accepted
- **Date:** 2026-06-12

## Context

Two things need to be decided for build tooling: where toolchain versions
are pinned (Go, golangci-lint, gofumpt) and where task targets live (build,
test, lint, format).

mise can do both: it has a `[tools]` section for version pinning and a
`[tasks]` section for runnable commands. A Makefile is the conventional
choice for the second job in a Go repo and works without mise installed.

Pinning toolchain versions is non-negotiable: the project follows a global
standard of no `^`/`~` ranges, and Go module behavior shifts subtly between
minor releases.

## Decision

`mise.toml` pins Go, golangci-lint, and gofumpt with exact versions. A
`Makefile` at the repo root hosts the task targets: `build`, `test`, `lint`,
`fmt`, `tidy`, `vet`, `check`, `clean`.

## Consequences

- One tool — `mise` — provides the entire toolchain. `mise install` is a
  one-line setup.
- `make` works without mise as long as the binaries are on `PATH`; mise is a
  convenience, not a hard requirement for running individual targets.
- Two configuration files (`mise.toml` and `Makefile`) instead of one. The
  separation matches each tool's strength: mise pins versions, Make runs
  tasks.
- A future contributor familiar with Go projects finds the conventional
  shape (a Makefile) at the front door.
