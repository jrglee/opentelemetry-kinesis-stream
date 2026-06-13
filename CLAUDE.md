# CLAUDE.md

Guidance for Claude Code (and other AI assistants) working in this repo.

## Project

A paired OpenTelemetry Collector exporter and receiver for Amazon Kinesis
Data Streams. Architecture lives in [`DESIGN.md`](DESIGN.md). Cross-cutting
decisions made during implementation land as ADRs in
[`docs/adr/`](docs/adr/). Per-component documentation lives inside each
component directory.

## Status

Early scaffolding. The factories register but the components are no-ops.
There is no encoding, compression, partitioning, shard ownership,
checkpointing, or `PutRecords` / `GetRecords` wiring yet. CI is intentionally
deferred until there is meaningful code to verify.

## Layout

```
DESIGN.md                            architecture; the source of truth
docs/adr/                            architectural decision records
exporter/awskinesisexporter/         Kinesis exporter component
receiver/awskinesisreceiver/         Kinesis receiver component
internal/encoding/                   wire encoding & compression names
```

Single Go module rooted at the repo root.

## Build & test

```
mise install        # pins Go, golangci-lint, gofumpt
make check          # fmt + vet + lint + test (the pre-push gate)
```

Other Makefile targets: `build`, `test`, `lint`, `fmt`, `vet`, `tidy`,
`clean`.

## Conventions

- Go pinned in `mise.toml`. Format with gofumpt. Lint with golangci-lint.
- Per-package godoc must stand on its own — do not reference `DESIGN.md` or
  other repo-relative paths from inside Go source or per-component READMEs.
  Code may be extracted into its own module; what ships with it must be
  self-contained.
- Short imperative commit messages. Every commit individually builds and
  tests.
- Semver. Wire compatibility is a public commitment once components leave
  development stability.

## Decision records

New architectural decisions land as ADRs in `docs/adr/`. Use
[`docs/adr/template.md`](docs/adr/template.md) as a starting point and the
next sequential number.
