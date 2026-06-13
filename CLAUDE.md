# CLAUDE.md

Guidance for Claude Code (and other AI assistants) working in this repo.

## Project

A paired OpenTelemetry Collector exporter and receiver for Amazon Kinesis
Data Streams. Architecture lives in [`DESIGN.md`](DESIGN.md). Cross-cutting
decisions made during implementation land as ADRs in
[`docs/adr/`](docs/adr/). Per-component documentation lives inside each
component directory.

## Status

Working end-to-end round trip (traces only): the exporter encodes
(OTLP-proto), compresses (none/gzip), and writes Kinesis records; the
receiver coordinates shard ownership across replicas via a lease store
(in-memory or KCL-shaped DynamoDB), polls `GetRecords`, and checkpoints
after downstream acceptance. A docker-compose E2E proves the round trip and
multi-replica no-duplicate delivery against the MiniStack emulator.

Deliberately deferred (see [ADR-0005](docs/adr/0005-poc-milestone-scope-cuts.md)):
OTLP-JSON/Arrow encodings, zstd, tag-hash partitioning, microbatch repacking,
metrics/logs signals, resharding, and rebalancing/autoscaling. CI is still
deferred.

## Layout

```
DESIGN.md                            architecture; the source of truth
docs/adr/                            architectural decision records
cmd/otelcol-kinesis/                 custom Collector binary + Dockerfile
compose/                             docker-compose E2E stack + configs
e2e/                                 E2E test driver (build tag: e2e)
exporter/awskinesisexporter/         Kinesis exporter component
receiver/awskinesisreceiver/         Kinesis receiver component (coordinator + pollers)
internal/encoding/                   wire encoding & compression
internal/lease/                      shard lease store (memory + DynamoDB)
```

Single Go module rooted at the repo root.

## Build & test

```
mise install        # pins Go, golangci-lint, gofumpt, awscli
make check          # fmt + vet + lint + test (the pre-push gate)
make e2e            # full docker-compose round-trip test (needs Docker)
```

Other Makefile targets: `build`, `test`, `lint`, `fmt`, `vet`, `tidy`,
`collector`, `docker`, `compose-up`, `compose-down`, `clean`.

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
