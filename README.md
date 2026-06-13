# opentelemetry-kinesis-stream

Paired OpenTelemetry Collector exporter and receiver for Amazon Kinesis Data
Streams. The exporter lifts ceilings of the existing `opentelemetry-collector-contrib`
Kinesis exporter (5 MiB records, zstd, OpenTelemetry Arrow, deterministic
partition keys). The receiver closes the gap contrib leaves open: there is
no first-party Kinesis Data Streams receiver today.

**Status:** working proof of concept. Traces flow end-to-end — exporter to
Kinesis, receiver back out — with shard ownership coordinated and rebalanced
across replicas via a KCL-shaped DynamoDB lease table. Many capabilities are
deliberately deferred (see [ADR-0005](docs/adr/0005-poc-milestone-scope-cuts.md)).

## Documentation

- [`docs/user-guide.md`](docs/user-guide.md) — configuration reference,
  examples, behavior sequence diagrams, and a **step-by-step walkthrough for
  testing against real AWS**.
- [`DESIGN.md`](DESIGN.md) — architecture.
- [`docs/adr/`](docs/adr/) — decisions made during implementation.

## Build

```
mise install
make check          # format, vet, lint, unit tests
make e2e            # full docker-compose round-trip test (needs Docker)
```

`mise install` pins the toolchain (Go, golangci-lint, gofumpt, awscli).
`make check` is the pre-push gate. `make e2e` stands up a producer collector,
the MiniStack emulator, and two consumer replicas, then asserts every span is
delivered exactly once.

## License

Apache-2.0. See [`LICENSE`](LICENSE).
