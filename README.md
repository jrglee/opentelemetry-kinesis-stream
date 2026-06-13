# opentelemetry-kinesis-stream

Paired OpenTelemetry Collector exporter and receiver for Amazon Kinesis Data
Streams. The exporter lifts ceilings of the existing `opentelemetry-collector-contrib`
Kinesis exporter (5 MiB records, zstd, OpenTelemetry Arrow, deterministic
partition keys). The receiver closes the gap contrib leaves open: there is
no first-party Kinesis Data Streams receiver today.

**Status:** early scaffolding. See [`DESIGN.md`](DESIGN.md) for the
architecture and [`docs/adr/`](docs/adr/) for decisions made during
implementation.

## Build

```
mise install
make check
```

`mise install` pins the toolchain (Go, golangci-lint, gofumpt). `make check`
runs format, vet, lint, and tests.

## License

Apache-2.0. See [`LICENSE`](LICENSE).
