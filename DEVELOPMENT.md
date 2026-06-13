# Development

Local setup for working on the exporter, receiver, and the custom collector
binary. For *using* the components, see [`docs/user-guide.md`](docs/user-guide.md).

## Toolchain

The toolchain is pinned with [mise](https://mise.jdx.dev/):

```
mise install
```

This installs the exact versions from [`mise.toml`](mise.toml): Go,
golangci-lint, gofumpt, and the AWS CLI. Use these versions — CI gates on the
same ones.

It is a single Go module rooted at the repo root.

## Makefile targets

| Target          | What it does                                                        |
|-----------------|---------------------------------------------------------------------|
| `make build`    | `go build ./...`                                                    |
| `make test`     | `go test ./...` (unit tests)                                        |
| `make vet`      | `go vet ./...`                                                      |
| `make lint`     | `golangci-lint run`                                                 |
| `make fmt`      | `gofumpt -w .` + `go mod tidy` (rewrites in place)                  |
| `make tidy`     | `go mod tidy`                                                       |
| `make check`    | **the pre-push gate**: fmt + vet + lint + test                     |
| `make ci`       | read-only gate: fails on unformatted/untidy code instead of fixing  |
| `make cover`    | per-package statement coverage summary                             |
| `make collector`| build the custom collector binary to `bin/otelcol-kinesis`         |
| `make docker`   | build the collector container image (`otelcol-kinesis:dev`)        |
| `make compose-up` / `make compose-down` | bring the E2E stack up / down (with volumes) |
| `make e2e`      | full docker-compose round-trip test (needs Docker)                 |
| `make clean`    | remove build artifacts and the compose `shared/` dir               |

Run `make check` before pushing; every commit should build and test on its own.

## Building and running the collector

`make collector` builds a Collector binary that bundles this repo's exporter and
receiver (composed via OpenTelemetry Collector Builder; see
[`otelcol-builder.yaml`](otelcol-builder.yaml) and
[`cmd/otelcol-kinesis/`](cmd/otelcol-kinesis/)). Run it against a config file:

```
./bin/otelcol-kinesis --config path/to/config.yaml
```

The user guide's [real-AWS walkthrough](docs/user-guide.md#testing-against-real-aws)
has complete producer and consumer configs.

## The docker-compose E2E stack

[`compose/`](compose/) holds a self-contained round-trip stack: a producer
collector, the [MiniStack](https://github.com/ministackorg/ministack) Kinesis +
DynamoDB emulator, and two consumer replicas. The E2E driver in
[`e2e/`](e2e/) (build tag `e2e`) stands the stack up, generates load, and asserts
every span is delivered exactly once across the two replicas.

```
make e2e          # build the image, run the round trip, tear down
make compose-up   # bring the stack up manually for poking at it
make compose-down # tear it down (removes volumes)
```

There is also an InfluxDB line-protocol metrics variant
([`compose/docker-compose.influx.yaml`](compose/docker-compose.influx.yaml),
`e2e/influx_test.go`).

### Known local gotchas

- **colima `/tmp` is not mounted into the VM.** The harness avoids bind-mounting
  host `/tmp`; it copies fixtures into a named volume instead. Keep generated
  config under the repo (`compose/shared/`), not `/tmp`.
- **Docker credential helper.** On some setups the default `docker-credential-*`
  helper errors during `compose up --build`. If you hit an auth-helper failure,
  check `~/.docker/config.json` `credsStore`.
- **Distroless image runs as root** by necessity for the emulator-init step;
  don't "fix" the Dockerfile user without re-checking the init script.
- **`telemetrygen --rate=0`** means "as fast as possible," not "off" — use a
  positive rate when you want a bounded load.

## Conventions

- Format with gofumpt; lint with golangci-lint. `make fmt` applies both
  (plus `go mod tidy`).
- Per-package godoc and per-component READMEs must stand alone — do **not**
  reference `DESIGN.md` or other repo-relative paths from inside Go source or
  component docs. The components may be extracted into their own module later;
  what ships with them must be self-contained. (Root docs like this one and
  `README.md` may reference repo paths freely.)
- New cross-cutting decisions land as ADRs in [`docs/adr/`](docs/adr/) — copy
  [`docs/adr/template.md`](docs/adr/template.md) and use the next number.
- Short imperative commit messages.
