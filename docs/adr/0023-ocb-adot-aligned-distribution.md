# 0023. Build the collector with OCB as an ADOT-aligned distribution

- **Status:** Accepted (supersedes [0007](0007-custom-collector-binary-and-e2e-stack.md))
- **Date:** 2026-07-02

## Context

ADR-0007 chose a hand-written `cmd/otelcol-kinesis/main.go` over the
OpenTelemetry Collector Builder to keep the PoC simple, accepting that the
binary pulled the full Collector dependency tree into the single module. Two
things changed: the components must be proven to work inside an ADOT-style
distribution (the collector AWS customers deploy), and the hand-rolled
`main.go` had become the only importer of ten-plus collector/contrib modules
in the root `go.mod`.

Facts established during research (2026-07-02):

- The ADOT Collector is actively released — latest v0.48.0 (May 2026), pinning
  upstream collector v0.151.0 — and is **not** in maintenance mode (the X-Ray
  SDKs/Daemon are; AWS steers those users *toward* ADOT/OTel).
  Sources: github.com/aws-observability/aws-otel-collector/releases;
  aws.amazon.com/blogs/mt/announcing-aws-x-ray-sdks-daemon-end-of-support-and-opentelemetry-migration/.
- ADOT publishes **no OCB manifest**: it hand-registers components in
  `pkg/defaultcomponents/defaults.go`. "ADOT-aligned" therefore means
  mirroring its component list in our own OCB manifest, not importing its
  build.
- This repo pins collector v0.154.0 — newer than ADOT's pin. Every mirrored
  component (`awsemfexporter`, `sigv4authextension`,
  `resourcedetectionprocessor`, `memorylimiterprocessor`) exists in contrib
  v0.154.0.
- OCB v0.154.0's defaults (env/file/http/https/yaml providers,
  `otelconftelemetry`) exactly match what the hand-written `main.go` wired, so
  existing configs work unchanged.

Alternatives: downgrading to ADOT's collector pin (loses three months of
upstream fixes for no compatibility gain — the components are the alignment
surface, not the core version) or keeping the hand-written binary alongside an
OCB build (two component registries to keep in sync).

## Decision

Generate the collector from `distro/builder-config.yaml` with
`go run go.opentelemetry.io/collector/cmd/builder@v0.154.0` and delete
`cmd/otelcol-kinesis/`. The manifest holds this repo's two components plus the
pipeline dependencies the compose stacks use and a representative ADOT subset
(`awsemf`, `sigv4auth`, `resourcedetection`, `memory_limiter`) proving
coexistence with AWS-SDK-heavy contrib components.

Generated code is not committed: `distro/_build/` is gitignored, the manifest
is the source of truth, and `make collector` / `distro/Dockerfile` regenerate
it. The generated module is standalone (own `go.mod`, local `replace` back to
the repo root), invisible to the root module's `./...` gates.

Version lockstep invariant: the collector versions in `go.mod`,
`distro/builder-config.yaml`, and `OCB_VERSION` in the Makefile move together.

## Consequences

- The E2E stacks now run the same binary shape AWS customers deploy, and the
  E2E asserts distro provenance via the `components` subcommand.
- Deleting `main.go` drops otelcol, service, the confmap providers, and five
  pipeline components from the root `go.mod` — the exact cost ADR-0007
  accepted is now paid by the generated module instead.
- Builds require the builder step (network on first run); Docker builds
  re-download modules unless the go.mod COPY layer is cached, so E2E image
  build timeouts are sized for the uncached path.
- Adding a component now means editing the manifest, not Go code — but
  keeping three version pins in lockstep is a new failure mode the Makefile
  comment and this ADR must keep visible.
- Revisit if ADOT publishes an official OCB manifest (mirror it directly) or
  if the ADOT collector's maintenance status changes.
