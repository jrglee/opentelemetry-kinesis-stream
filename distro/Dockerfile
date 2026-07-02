# Builds the otelcol-kinesis distribution via the OpenTelemetry Collector
# Builder from distro/builder-config.yaml. Build context is the repo root so
# the builder's local-module replace (=> ../..) resolves to this checkout.
FROM golang:1.26 AS build
ARG OCB_VERSION=v0.154.0
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go run go.opentelemetry.io/collector/cmd/builder@${OCB_VERSION} \
      --config distro/builder-config.yaml

# Root variant (not :nonroot) deliberately: the E2E consumer writes to the
# /shared named volume, which distroless-nonroot cannot chown. Production
# hardening is out of scope for the PoC.
FROM gcr.io/distroless/static-debian12:latest
COPY --from=build /src/distro/_build/otelcol-kinesis /otelcol-kinesis
ENTRYPOINT ["/otelcol-kinesis"]
CMD ["--config", "file:/etc/otelcol/config.yaml"]
