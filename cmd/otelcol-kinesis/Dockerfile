# Build context is the repo root.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /otelcol-kinesis ./cmd/otelcol-kinesis

# Root variant (not :nonroot) so the process can write to the named volume
# the E2E mounts at /shared. A production image would run nonroot with the
# volume pre-chowned; that hardening is out of scope for the PoC.
FROM gcr.io/distroless/static-debian12:latest
COPY --from=build /otelcol-kinesis /otelcol-kinesis
ENTRYPOINT ["/otelcol-kinesis"]
# Explicit file: scheme — the resolver's default URI scheme is env.
CMD ["--config", "file:/etc/otelcol/config.yaml"]
