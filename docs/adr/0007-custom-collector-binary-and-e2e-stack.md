# 0007. Hand-written collector binary and docker-compose E2E stack

- **Status:** Accepted
- **Date:** 2026-06-13

## Context

Demonstrating the round trip needs a runnable Collector that includes both
components, plus a way to exercise it against a Kinesis-compatible backend
without real AWS. Two questions: how to build the binary, and what to run it
against.

For the binary, the OpenTelemetry Collector Builder (OCB) is the idiomatic
distribution tool — a YAML manifest plus a code-generation step. The
alternative is a hand-written `main.go` that registers the factory set
directly using the `otelcol` assembly API.

For the backend, the testing strategy ADR already settled on MiniStack as the
LocalStack replacement; this milestone is its first real use.

## Decision

Ship a hand-written `cmd/otelcol-kinesis/main.go` that assembles the Collector
from an explicit factory set (OTLP receiver, our Kinesis receiver and
exporter, file and debug exporters, batch processor). Build it with a
multi-stage Dockerfile to a distroless static image.

Run the E2E against MiniStack (`ministackorg/ministack`, one container
covering Kinesis and DynamoDB) via docker-compose: a producer collector, two
consumer collector replicas sharing a DynamoDB lease table, an init container
that creates the stream and lease table, and `telemetrygen` as the load
source. A Go test behind the `e2e` build tag drives the stack up, emits a
fixed number of spans, and asserts every span arrives exactly once across the
two consumers — proving both the wire round trip and multi-replica lease
coordination in one run.

OCB is deferred, not rejected. The hand-written binary has fewer moving parts
for a PoC and no build-tool version to keep in lockstep with the module's
collector pins. Switching to OCB later is a contained change.

## Consequences

- One binary, two configs (producer and consumer) — the same image runs
  either role, which keeps the compose stack small.
- The factory set is explicit Go code, so adding or removing a component is a
  code review, not a manifest edit plus regeneration. The cost is that the
  binary does not advertise its build provenance the way an OCB manifest
  does.
- The E2E asserts the property that matters most for the sharding work — no
  duplicate delivery across replicas — rather than just a single-consumer
  round trip. It is opt-in (`make e2e`, build tag) and needs a Docker daemon,
  so it does not run as part of `make check`.
- The compose stack pins MiniStack to a verified tag and uses two shards so
  ownership genuinely splits across the two consumers. Reshard scenarios are
  a later addition to the same harness.
- The binary pulls the full Collector dependency tree into the single module.
  This is the cost the single-module decision
  ([0001](0001-standalone-repo-single-module.md)) already accepted for the
  PoC; it is the most likely pressure point if the module is later split.
