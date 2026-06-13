# 0002. Component naming mirrors contrib

- **Status:** Accepted
- **Date:** 2026-06-12

## Context

The components could be named `kinesisexporter` / `kinesisreceiver` (shorter,
matches what most users would type) or `awskinesisexporter` /
`awskinesisreceiver` (matches the existing `opentelemetry-collector-contrib`
convention).

The `aws` prefix carries information: there are several Kinesis services
(Data Streams, Firehose, Video, Video WebRTC, Video Media), and operators
familiar with contrib already expect the disambiguation.

`DESIGN.md` calls out wire compatibility with the contrib exporter as a v1
commitment, and notes that the shared encoding package is the most plausible
upstream contribution path. Matching names now means no rename later if any
code ends up upstream.

## Decision

Package names are `awskinesisexporter` and `awskinesisreceiver`. The
component type registered with the Collector is `awskinesis`.

## Consequences

- Operator-familiar; no surprise for anyone coming from contrib.
- Upstream contribution does not require a rename.
- The package paths are longer than they would otherwise be. Acceptable.
