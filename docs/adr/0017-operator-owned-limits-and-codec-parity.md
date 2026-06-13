# 0017. Operator-owned limits; full collector codec parity

- **Status:** Accepted
- **Date:** 2026-06-13

## Context

The exporter previously hardcoded Kinesis limits — `maxBytesPerPut = 5 MiB`,
`maxRecordsPerPut = 500` — and doc/comments asserted fixed "hard limits" (e.g.
"1 MiB per record"). Those assertions rot: AWS changes limits (Oct 2025 raised
per-record size from 1 MiB to up to 10 MiB, opt-in via the stream's
`maxRecordSize`, and PutRecords aggregate from 5 MiB to 10 MiB), and limits also
vary by account, region, and enterprise/special configurations. Baking AWS's
current numbers into the code makes the library wrong whenever the environment
differs.

Separately, the exporter supported only `none`/`gzip`/`zstd`/`snappy`
compression while the OpenTelemetry Collector's own codec set is small and
bounded — a consumer built from standard collector components may be configured
to expect a codec this exporter could not emit.

## Decision

1. **Treat record/request size limits as operator-owned knobs** the exporter
   enforces verbatim, asserting nothing about the live AWS ceiling. Knobs:
   `max_record_size` (per record) and a nested `put_records: { max_records,
   max_bytes }` block (per PutRecords call). Defaults are the conservative values
   every stream has historically accepted (1 MiB record, 500 records, 5 MiB
   request); raise them to match a stream configured for larger records/requests.
   The exporter operates agnostic to the AWS environment it runs in.
2. **Support the Collector's full compression set** for consumer compatibility:
   add `x-snappy-framed`, `zlib`, and `deflate` alongside the existing
   `none`/`gzip`/`zstd`/`snappy`. No new dependencies — stdlib `compress/zlib` +
   `compress/flate` and the framed API in the already-vendored
   `klauspost/compress`. A consumer built on standard collector components can
   always match what this exporter writes.

## Consequences

- The library stays correct as AWS limits change and across regions/enterprise
  configs; operators tune limits to their environment; docs no longer assert AWS
  "hard limits" as truth (they cite current AWS defaults only as informational).
- Defaulting conservatively means opted-in larger-record streams must raise the
  knobs to use the extra headroom.
- A hostile-review concern that the `shards.owned` UpDown counter could drift
  under generation churn was investigated and refuted: `startPoller` installs a
  shard's poller only when the slot is absent and the poller-exit defer is the
  sole deleter, so each +1 is paired with exactly one -1.
