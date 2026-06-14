# 0019. Oversize recovery is a chain of policies, not a single mode

- **Status:** Accepted
- **Date:** 2026-06-13

## Context

[ADR-0012](0012-tag-grouping-and-oversize-repack.md) defined the original
oversize handling: `oversize.policy` was one of `split_half`, `drop_largest`,
or `reject`, with `split_half` the default. In practice two of those three
did not earn their config surface:

- `drop_largest` was implemented as a thin alias for `split_half` — the
  recursive splitter already drops the irreducible leaf, so the dedicated
  "remove the largest top-level resource" pass would only have differed on
  multi-resource batches whose largest resource fits alone, which the split
  recursion also handles.
- `split_half` cannot recover the most common real-world failure: a single
  span or datapoint whose own attributes are too large (one long tag value or
  a high-cardinality attribute set). The recursion terminates on the
  irreducible leaf and the item is dropped.

DESIGN.md §4 calls oversize handling "best-effort under operator-supplied
knobs" and explicitly invites richer policies. The PoC's narrow surface left
operators with no knob that attacks attribute bloat — the root cause they
hit first.

A second deficiency was observability: all oversize-time drops were counted
on `kinesis.exporter.records_dropped` with `reason="oversize"`, collapsing
marshal failures, compression failures, recursion-bound exhaustion,
irreducible leaves, and policy-driven rejections into one bucket. Operators
could not tell *why* data was being lost, which is the question the metric
exists to answer.

## Decision

- **`oversize.policy` becomes `oversize.policies`** — an ordered list of
  strategies tried against the still-oversize payload until one fits or the
  chain is exhausted. The first policy whose output fits wins; chain
  exhaustion drops the remainder and counts it with a reason label.
- **Strategies:**
  - `split_half` (unchanged structurally; default).
  - `truncate_attribute_values` (new). Walks resource, scope, and
    span/datapoint attributes (plus span events, links, and exemplars for
    metrics); clamps string-valued attributes longer than
    `oversize.max_attribute_value_bytes` (default 4096), backstepping to a
    UTF-8 codepoint boundary so the output is never invalid UTF-8 (strict
    encoders such as `otel_arrow` reject mid-codepoint truncations). Every
    clamp increments `kinesis.exporter.attributes_truncated` regardless of
    whether truncation alone fit the payload or `split_half` shipped it
    afterward — the data was mutated either way, and the metric records
    that fact.
  - `reject` (unchanged).
- **Chain shape constraint.** `truncate_attribute_values` is the only
  composable policy: it returns a (possibly unmodified) batch the next
  policy operates on. `split_half` and `reject` are terminal — their drops
  cannot be re-presented to a subsequent policy, so listing anything after
  them would silently never run. Validation rejects such configs at start-up.
- **`drop_largest` is removed**, not aliased. Validation fails fast with
  `unknown oversize.policies entry "drop_largest"`. This is a pre-1.0
  breaking change called out in the user guide.
- **Drop reasons are split** into `marshal_error`, `compress_error`,
  `max_attempts`, `irreducible`, `reject_policy`, `chain_exhausted`. The
  existing `rejected` label (PutRecords path) is unchanged.
- **Debug logs trace the chain**: each `encode attempt` records pre- and
  post-compression bytes, depth, and fit; each policy step records its
  decision (`oversize policy: truncate_attribute_values` with the count of
  attributes clamped; `oversize policy: split_half` with left/right item
  counts). Chain exhaustion is logged at Warn level with the full policy
  list so the operator sees what was tried.

## Consequences

- Operators get a knob that directly attacks the single-long-tag-value
  failure mode. The recommended chain for attribute-heavy workloads is
  `[truncate_attribute_values, split_half]`: truncation runs first so the
  splitter does not waste cycles on a payload whose bloat lives in one
  attribute.
- The new `attributes_truncated` counter makes recovery observable. A
  sustained non-zero rate is a signal that something upstream is emitting
  attribute values longer than the threshold — operators chase the source
  rather than blindly raising the ceiling.
- The pre-1.0 break is loud. The legacy `oversize.policy` field is kept on
  the struct as a tombstone so Validate() fails at start-up with an
  explicit migration message naming the value the operator had set —
  silent fallback to the default would have left stale configs working "by
  accident" with different semantics, which is worse than failing fast.
- Existing alerts on `kinesis.exporter.records_dropped{reason="oversize"}`
  must be migrated to the new label set (`marshal_error`, `compress_error`,
  `max_attempts`, `irreducible`, `reject_policy`, `chain_exhausted`,
  plus the unchanged `rejected` for PutRecords-level failures). There is no
  compatibility alias; the reasons are too distinct to collapse without
  hiding the lever the operator should pull.
- The chain shape leaves room for future policies (e.g. dropping span events,
  switching codecs on retry) without further config breakage — they slot
  into the list as new named entries.
- Symmetric dead-letter (wrapping the oversize raw batch as a pipeline
  telemetry record, as the receiver does for undecodable records under
  [ADR-0013](0013-dead-letter-via-pipeline-reemit.md)) was considered as a
  fourth strategy and deferred. The exporter does not own a dead-letter
  sink today, and adding one is a larger commitment than the chain itself —
  revisit when an operator hits a workload where neither truncation nor
  splitting recovers and `chain_exhausted` is the only fallback.
