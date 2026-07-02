# 0022. Adopt exporterhelper queue, retry, and timeout

- **Status:** Accepted
- **Date:** 2026-07-02

## Context

The exporter's create functions returned the consumer directly, so none of
the Collector's standard exporter machinery ran: no sending queue, no
retry-on-failure, no per-request timeout. The internal `PutRecords` loop
classifies errors as retryable or permanent (`consumererror.NewPermanent`),
but that classification had no consumer — a whole-request throttle or a
partial failure that outlived the in-place retry budget propagated to the
caller and the data was silently lost. Comments claiming the residual is
"handed to the Collector's retry policy" described machinery that was not
wired.

Alternatives considered: growing the internal retry loop into a full
queue/backoff implementation (reinvents exporterhelper, non-standard config
surface), or keeping the direct consumer and documenting the loss modes
(unacceptable for an at-least-once pipeline).

## Decision

Wrap the traces, metrics, and logs create functions in
`exporterhelper.NewTraces/NewMetrics/NewLogs`, exposing the collector-standard
configuration blocks with their standard defaults:

- `sending_queue` (`exporterhelper.QueueBatchConfig`, enabled by default),
- `retry_on_failure` (`configretry.BackOffConfig`, enabled by default),
- `timeout` (`exporterhelper.TimeoutConfig`, 5s default).

Split retry responsibility between the two layers:

- The **internal `PutRecords` loop remains the only handler of per-record
  partial failures**. PutRecords reports per-record results; retrying only
  the failed subset in place is the only way to avoid re-sending records
  that already succeeded. Its attempt budget and backoff stay deliberately
  short so the helper's policy dominates.
- **Whole-request errors and the residual subset that outlives the internal
  budget** are returned to exporterhelper's retry sender: retryable unless
  wrapped `consumererror.NewPermanent` (stream missing, invalid argument).

## Consequences

- Sustained throttling now backs off and retries under an operator-tunable,
  collector-standard policy instead of dropping data; queue-full and retry
  exhaustion surface through the standard `otelcol_exporter_*` metrics.
- Acceptance becomes asynchronous by default (queue in front of the
  pipeline): a `Consume*` call succeeding no longer means the data reached
  Kinesis. Operators who need synchronous backpressure can disable the queue
  or enable `wait_for_result`.
- A helper-level retry of a request that partially succeeded re-sends the
  whole request, so duplicates are possible — consistent with the pipeline's
  at-least-once contract, but now an explicit consequence of two retry
  layers.
- The config surface (`sending_queue`, `retry_on_failure`, `timeout`) is a
  compatibility commitment tracking upstream exporterhelper; revisit if
  upstream renames the blocks (e.g. a future `queue_batch` migration) or if
  per-record NACK support ever lands in the helper, which would let the
  internal loop shrink further.

## Amendment (2026-07-02): timeout sizing

The per-attempt `timeout` wraps the *entire* flush — every PutRecords chunk
plus the internal transient-retry budget (`maxPutAttempts` with backoff,
≈3s worst case). An attempt deadline shorter than a healthy flush makes every
retry die at the same point: head chunks are re-written (duplicated) each
attempt and, once `retry_on_failure` gives up, the tail is dropped. The
shipped default is therefore 30s rather than the helper's 5s, the flush loop
checks the context between chunks, and the internal backoff returns
immediately when the deadline is nearer than the wait. Operators lowering
`timeout` should keep it comfortably above their observed flush time.
