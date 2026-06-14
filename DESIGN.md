# opentelemetry-kinesis-stream — Design

## 1. Problem

The OpenTelemetry Collector ecosystem has only partial coverage for Amazon Kinesis Data Streams. An exporter exists in `opentelemetry-collector-contrib` (`exporter/awskinesisexporter`, Beta). A receiver does not. Operators wiring OTel through Kinesis today encounter four distinct gaps:

1. The contrib exporter caps each record and does not expose the ceiling as a knob. Kinesis Data Streams itself supports far larger records — up to 10 MiB, opt-in via the stream's `maxRecordSize` since Oct 2025 — and the actual ceiling varies by account, region, and stream configuration, so a fixed cap leaves headroom on the table that operators cannot reclaim.
2. There is no Kinesis Streams receiver. The closest substitute is the Firehose receiver, which is push-based, adds buffering latency on the order of a minute, loses cross-record ordering, and is itself capped at 1 MiB per record on the HTTP destination.
3. Compression is restricted to deflate, gzip, and zlib at a single level. zstd is absent.
4. Only OTLP proto/JSON, Jaeger proto, and Zipkin encodings are available. There is no path for OTel-Arrow, despite clear throughput advantages for large telemetry batches and a maturing reference implementation in `open-telemetry/otel-arrow`.

The four gaps are independently real, but the architecturally important observation is that **they couple at the wire**: every choice on the exporter side has to be matched by a decoder on the receiver side, and vice versa. Closing one gap without the other produces an asymmetric pair that does not interoperate end-to-end.

## 2. Constraints that shape the design

The constraints below are load-bearing for later sections; each one rules out shapes that would otherwise be plausible.

- **No external control plane.** State stores must be either AWS-native (DynamoDB, S3) or filesystem-local. Anything that requires a separately operated coordination service (etcd, ZooKeeper, a SaaS) is out of scope.
- **Deferred complexity is acceptable** in exchange for a smaller v1 surface. EFO, KPL aggregation, Arrow schema migration, multi-replica work-stealing, and cross-region replication are explicitly deferrable. The architecture must leave room for them without committing to them.
- **API surface area is a liability.** Every configurable knob is a long-lived compatibility commitment. Defaults should make most configurations empty; new knobs require a stated use case that cannot be served by an existing one.
- **Wire compatibility outranks code-level ergonomics.** A later rewrite of either component must be possible without breaking deployed producers and consumers. Internal interfaces can churn freely; the wire cannot.
- **Correctness of stateful behavior is non-negotiable.** Shard ownership, checkpointing, and reshard handling either work or silently corrupt downstream telemetry. There is no middle ground that ships.

## 3. Shape of the solution

Two paired components — an exporter and a receiver — that share a wire contract. The exporter writes Kinesis records; the receiver reads them; both interpret the bytes identically. Beyond that wire agreement, the two ends are independent.

The components live in a **standalone repository**, not a fork of contrib. The reasoning:

- Forking contrib inherits the build and CI surface of hundreds of unrelated components. The cost is paid every commit.
- The OpenTelemetry Collector Builder composes collectors from arbitrary Go modules, so a standalone repository can be consumed by any collector deployment without a fork in the dependency chain.
- Contributing components to contrib couples their lifecycle to contrib's release cadence, review process, and code-owner expectations. Standing the repository up independently keeps the question of upstream contribution separable from the question of where the code lives day-to-day, and allows the wire contract and configuration surface to stabilize before either is proposed as a public commitment.

The choice of license is **Apache 2.0**, matching the broader OpenTelemetry ecosystem. Any code derived from contrib stays under its original license and is attributed accordingly. A more permissive license (MIT) was considered and rejected because it complicates eventual upstreaming and creates per-file bookkeeping for derived works without any downstream benefit users would actually feel.

## 4. The wire contract

The wire contract is the most consequential decision in this design, because it is the only thing the two components must agree on, and it is the only thing that becomes a compatibility commitment over time.

A Kinesis record carries a payload that is the compression of the marshaling of OTel telemetry. **The wire is headerless and matches the contrib exporter's existing layout**, so the contrib exporter and this receiver can interoperate at the OTLP encodings out of the box. A framing header (magic byte + version + encoding tag) was considered for encoding-mismatch detection and in-place migration; it is deferred because the absence of one is what makes contrib compatibility free, and because encoding/compression are already declared on both ends by configuration.

The unit a record represents is **configurable, defaulting to one OTel resource per record, with microbatching gated on the standard pair of triggers: a maximum record count and a maximum time window, whichever fires first**, both with documented upper bounds. Size-based triggers were considered and rejected because the compressed size of a payload cannot be estimated cheaply before compression runs, and a size estimate accurate enough to use as a trigger would duplicate work the compressor is about to do anyway. The cost of this choice is that operators cannot tune microbatching directly against the record-size ceiling (itself an operator knob, defaulting to a conservative 1 MiB); the exporter and receiver must therefore emit enough observability (record-size histograms, compression-ratio histograms, per-batch fill rates, verbose-level diagnostic logs) to let operators tune the count and time bounds empirically.

Because the trigger is count/time rather than size, a microbatch can exceed the per-record size limit after compression. **The exporter handles oversize records by an ordered chain of recovery policies**, configured per deployment, and the design treats this as best-effort: the adapter does what it can with operator-supplied knobs, and surfaces the rest as observable failures rather than guessing. The shipped policies are `truncate_attribute_values` (clamp string attribute values whose UTF-8 byte length exceeds a configurable ceiling — the practical answer when one tag value is the bloat source), `split_half` (recursively halve the batch, the only lossless policy when item count is the bloat source), and `reject` (drop and let the §6 failure policy surface the loss). The chain is tried in order until something fits; `split_half` and `reject` terminate the chain by construction (their drops cannot be re-presented to a subsequent policy) and validation rejects configs that list them anywhere but last. The default is `[split_half]`. The repack loop has a configurable maximum attempt count to bound worst-case work. See [ADR-0019](docs/adr/0019-oversize-recovery-chain.md) for the rationale that replaced the earlier `drop_largest` policy.

Microbatching trades per-record granularity for better amortization of fixed costs (compression headers, `PutRecords` entry overhead, Kinesis per-record billing) when telemetry is sparse, without changing the wire shape on the receiving side — a record decodes to one or more resources via the same encoding. The Arrow case is the same shape: one Arrow record batch per Kinesis record, with multiple resources carried inside the batch.

Partition keys are wire-adjacent: they do not appear in the payload but they determine per-shard ordering, and the receiver's per-shard ordering guarantees depend on the producer's strategy. The default strategy is a **hash over user-configured record tags** rather than the random spread that contrib defaults to. The operator declares the **ordered list of tags** to hash (typically resource attributes such as `service.name`, optionally followed by a finer-grained discriminator), and the **hash algorithm** to use. Random remains available as an explicit option. The reasoning for diverging from contrib's default: a deterministic, tag-based key gives the receiver useful per-shard locality without producers having to think about ordering, and it pairs naturally with microbatching, where resources sharing a tag prefix are most usefully co-located in the same record. The same configuration applies to Arrow encoding: the tag values are taken from the resource attributes inside the batch, and the operator's tag-list definition decides what "the record's key" means when a single batch carries multiple resources.

## 5. The exporter

Conceptually a small delta on contrib's existing exporter: same AWS SDK v2 client, same `PutRecords` batching, same use of the standard exporter helper for queue/retry/timeout. The architectural changes are:

- Make the record-size ceiling an operator knob rather than a hardcoded cap, keeping a conservative 1 MiB default for behavioral parity with existing deployments. The library enforces the operator-provided limit and stays agnostic to the AWS environment, since the real ceiling varies by account, region, and stream configuration (AWS's current published values: up to 10 MiB per record, opt-in, and a 10 MiB `PutRecords` aggregate).
- Treat compression as a typed concern with a codec choice and a level, rather than a single string. zstd is the first new codec; the abstraction should make further codecs cheap to add without a config break.
- Treat encoding as an open registry. The registry shape matters more than which encoding shipped first. As built, it carries `otlp_proto`, `otlp_json`, and `otel_arrow`. `otel_arrow` uses a fresh producer per record so each Kinesis record is a self-contained Arrow batch — see [ADR-0018](docs/adr/0018-implement-otel-arrow-encoding.md) for the rationale and the trade-off (per-record schema overhead in exchange for store-and-forward compatibility).

Architectural questions worth flagging:

- **Compressor pooling.** Per-record codec construction is fine for stateless codecs but expensive for zstd. The exporter must either pool or accept the allocation cost. The choice has correctness implications under concurrency.
- **Partition-key default divergence from contrib.** Contrib's random-UUID default sacrifices locality; this design takes the opposite default (tag-hash) for the reasons given in §4. Operators migrating from contrib will see a different shard distribution unless they explicitly select `random`, and that migration note belongs in the README, not the architecture.

## 6. The receiver

The receiver is the meaningful new system. The cost of building it correctly is dominated by three concerns that have nothing to do with OTel:

1. **Shard ownership.** Kinesis exposes a list of shards; a consumer must read each one exactly once across a deployment. Multi-replica deployments need a coordination mechanism so two replicas do not read the same shard concurrently.
2. **Checkpointing.** A consumer that restarts must resume where the last successful read left off, or risk duplication or loss. The checkpoint write must happen *after* downstream acceptance, not at decode time.
3. **Resharding.** Kinesis shards split and merge over time. Consumers must drain parent shards before reading child shards to preserve per-key ordering. This is a known sharp edge of Kinesis consumer implementations.

The Amazon-blessed solution to all three is the Kinesis Client Library, which has no idiomatic Go port. This is the structural reason no Kinesis receiver exists in contrib today, and it is the architectural risk this project takes on.

Alternatives considered:

- **AWS Lambda as the runtime.** Lambda's event-source mapping handles shard ownership, checkpointing, and reshard handling natively, and is genuinely elegant for stateless decoders. It is rejected here because the OTel Collector is a long-lived, stateful, pipeline-oriented process: queues, exporters, batchers, throttle state, and back-pressure all assume an always-on runtime. Reshaping the Collector to fit a per-invocation handler model breaks its execution model and forfeits exactly the in-process pipeline benefits that make the Collector worth using. Lambda is the right answer for a thin Kinesis-to-OTLP forwarder; it is the wrong answer for a Collector-resident receiver.
- **Firehose as a bridge.** A delivery stream sourced from Kinesis pushing to the existing Firehose receiver works today with no new code. It is not a substitute when sub-minute latency, per-shard ordering, IAM-only auth, or larger records (the record-size ceiling is an operator knob) are required, but it is the right baseline to measure against before committing to a custom receiver.
- **EFO (`SubscribeToShard`) instead of polling.** EFO removes the lease-coordination problem in deployments under 20 consumers per stream, at higher cost. It is a viable shape; the bias is to start with polling because polling makes throttle behavior easier to reason about and because EFO can be added behind a flag later without changing the rest of the design.

Resolved design choices:

- **Checkpoint backing store.** The canonical store is DynamoDB, and **the table schema is KCL-compatible**. Inheriting KCL's schema buys a vetted design for lease items, ownership transfer, and checkpoint columns; it also opens a migration path to and from official KCL consumers, which is genuinely useful operationally. A simpler in-process or filesystem-local store is available for development and single-node deployments. The KCL-compatibility commitment is to the table layout, not to KCL's full state machine.
- **Failure semantics.** The default policy for decode errors, downstream pipeline rejection, and checkpoint write failures is **reject the affected record(s), log with sufficient context to investigate, and continue**. The policy is configurable per failure class. Dead-letter behavior is not a bespoke sink: failed raw records are wrapped (raw bytes, originating shard, sequence number, failure class, encoding/compression at receive time) and emitted into the Collector's own pipeline as telemetry, where the operator routes them with the standard routing components to S3, another Kinesis stream, log analytics, or wherever fits. This keeps the receiver free of sink implementations and gives operators the full expressive power of the Collector for failure handling.
- **Multi-replica coordination.** Heartbeated leases over conditional DynamoDB writes (the KCL-aligned mechanism) are the chosen design. Work-stealing and capacity-aware rebalancing are deliberately out of scope; mismatched shard and replica counts will produce hot spots, and observability is the answer.

Architectural risk still worth flagging at the design stage:

- **Reshard correctness.** The parent-drains-before-child invariant is straightforward to state and easy to violate. It must be testable against synthetic shard splits without depending on real AWS.

## 7. Shared encoding surface

Encoding, compression, and partition-key logic are needed by both components. They should live in one place. This is also the part of the system most likely to be useful to others — a small, focused module covering "how OTel telemetry is laid out in a Kinesis record" has reuse value beyond this repository, and is the most plausible upstream contribution path.

The architectural question is the interface, not the implementations. An encoder/decoder pair plus a compressor pair, keyed by signal (traces/metrics/logs) and codec name, is the minimum shape. **The module starts as an internal package**, giving the design room to churn under real use, **with the explicit intent to graduate it to a public, beta-stability API once the interface has settled.** The graduation point is the right moment to propose it for upstream contribution: a stabilized small module is much easier to land than a sprawling one.

## 8. What is intentionally not designed here

These are deliberate omissions, not oversights:

- The concrete module layout, file naming, and Go package boundaries.
- The configuration schema (field names, defaults, units, validation rules).
- The set and naming of metrics emitted by the components.
- The release cadence, CI policy, integration-test strategy, and dependency-bump rhythm.
- The roadmap of which features land in which release.
- The upstream contribution timeline for any specific change.

These belong to implementation. Encoding them in the architecture document creates premature commitments that will need to be renegotiated as soon as concrete code reveals the actual constraints.

## 9. Alternatives ruled out, with reasoning

- **Fork contrib.** Inherits the entire repository's CI and review surface; ownership inside contrib is harder than ownership outside it; OCB removes the need.
- **Lambda runtime.** Incompatible with the Collector's long-lived, stateful pipeline model. Good fit for a thin forwarder; wrong fit for an in-Collector receiver.
- **Firehose as a permanent answer.** Works for batch-tolerant workloads, but cannot meet the latency, ordering, IAM, or larger-record requirements that motivate gap 1 in the first place.
- **MIT license.** No downstream benefit users would feel; complicates derivation from contrib and eventual upstreaming.
- **A new wire protocol.** Every encoding under consideration is already standard. Inventing one creates an interop problem without solving a real one.
- **KCL-equivalent semantics (work-stealing, full reshard simulation, deterministic placement).** The full set of guarantees KCL provides is disproportionate complexity for the workload shapes in scope. The simplified coordination model in §6 is the deliberate compromise.

## 10. Compatibility commitments

A small number of cross-cutting decisions are stated here because they shape every later choice and should be treated as fixed unless explicitly revisited:

- **Wire compatibility with contrib's exporter** at the OTLP encodings is preserved at v1. The wire is headerless; encoding and compression are agreed by configuration on both ends.
- **Checkpoint table schema compatibility with KCL** is preserved. KCL-blessed migrations to and from this receiver are an intended use case.
- **Versioning follows semver.** Minor releases are backward-compatible at the wire, configuration, and checkpoint-schema layers. Major releases may break any of these and must document the migration. This applies in particular to OTel-Arrow schema evolution, which inherits the project's semver semantics rather than introducing its own.

## 11. Glossary

- **OCB** — OpenTelemetry Collector Builder; composes a Collector binary from Go modules per a YAML configuration.
- **EFO** — Enhanced Fan-Out; Kinesis push-based consumer protocol via `SubscribeToShard`.
- **KCL** — Kinesis Client Library; Amazon's reference stateful-consumer library, available for Java and Python.
- **pdata** — OpenTelemetry's in-memory representation of telemetry data inside the Collector.
- **Reshard** — A split or merge of Kinesis shards, changing the topology that consumers must follow.
