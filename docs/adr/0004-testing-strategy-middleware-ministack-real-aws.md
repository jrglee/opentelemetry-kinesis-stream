# 0004. Testing strategy: middleware fakes, MiniStack, real AWS

- **Status:** Accepted
- **Date:** 2026-06-12

## Context

This project is being developed without immediate access to real AWS, so
local testing is load-bearing rather than a convenience. Three distinct
needs:

1. Unit tests of marshaling, partitioning, retry, and error handling.
2. Stateful integration tests of shard lifecycle, resharding, lease
   coordination, and DynamoDB-backed checkpointing.
3. A path to validate against real AWS when credentials are available.

Tools considered:

- **LocalStack** is the obvious historical answer. As of March 2026 the
  community edition was sunset; LocalStack is effectively unavailable for
  non-commercial use. Removed from consideration.
- **MiniStack** (ministack.org, MIT-licensed) shipped after LocalStack's
  sunset to fill the same gap. Covers Kinesis, DynamoDB, STS, and ~35 other
  services in a single Docker image (~200 MB, ~2 s start). Actively released
  through at least June 2026.
- **kinesalite** (in-process Node.js Kinesis mock). Long-standing, but
  effectively unmaintained — not a credible foundation for ongoing work.
- **DynamoDB Local** is official and maintained, but covers only DynamoDB,
  so it would have to be paired with a separate Kinesis emulator.
- **AWS SDK for Go v2 Smithy middleware injection.** AWS-documented pattern:
  install a deserialize-step middleware via `aws.Config.APIOptions` that
  fabricates the response. The test subject is the real `*kinesis.Client`,
  including its serialization, retry, and error-handling paths. No fake
  server, no Docker, no narrow interface to maintain.
- **A hand-rolled in-process stateful fake.** Cheap to write, fast, but
  cannot keep up with real AWS semantics as they evolve. Acceptable for
  narrow correctness invariants (parent-drains-before-child during a synthetic
  reshard) only when an emulator's behavior is itself in question.

## Decision

Layered, lazy-loaded:

- **Layer 1 — Unit tests.** Use the AWS SDK for Go v2 Smithy middleware
  pattern to fabricate responses. The real client is the test subject.
- **Layer 2 — Stateful integration tests.** Use MiniStack via testcontainers.
  One container covers Kinesis, DynamoDB, and STS, which removes the need
  for a separate DynamoDB-Local dependency. MiniStack is maintained against
  real AWS APIs, which is the property we cannot reproduce in-process.
- **Layer 3 — Real AWS.** Behind a build tag, run manually when credentials
  are available.

There is **no narrow `KinesisClient` / `DynamoClient` interface in this
codebase**. The middleware approach removes the need for one, and avoiding
it removes one ongoing maintenance surface.

No test-harness code lands as part of scaffolding. The harness for each
layer is added when the first test that needs that layer is written.

## Consequences

- Unit tests stay fast and run anywhere `go test` runs, with no Docker.
- Integration tests stay realistic: MiniStack tracks real AWS behavior more
  faithfully than an in-process fake could, and is one of the few options
  still maintained against the moving target of the AWS APIs.
- The codebase stays free of a narrow client-interface seam.
- Verify on first integration-test write whether MiniStack's shard
  split/merge semantics are faithful enough to validate the
  parent-drains-before-child invariant. If not, that one invariant is the
  place where a narrow in-process fake earns its keep — locally, for that
  test class only.
- Real-AWS validation is opt-in, not a default. Document the build tag and
  required environment when the first such test lands.
- Revisit if MiniStack stops being actively maintained, or if a different
  AWS-emulation tool becomes notably more faithful for Kinesis specifically.
