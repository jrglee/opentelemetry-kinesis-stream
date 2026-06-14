.PHONY: build test lint fmt fmt-check tidy-check tidy vet check ci cover clean collector docker compose-up compose-down e2e e2e-matrix perf perf-parallel

COMPOSE := docker compose -f compose/docker-compose.yaml

build:
	go build ./...

test:
	go test ./...

lint:
	golangci-lint run

fmt:
	gofumpt -w .
	go mod tidy

tidy:
	go mod tidy

vet:
	go vet ./...

check: fmt vet lint test

# CI gate: like check, but read-only — fails on unformatted or untidy code
# instead of rewriting it.
fmt-check:
	@out="$$(gofumpt -l .)"; if [ -n "$$out" ]; then echo "gofumpt needed on:"; echo "$$out"; exit 1; fi

tidy-check:
	go mod tidy
	@git diff --exit-code go.mod go.sum || (echo "go.mod/go.sum not tidy; run 'go mod tidy'"; exit 1)

ci: fmt-check tidy-check vet lint test

# Per-package statement coverage summary.
cover:
	go test -cover ./internal/... ./exporter/... ./receiver/...

collector:
	go build -o bin/otelcol-kinesis ./cmd/otelcol-kinesis

docker:
	docker build -f cmd/otelcol-kinesis/Dockerfile -t otelcol-kinesis:dev .

compose-up:
	$(COMPOSE) up -d --build

compose-down:
	$(COMPOSE) down -v

# E2E spins the full stack up and down itself; -count=1 defeats caching.
# ENCODING and COMPRESSION are passed through to the compose configs so the
# same harness can spot-check different combos without editing YAML.
e2e:
	go test -tags e2e -count=1 -timeout 300s ./e2e/...

# Sequential sweep over a small representative set of (encoding, compression)
# combos. Heavy — ~15 min per combo — opt-in, not part of `check`.
E2E_MATRIX ?= otlp_proto/none otlp_proto/zstd otlp_json/zstd otlp_json/gzip
e2e-matrix:
	@set -e; for combo in $(E2E_MATRIX); do \
	  enc=$${combo%/*}; cmp=$${combo#*/}; \
	  echo "=== e2e: encoding=$$enc compression=$$cmp ==="; \
	  ENCODING=$$enc COMPRESSION=$$cmp $(MAKE) e2e; \
	done

# Reproducible encode/decode benchmark sweep across (profile, encoding, codec,
# batch size). Output is the standard `go test -bench` format; pipe through
# benchstat to compare runs. Determinism: compressed_bytes, ratio, and
# bytes-per-record are seed-driven and stable across machines; ns/op varies.
#
# `perf` runs sequentially: clean numbers, CI-safe, no cross-bench cache or
# memory-bandwidth contention. The default for CI and for any comparison
# you intend to publish.
PERF_BENCHTIME ?= 2s
PERF_COUNT     ?= 3
perf:
	go test -tags perf -bench=. -benchmem -benchtime=$(PERF_BENCHTIME) -count=$(PERF_COUNT) -run=^$$ ./perf/...

# `perf-parallel` runs the four phases (EncodeMetrics, DecodeMetrics,
# EncodeTraces, DecodeTraces) concurrently, each pinned to a slice of cores
# via `-cpu`. Faster on a workstation; not for CI, since cross-process
# memory-bandwidth and cache contention adds noise to ns/op. Sizes and
# ratios are unaffected (deterministic), so this is fine for a
# pre-publication spot-check before a `make perf` run.
PERF_PARALLEL_CPU ?= 6
PERF_OUT_DIR      ?= perf-out
perf-parallel:
	@mkdir -p $(PERF_OUT_DIR)
	@echo "running 4 phases in parallel, GOMAXPROCS=$(PERF_PARALLEL_CPU) each"
	@set -e ; \
	  go test -tags perf -bench=BenchmarkEncodeMetrics -benchmem -benchtime=$(PERF_BENCHTIME) -count=$(PERF_COUNT) -cpu=$(PERF_PARALLEL_CPU) -run=^$$ ./perf/ > $(PERF_OUT_DIR)/encode-metrics.txt 2>&1 & PID_EM=$$! ; \
	  go test -tags perf -bench=BenchmarkDecodeMetrics -benchmem -benchtime=$(PERF_BENCHTIME) -count=$(PERF_COUNT) -cpu=$(PERF_PARALLEL_CPU) -run=^$$ ./perf/ > $(PERF_OUT_DIR)/decode-metrics.txt 2>&1 & PID_DM=$$! ; \
	  go test -tags perf -bench=BenchmarkEncodeTraces  -benchmem -benchtime=$(PERF_BENCHTIME) -count=$(PERF_COUNT) -cpu=$(PERF_PARALLEL_CPU) -run=^$$ ./perf/ > $(PERF_OUT_DIR)/encode-traces.txt 2>&1 & PID_ET=$$! ; \
	  go test -tags perf -bench=BenchmarkDecodeTraces  -benchmem -benchtime=$(PERF_BENCHTIME) -count=$(PERF_COUNT) -cpu=$(PERF_PARALLEL_CPU) -run=^$$ ./perf/ > $(PERF_OUT_DIR)/decode-traces.txt 2>&1 & PID_DT=$$! ; \
	  fail=0 ; \
	  wait $$PID_EM || fail=1 ; \
	  wait $$PID_DM || fail=1 ; \
	  wait $$PID_ET || fail=1 ; \
	  wait $$PID_DT || fail=1 ; \
	  if [ $$fail -ne 0 ]; then echo "one or more phases failed; see $(PERF_OUT_DIR)/" >&2; exit 1; fi
	@echo "done; outputs in $(PERF_OUT_DIR)/"
	@cat $(PERF_OUT_DIR)/encode-metrics.txt $(PERF_OUT_DIR)/decode-metrics.txt $(PERF_OUT_DIR)/encode-traces.txt $(PERF_OUT_DIR)/decode-traces.txt

clean:
	rm -rf bin/ coverage.out compose/shared
	go clean ./...
