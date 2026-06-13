.PHONY: build test lint fmt fmt-check tidy-check tidy vet check ci cover clean collector docker compose-up compose-down e2e

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
e2e:
	go test -tags e2e -count=1 -timeout 300s ./e2e/...

clean:
	rm -rf bin/ coverage.out compose/shared
	go clean ./...
