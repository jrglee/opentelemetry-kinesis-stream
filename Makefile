.PHONY: build test lint fmt tidy vet check clean

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

clean:
	rm -rf bin/ coverage.out
	go clean ./...
