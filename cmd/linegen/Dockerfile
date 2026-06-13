# Build context is the repo root (built as part of this module). linegen
# imports nothing outside the standard library, so the result is a tiny
# static binary on scratch.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /linegen ./cmd/linegen

FROM scratch
COPY --from=build /linegen /linegen
ENTRYPOINT ["/linegen"]
