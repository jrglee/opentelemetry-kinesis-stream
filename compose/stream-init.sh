#!/bin/sh
# Idempotent setup for the E2E stack: wait for MiniStack, create the Kinesis
# stream (multi-shard so two consumers can split ownership) and the
# KCL-shaped DynamoDB lease table.
set -eu

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
EP=http://ministack:4566

echo "waiting for ministack kinesis..."
until aws --endpoint-url "$EP" kinesis list-streams >/dev/null 2>&1; do
  sleep 1
done

aws --endpoint-url "$EP" kinesis create-stream \
  --stream-name otel-traces --shard-count 2 2>/dev/null || true
aws --endpoint-url "$EP" kinesis wait stream-exists --stream-name otel-traces

aws --endpoint-url "$EP" dynamodb create-table \
  --table-name otel-leases \
  --attribute-definitions AttributeName=leaseKey,AttributeType=S \
  --key-schema AttributeName=leaseKey,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST 2>/dev/null || true
aws --endpoint-url "$EP" dynamodb wait table-exists --table-name otel-leases

echo "stream-init done"
