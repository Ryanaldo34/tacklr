#!/usr/bin/env bash
# Run cmd/testserver with OTLP pointed at a local LGTM stack (make lgtm-up first).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Defaults for local LGTM (do not override vars the user already set).
export OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-localhost:4317}"
export OTEL_EXPORTER_OTLP_PROTOCOL="${OTEL_EXPORTER_OTLP_PROTOCOL:-grpc}"
export OTEL_SERVICE_NAME="${OTEL_SERVICE_NAME:-tacklr-testserver}"
export PORT="${PORT:-3000}"

# Load repo .env without clobbering exports above (testserver also loads .env).
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env 2>/dev/null || true
  set +a
  # Re-assert LGTM OTLP defaults if .env left them empty.
  export OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-localhost:4317}"
  export OTEL_EXPORTER_OTLP_PROTOCOL="${OTEL_EXPORTER_OTLP_PROTOCOL:-grpc}"
  export OTEL_SERVICE_NAME="${OTEL_SERVICE_NAME:-tacklr-testserver}"
fi

echo "testserver → OTLP ${OTEL_EXPORTER_OTLP_PROTOCOL}://${OTEL_EXPORTER_OTLP_ENDPOINT} service=${OTEL_SERVICE_NAME} port=${PORT}"
exec go run ./cmd/testserver "$@"
