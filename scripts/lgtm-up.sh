#!/usr/bin/env bash
# Start a local Grafana LGTM stack (Loki, Grafana, Tempo, Prometheus + OTel Collector)
# using Apple's container runtime: https://github.com/apple/container
#
# Image: grafana/otel-lgtm (all-in-one OpenTelemetry backend for dev/demo).
set -euo pipefail

RUNTIME="${CONTAINER_RUNTIME:-container}"
NAME="${LGTM_CONTAINER_NAME:-tacklr-lgtm}"
IMAGE="${LGTM_IMAGE:-grafana/otel-lgtm:latest}"

# Host ports (Grafana on 3001 so testserver can keep ACP on 3000)
GRAFANA_PORT="${LGTM_GRAFANA_PORT:-3001}"
OTLP_GRPC_PORT="${LGTM_OTLP_GRPC_PORT:-4317}"
OTLP_HTTP_PORT="${LGTM_OTLP_HTTP_PORT:-4318}"
PROMETHEUS_PORT="${LGTM_PROMETHEUS_PORT:-9090}"
TEMPO_PORT="${LGTM_TEMPO_PORT:-3200}"
LOKI_PORT="${LGTM_LOKI_PORT:-3100}"
PYROSCOPE_PORT="${LGTM_PYROSCOPE_PORT:-4040}"

# Common Grafana Explore apps for LGTM (metrics / logs / traces / profiles).
# Built-in datasources (Prometheus, Loki, Tempo, Pyroscope) ship with the image.
GF_PLUGINS_PREINSTALL="${GF_PLUGINS_PREINSTALL:-grafana-lokiexplore-app,grafana-exploretraces-app,grafana-metricsdrilldown-app,grafana-pyroscope-app}"

# Tempo MCP for AI tools (optional; safe for local dev)
if [[ -z "${TEMPO_EXTRA_ARGS+x}" ]]; then
  TEMPO_EXTRA_ARGS="--query-frontend.mcp-server.enabled=true"
fi

if ! command -v "$RUNTIME" >/dev/null 2>&1; then
  echo "error: container runtime '$RUNTIME' not found on PATH" >&2
  echo "Install Apple's container CLI (brew install container) or set CONTAINER_RUNTIME." >&2
  exit 1
fi

# Replace an existing container with the same name.
if "$RUNTIME" list --all -q 2>/dev/null | grep -qx "$NAME" \
  || "$RUNTIME" inspect "$NAME" >/dev/null 2>&1; then
  echo "stopping existing container $NAME..."
  "$RUNTIME" stop "$NAME" 2>/dev/null || true
  "$RUNTIME" delete "$NAME" 2>/dev/null || true
fi

echo "pulling ${IMAGE}..."
"$RUNTIME" image pull "$IMAGE"

echo "starting ${NAME}..."
run_args=(
  run -d --name "$NAME" --rm
  -p "${GRAFANA_PORT}:3000"
  -p "${OTLP_GRPC_PORT}:4317"
  -p "${OTLP_HTTP_PORT}:4318"
  -p "${PROMETHEUS_PORT}:9090"
  -p "${TEMPO_PORT}:3200"
  -p "${LOKI_PORT}:3100"
  -p "${PYROSCOPE_PORT}:4040"
  -e "GF_PLUGINS_PREINSTALL=${GF_PLUGINS_PREINSTALL}"
  -e "GF_SECURITY_ADMIN_USER=${GF_SECURITY_ADMIN_USER:-admin}"
  -e "GF_SECURITY_ADMIN_PASSWORD=${GF_SECURITY_ADMIN_PASSWORD:-admin}"
  -e "GF_AUTH_ANONYMOUS_ENABLED=${GF_AUTH_ANONYMOUS_ENABLED:-false}"
  -e "TEMPO_EXTRA_ARGS=${TEMPO_EXTRA_ARGS}"
  -e "ENABLE_LOGS_OTELCOL=${ENABLE_LOGS_OTELCOL:-false}"
)
# Optional extra args as a single string, e.g. LGTM_EXTRA_RUN_ARGS='-m 4G'
if [[ -n "${LGTM_EXTRA_RUN_ARGS:-}" ]]; then
  # shellcheck disable=SC2206
  extra=( ${LGTM_EXTRA_RUN_ARGS} )
  run_args+=("${extra[@]}")
fi
run_args+=("$IMAGE")

"$RUNTIME" "${run_args[@]}"

echo
echo "LGTM is starting (first boot can take 30-60s while plugins install)."
echo
echo "  Grafana:     http://127.0.0.1:${GRAFANA_PORT}  (admin / admin)"
echo "  OTLP gRPC:   localhost:${OTLP_GRPC_PORT}"
echo "  OTLP HTTP:   localhost:${OTLP_HTTP_PORT}"
echo "  Prometheus:  http://127.0.0.1:${PROMETHEUS_PORT}"
echo "  Tempo:       http://127.0.0.1:${TEMPO_PORT}"
echo "  Loki:        http://127.0.0.1:${LOKI_PORT}"
echo "  Pyroscope:   http://127.0.0.1:${PYROSCOPE_PORT}"
echo
echo "Wire the testserver (also see deploy/lgtm/env.example):"
echo "  export OTEL_EXPORTER_OTLP_ENDPOINT=localhost:${OTLP_GRPC_PORT}"
echo "  export OTEL_EXPORTER_OTLP_PROTOCOL=grpc"
echo "  export OTEL_SERVICE_NAME=tacklr-testserver"
echo "  export PORT=3000"
echo "  go run ./cmd/testserver"
echo
echo "Or: make lgtm-testserver"
echo "Logs: $RUNTIME logs $NAME"
echo "Stop: make lgtm-down"
