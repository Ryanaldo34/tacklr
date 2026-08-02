#!/usr/bin/env bash
# Stop and remove the local LGTM container started by lgtm-up.sh.
set -euo pipefail

RUNTIME="${CONTAINER_RUNTIME:-container}"
NAME="${LGTM_CONTAINER_NAME:-tacklr-lgtm}"

if ! command -v "$RUNTIME" >/dev/null 2>&1; then
  echo "error: container runtime '$RUNTIME' not found" >&2
  exit 1
fi

echo "stopping $NAME..."
"$RUNTIME" stop "$NAME" 2>/dev/null || true
"$RUNTIME" delete "$NAME" 2>/dev/null || true
echo "done"
