#!/usr/bin/env bash
# dev.sh — Quick local dev loop without Kubernetes.
#
# Starts a NATS server (Docker) and runs storage-service via wasmtime.
# Requires: docker, cargo, wasmtime
#
# Usage:
#   bash dev.sh          # build + run
#   bash dev.sh build    # build only

set -euo pipefail
cd "$(dirname "$0")"

NATS_CONTAINER="lattice-db-nats"
NATS_PORT=4222

cleanup() {
    echo ""
    echo "==> Stopping..."
    docker rm -f "$NATS_CONTAINER" 2>/dev/null || true
}

# ── Build ────────────────────────────────────────────────────

echo "==> Building workspace..."
cargo build --workspace --target wasm32-wasip2

if [[ "${1:-}" == "build" ]]; then
  echo "==> Build complete."
  exit 0
fi

trap cleanup EXIT

# ── NATS ─────────────────────────────────────────────────────

if docker inspect "$NATS_CONTAINER" &>/dev/null; then
  echo "==> NATS already running"
else
  echo "==> Starting NATS with JetStream on :${NATS_PORT}"
  docker run -d --rm \
    --name "$NATS_CONTAINER" \
    -p "${NATS_PORT}:4222" \
    nats:2-alpine -js
fi

# Wait for NATS to be ready
for i in $(seq 1 15); do
  if docker exec "$NATS_CONTAINER" nats-server --help &>/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done
sleep 1

# ── Run ──────────────────────────────────────────────────────

echo ""
echo "==> Running lattice-db storage-service"
echo "    NATS: localhost:${NATS_PORT}"
echo ""
echo "  Test with:"
echo "    nats req ldb.put '{\"table\":\"test\",\"key\":\"k1\",\"value\":\"$(echo -n '{"hello":"world"}' | base64)\"}'"
echo "    nats req ldb.get '{\"table\":\"test\",\"key\":\"k1\"}'"
echo ""

exec wasmtime run \
  -S inherit-network=y \
  --env NATS_URL=127.0.0.1:${NATS_PORT} \
  --env LDB_DEV_SEED=lattice-db-local-dev \
  target/wasm32-wasip2/debug/storage_service.wasm
