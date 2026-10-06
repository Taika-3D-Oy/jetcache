#!/usr/bin/env bash
# deploy-local.sh — Deploy lattice-db on a local Kind cluster.
#
# Sets up a single Kind cluster with NATS + JetStream and deploys the
# storage-service as a wasmCloud service workload. NATS is exposed on
# localhost:4222 so you can test with the nats CLI from the host.
#
# Usage:
#   bash deploy/deploy-local.sh            # full setup from scratch
#   bash deploy/deploy-local.sh rebuild    # rebuild + redeploy service only
#   bash deploy/deploy-local.sh teardown   # destroy everything
#   bash deploy/deploy-local.sh status     # show cluster status

set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER_NAME="lattice-db"
NAMESPACE="default"
REGISTRY_NAME="kind-registry"
REGISTRY_PORT=5001
HELM_VERSION="2.0.1"

log() { echo "==> $*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

# ── Prerequisite checks ─────────────────────────────────────

check_prereqs() {
  local missing=()
  for cmd in kind kubectl helm docker cargo wash; do
    command -v "$cmd" &>/dev/null || missing+=("$cmd")
  done
  if [[ ${#missing[@]} -gt 0 ]]; then
    die "Missing required tools: ${missing[*]}"
  fi
}

# ── Teardown ─────────────────────────────────────────────────

teardown() {
  log "Tearing down lattice-db local environment"
  kind delete cluster --name "$CLUSTER_NAME" 2>/dev/null || true
  docker rm -f "$REGISTRY_NAME" 2>/dev/null || true
  log "Done"
}

if [[ "${1:-}" == "teardown" ]]; then
  teardown
  exit 0
fi

# ── Status ───────────────────────────────────────────────────

if [[ "${1:-}" == "status" ]]; then
  echo "--- Cluster ---"
  kind get clusters 2>/dev/null | grep "$CLUSTER_NAME" && echo "Cluster: running" || echo "Cluster: not found"
  echo ""
  echo "--- NATS ---"
  kubectl get pods -l app=nats -o wide 2>/dev/null || echo "NATS: not deployed"
  echo ""
  echo "--- wasmCloud ---"
  kubectl get pods -l app.kubernetes.io/name=runtime-operator -o wide 2>/dev/null || true
  kubectl get pods -l wasmcloud.com/hostgroup -o wide 2>/dev/null || true
  echo ""
  echo "--- Workloads ---"
  kubectl get workloaddeployment 2>/dev/null || echo "No workloads"
  echo ""
  echo "--- Test command ---"
  echo '  nats req ldb.put '\''{"table":"test","key":"hello","value":"'"$(echo -n '{"msg":"world"}' | base64)"'"}'\'''
  exit 0
fi

check_prereqs

# ── Build & Push ─────────────────────────────────────────────

build_and_push() {
  log "Building workspace (release, wasm32-wasip3)"
  cargo build --workspace --target wasm32-wasip3 --release

  log "Pushing storage-service to local registry"
  wash oci push --insecure "localhost:${REGISTRY_PORT}/lattice-db/storage-service:dev" \
    target/wasm32-wasip3/release/storage_service.wasm
}

# ── Deploy workload with TLS certs ───────────────────────────

deploy_workload() {
  log "Extracting TLS certs from wasmcloud-data-tls secret"

  # The NATS server's CN is "wasmcloud-data" — that's the SNI we must use.
  local ca_pem cert_pem key_pem nats_ip
  ca_pem=$(kubectl get secret wasmcloud-data-tls -o jsonpath='{.data.ca\.crt}' | base64 -d)
  cert_pem=$(kubectl get secret wasmcloud-data-tls -o jsonpath='{.data.tls\.crt}' | base64 -d)
  key_pem=$(kubectl get secret wasmcloud-data-tls -o jsonpath='{.data.tls\.key}' | base64 -d)

  # WASI sockets sandbox can't do DNS — use the NATS ClusterIP directly.
  nats_ip=$(kubectl get svc nats -o jsonpath='{.spec.clusterIP}')

  log "Templating workload deployment with mTLS certs (NATS @ ${nats_ip})"
  python3 - "$ca_pem" "$cert_pem" "$key_pem" "$nats_ip" <<'PYEOF'
import sys, json

ca_pem, cert_pem, key_pem, nats_ip = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

doc = {
    "apiVersion": "runtime.wasmcloud.dev/v1alpha1",
    "kind": "WorkloadDeployment",
    "metadata": {
        "name": "lattice-db",
        "annotations": {"description": "lattice-db storage service (local dev, mTLS)"}
    },
    "spec": {
        "replicas": 1,
        "deployPolicy": "RollingUpdate",
        "template": {
            "labels": {
                "app.kubernetes.io/name": "lattice-db",
                "app.kubernetes.io/component": "storage"
            },
            "spec": {
                "components": [],
                "service": {
                    "image": "kind-registry:5000/lattice-db/storage-service:dev",
                    "maxRestarts": 5,
                    "localResources": {
                        "environment": {
                            "config": {
                                "NATS_URL": nats_ip + ":4222",
                                "NATS_TLS_SERVER_NAME": "nats",
                                "NATS_CA_PEM": ca_pem,
                                "NATS_CERT_PEM": cert_pem,
                                "NATS_KEY_PEM": key_pem,
                                # Encryption is on by default; local dev uses a
                                # deterministic dev seed (never use in production).
                                "LDB_DEV_SEED": "lattice-db-local-dev",
                            }
                        }
                    }
                }
            }
        }
    }
}

with open("/tmp/lattice-db-workload.json", "w") as f:
    json.dump(doc, f)
print("Wrote /tmp/lattice-db-workload.json")
PYEOF

  kubectl apply -f /tmp/lattice-db-workload.json
}

# ── Rebuild mode ─────────────────────────────────────────────

if [[ "${1:-}" == "rebuild" ]]; then
  build_and_push

  log "Clearing OCI cache and redeploying"
  for pod in $(kubectl get pods -l wasmcloud.com/hostgroup -o name 2>/dev/null); do
    kubectl exec "$pod" -- \
      sh -c 'rm -rf /oci-cache/kind-registry_5000_*' 2>/dev/null || true
  done
  kubectl delete workloaddeployment --all 2>/dev/null || true
  sleep 2
  deploy_workload
  log "Rebuild done — waiting for workload to come up"
  sleep 5
  kubectl get workloaddeployment
  exit 0
fi

# ══════════════════════════════════════════════════════════════
# Full Setup
# ══════════════════════════════════════════════════════════════

# 1. Docker registry (shared, HTTP — no TLS for local dev)
if docker inspect "$REGISTRY_NAME" &>/dev/null; then
  log "Registry '$REGISTRY_NAME' already running"
else
  log "Starting local Docker registry on port ${REGISTRY_PORT}"
  docker run -d --restart=always -p "${REGISTRY_PORT}:5000" \
    --network bridge --name "$REGISTRY_NAME" registry:2
fi

# 2. Create Kind cluster
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  log "Kind cluster '$CLUSTER_NAME' already exists"
else
  log "Creating Kind cluster '$CLUSTER_NAME'"
  kind create cluster --name "$CLUSTER_NAME" --config deploy/kind-config.yaml
fi

# Connect registry to Kind Docker network
docker network connect kind "$REGISTRY_NAME" 2>/dev/null || true

# 3. In-cluster registry DNS (so pods can pull from kind-registry:5000)
log "Setting up in-cluster registry DNS"
REGISTRY_IP=$(docker inspect -f '{{.NetworkSettings.Networks.kind.IPAddress}}' "$REGISTRY_NAME")
kubectl apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: kind-registry
  namespace: $NAMESPACE
spec:
  type: ClusterIP
  ports:
    - port: 5000
      targetPort: 5000
---
apiVersion: v1
kind: Endpoints
metadata:
  name: kind-registry
  namespace: $NAMESPACE
subsets:
  - addresses:
      - ip: ${REGISTRY_IP}
    ports:
      - port: 5000
EOF

# 4. wasmCloud operator (includes NATS with mTLS)
log "Installing wasmCloud runtime-operator v${HELM_VERSION}"
helm install wasmcloud oci://ghcr.io/wasmcloud/charts/runtime-operator \
  --namespace "$NAMESPACE" \
  --version "$HELM_VERSION" \
  --set 'gateway.service.type=NodePort' \
  --set 'gateway.service.nodePort=30950' 2>/dev/null || \
  log "wasmcloud already installed"

log "Waiting for wasmCloud pods"
kubectl wait --for=condition=available --timeout=120s \
  deployment --all 2>/dev/null || true

# 5. Expose NATS on NodePort for host-side testing
log "Patching NATS service to NodePort 30422"
kubectl patch svc nats --type=json \
  -p '[{"op":"replace","path":"/spec/type","value":"NodePort"},{"op":"add","path":"/spec/ports/0/nodePort","value":30422}]' \
  2>/dev/null || true

# 6. Patch host to allow insecure (HTTP) registry pulls
log "Patching host for insecure registry"
kubectl patch deploy hostgroup-default --type=json \
  -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--allow-insecure-registries"}]' \
  2>/dev/null || true

kubectl rollout status deploy/hostgroup-default --timeout=60s 2>/dev/null || true
sleep 5

# 7. Build and push
build_and_push

# 8. Deploy workload (with TLS cert injection)
deploy_workload

# 9. Wait and verify
log "Waiting for workload to start"
sleep 10

echo ""
log "Deployment complete!"
echo ""
echo "  Cluster:  kind-${CLUSTER_NAME}"
echo "  NATS:     nats:4222  (NodePort 30422, mTLS)"
echo ""
echo "  IMPORTANT: Add hostname for TLS cert validation (one-time):"
echo "    echo '127.0.0.1 nats' | sudo tee -a /etc/hosts"
echo ""
echo "  Run integration tests:"
echo "    bash tests/integration.sh --tls"
echo ""
echo "  Rebuild after code changes:"
echo "    bash deploy/deploy-local.sh rebuild"
echo ""
echo "  Tear down:"
echo "    bash deploy/deploy-local.sh teardown"
