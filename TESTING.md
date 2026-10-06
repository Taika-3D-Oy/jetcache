# Testing on Kubernetes with `wasip3`

Currently, `lattice-db` compiles to `wasm32-wasip3`. Official wasmCloud releases may not yet have the `wasip3` feature fully enabled by default. To run the full integration test suite against a Kubernetes cluster, you need to build a custom wasmCloud host with the `wasip3` feature and configure the local Kind cluster accordingly.

Here are the step-by-step instructions.

## 1. Build a custom wasmCloud host with `wasip3`

Clone the wasmCloud repository and build a custom Docker image from the `main` branch.

```bash
# Clone the repository
git clone --depth 1 https://github.com/wasmCloud/wasmCloud.git wasmcloud-src
cd wasmcloud-src

# Create a Dockerfile for the p3 build
cat << 'EOF' > Dockerfile.wasip3
FROM rust:1.85 AS builder
WORKDIR /src
COPY . .
RUN cargo build --release --bin wash --features wasip3

FROM debian:12-slim
COPY --from=builder /src/target/release/wash /usr/local/bin/wash
ENTRYPOINT ["wash"]
EOF

# Build the Docker image
docker build -f Dockerfile.wasip3 -t localhost:5001/wasmcloud/wash:p3 .
```

## 2. Load the custom image into Kind

Assuming your Kind cluster is already created (e.g., via `bash deploy/deploy-local.sh`), load the custom image into the cluster nodes:

```bash
kind load docker-image localhost:5001/wasmcloud/wash:p3 --name lattice-db
```

## 3. Patch the wasmCloud deployments

You need to update the `wasmcloud-host` deployment to use the custom image and the `--wasip3` flag. If testing without TLS, also remove the TLS arguments from the operator.

```bash
# Patch the hostgroup deployment to use the new image and flag
kubectl patch deployment hostgroup-default -n default --type='json' -p='[
  {"op": "replace", "path": "/spec/template/spec/containers/0/image", "value": "localhost:5001/wasmcloud/wash:p3"},
  {"op": "replace", "path": "/spec/template/spec/containers/0/imagePullPolicy", "value": "Never"},
  {"op": "replace", "path": "/spec/template/spec/containers/0/args", "value": [
    "host",
    "--wasip3",
    "--host-name=$(WASMCLOUD_HOST_IP)",
    "--host-group=default",
    "--scheduler-nats-url=nats://nats:4222",
    "--data-nats-url=nats://nats:4222",
    "--oci-cache-dir=/oci-cache",
    "--http-addr=0.0.0.0:9191",
    "--allow-insecure-registries"
  ]}
]'

# Patch the runtime operator mapping to strip TLS requirements (if skipping TLS)
kubectl patch deployment runtime-operator -n default --type='json' -p='[
  {"op": "replace", "path": "/spec/template/spec/containers/0/args", "value": [
    "-nats-url=nats://nats:4222"
  ]}
]'
```

Wait until the pods restart and show as `Running`.

## 4. Deploy the workload

For the wasm TCP stack to properly route, it currently does not fully support Kubernetes DNS resolution, so we inject the direct NATS ClusterIP.

```bash
# Get the NATS ClusterIP
NATS_IP=$(kubectl get svc nats -o jsonpath='{.spec.clusterIP}')

# Deploy the workload using the IP
sed "s/__NATS_URL__/${NATS_IP}:4222/" deploy/workloaddeployment.yaml | kubectl apply -f -
```

*Note: Ensure your `deploy/workloaddeployment.yaml` specifies the `sockets ── tcp` interface, but omits the `cli ── environment` interface, as the `wasip3` host handles environment variable provisioning implicitly via config.*

*Encryption note: the storage service refuses to boot without a master key (encryption is on by default). For test clusters, uncomment `LDB_DEV_SEED` in `deploy/workloaddeployment.yaml` (or set `LDB_MASTER_KEY`). `deploy/deploy-local.sh` already injects a dev seed automatically. Without a key the workload will crash-loop with a "no master key configured" error — that fail-closed behavior is intentional.*

## 5. Run the Integration Tests

Finally, expose the NATS cluster locally and run the integration test suite.

```bash
# Port-forward the NATS service to your local machine
kubectl port-forward svc/nats 4222:4222 &

# Run the 94 integration tests
cd lattice-db
NATS_URL=127.0.0.1:4222 bash tests/integration.sh
```

If successful, you should see:
```text
===================================
  Results: 94 passed, 0 failed, 26 total
===================================
```
