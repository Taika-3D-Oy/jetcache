# jetcache (formerly lattice-db)

A lightweight in-memory read-through cache for wasmCloud backed by NATS JetStream KV.

The engine is a `wasm32-wasip3` component that implements the official **NATS Microservices Framework (ADR-32)** and includes a high-performance local TCP listener. It connects to NATS, registers endpoints under its service group, persists every table to its own JetStream KV bucket, and serves low-latency key-value CRUD operations over standardized NATS microservice request/reply and local TCP.

```
clients ──NATS ADR-32 req/rep──▶ storage-service (Wasm component)
                                       │
co-located ──TCP :4080─────────▶       │
  components                           │
                                 NATS JetStream KV
                                 (one bucket per table: {instance}-{table})
```

- **In-Memory Cache with JetStream KV Persistence.** Ultra-fast in-memory reads backed by JetStream KV buckets.
- **Cross-Replica Cache Invalidation via KV Watchers.** Every replica keeps a local in-memory table cache and invalidates entries by subscribing to the bucket's change stream.
- **Atomic Concurrency Control.** Compare-and-swap (`cas`, `create`, `cas_delete`) built on JetStream KV revisions.
- **Prefix Scans.** Single round-trip key & value prefix retrieval (`prefix`) ideal for hierarchical partition keys and user-scoped data.
- **NATS ADR-32 Microservice Architecture.** Discovered and monitored via standard NATS tooling (`nats service ls`, `nats service info`, `nats service stats`, `nats service ping`).
- **Stateless Components.** A replica can restart, scale up, or scale down without data migration. Persistent state resides in JetStream KV.

## Operations

All requests are available over both NATS request/reply (`{instance}.{op}`) and localhost TCP framed messages. JSON payloads; binary values are base64-encoded.

| Operation | Request Payload | Description |
|---|---|---|
| `get` | `{"table": "...", "key": "...", "consistency"?: ...}` | Fetch row value & revision |
| `put` | `{"table": "...", "key": "...", "value": "...", "ttl_seconds"?: ...}` | Upsert row with optional TTL |
| `create` | `{"table": "...", "key": "...", "value": "...", "ttl_seconds"?: ...}` | Insert only if key does not exist |
| `cas` | `{"table": "...", "key": "...", "value": "...", "revision": 123, "ttl_seconds"?: ...}` | Atomic compare-and-swap by revision |
| `delete` | `{"table": "...", "key": "..."}` | Delete key |
| `cas_delete` | `{"table": "...", "key": "...", "revision": 123}` | Atomic delete by revision |
| `purge` | `{"table": "...", "key": "...", "revision"?: 123, "ttl_seconds"?: ...}` | Purge key tombstone |
| `exists` | `{"table": "...", "key": "...", "consistency"?: ...}` | Check if key exists |
| `keys` | `{"table": "...", "cursor"?: ...}` | Paginate keys in table |
| `prefix` | `{"table": "...", "prefix": "..."}` | Fetch all rows matching key prefix |
| `batch.get` | `{"table": "...", "keys": [...]}` | Fetch multiple keys in one round-trip |
| `batch.put` | `{"table": "...", "entries": [...]}` | Write multiple entries in one round-trip |
| `schema.set` | `{"table": "...", "schema": {...}}` | Set JSON validation schema |
| `schema.get` | `{"table": "..."}` | Retrieve table schema |
| `schema.delete`| `{"table": "..."}` | Remove table schema |

Every mutation also publishes a change event on `{instance}-events.{table}.{key}`:

```bash
nats sub "lid-events.users.>"
```

## Configuration

Both modern `CACHE_*` and backward-compatible `LDB_*` environment variables are supported:

| Environment Variable | Fallback | Default | Effect |
|---|---|---|---|
| `CACHE_INSTANCE` | `LDB_INSTANCE` | `lid` | NATS subject prefix and KV bucket namespace (e.g. `lid.get`, `lid-users`) |
| `CACHE_DATA_INSTANCE` | `LDB_DATA_INSTANCE` | (same as instance) | NATS KV bucket namespace prefix |
| `CACHE_AUTH_TOKEN` | `LDB_AUTH_TOKEN` | (none) | Required token sent as `"_auth": "..."` |
| `CACHE_NATS_URL` | `NATS_URL` | (none) | NATS address for messaging and request/reply |
| `CACHE_DATA_URL` | `NATS_DATA_URL` | (same as NATS_URL) | NATS address for storage (KV buckets) |
| `CACHE_TCP_PORT` | `LDB_TCP_PORT` / `TCP_PORT` | `4080` | Localhost TCP loopback port for co-located components |
| `CACHE_MASTER_KEY` | `LDB_MASTER_KEY` | (none) | Master key for AES-GCM encrypted tables |
| `CACHE_DEV_SEED` | `LDB_DEV_SEED` | (none) | Deterministic dev key derivation seed |

### Rust Client

```rust
use lattice_db_client::TaikaCache; // or LatticeDb

let cache = TaikaCache::new(client)
    .with_instance("lid")
    .with_auth("secret");

// Prefix scan
let user_keys = cache.prefix("oidc_keys", "user:1234:").await?;
```

```rust
let db = LatticeDb::new(client)
    .with_instance("instancename")   // must match LDB_INSTANCE on the server
    .with_auth("secret");      // must match LDB_AUTH_TOKEN
```

## Build & run

Builds with stable Rust targeting `wasm32-wasip2`. Output is `target/wasm32-wasip2/release/storage_service.wasm`.

```bash
cargo build --target wasm32-wasip2 --release

# Run with wasmtime against a local NATS server
nats-server -js -p 14222 &
wasmtime run -S inherit-network=y -W component-model-async=y \
  --env NATS_URL=127.0.0.1:14222 \
  target/wasm32-wasip2/release/storage_service.wasm
```

For a full Kind + wasmCloud + mTLS local environment:

```bash
bash deploy/deploy-local.sh           # full setup
bash deploy/deploy-local.sh rebuild   # rebuild service only
bash deploy/deploy-local.sh teardown
```

Prerequisites: `kind`, `kubectl`, `helm`, `docker`, `cargo`, `wash`. Runtime requirement: wasmCloud ≥ 2.7.0 or Wasmtime ≥ 47.

For a public-registry deployment, push `storage_service.wasm` to OCI and apply [`deploy/workloaddeployment-public.yaml`](deploy/workloaddeployment-public.yaml).

## Co-located TCP service (wasmCloud)

When running on wasmCloud v2, you can deploy `storage-service` as a **co-located service** inside a `WorkloadDeployment`. Components in the same workload connect over localhost TCP (`127.0.0.1:4080`) instead of going through NATS request/reply — eliminating a network hop and giving sub-millisecond latency for reads.

```
┌─ WorkloadDeployment ──────────────────────────────┐
│                                                    │
│  component-a ──TCP──▶ storage-service ──▶ NATS KV │
│  component-b ──TCP──┘       :4080                  │
│                                                    │
└────────────────────────────────────────────────────┘
```

### Wire protocol

The TCP protocol uses framed commands matching the NATS ADR-32 endpoint dispatch:

**Request frame:**
```
┌──────────────┬────────────┬──────────────────┬────────────────────────┐
│ 4 bytes (BE) │ 1 byte     │ op bytes (UTF-8) │ JSON payload bytes     │
│ total length │ op length  │ (e.g. "get")     │ (e.g. {"table":".."})  │
└──────────────┴────────────┴──────────────────┴────────────────────────┘
```

**Response frame:**
```
┌──────────────┬──────────────────┬─────────────────────────────────────┐
│ 4 bytes (BE) │ 2 bytes (BE)     │ Response payload bytes              │
│ total length │ status code      │ (JSON result if 0, error if != 0)   │
│              │ (0 = OK)         │                                     │
└──────────────┴──────────────────┴─────────────────────────────────────┘
```

### Configuration

| Env var | Default | Description |
|---|---|---|
| `LDB_TCP_PORT` | `4080` | Port to listen on (localhost only) |

The TCP listener runs alongside the NATS ADR-32 microservice endpoints. All database operations (`get`, `put`, `cas`, `scan`, `txn`, etc.) execute identically with zero overhead.

### Deployment example (WorkloadDeployment)

```yaml
apiVersion: runtime.wasmcloud.dev/v1alpha1
kind: WorkloadDeployment
metadata:
  name: my-app
spec:
  lattice: default
  service:
    name: storage-service
    image: ghcr.io/your-org/lattice-db/storage-service:latest
    imagePullPolicy: Always
    config:
      - name: storage-service-config  # ConfigMap with NATS_URL, LDB_INSTANCE, etc.
  components:
    - name: my-component
      image: ghcr.io/your-org/my-component:latest
      replicas: 1
```

The service runs as a sidecar — one long-lived process that all component instances in the workload connect to.

### Client example (Rust, wasm32-wasip3)

```rust
use wasi::sockets::types::{IpAddressFamily, IpSocketAddress, Ipv4SocketAddress, TcpSocket};

async fn ldb_request(op: &str, payload: &serde_json::Value) -> Result<serde_json::Value, String> {
    let body = serde_json::to_vec(payload).map_err(|e| e.to_string())?;

    // Connect to co-located storage-service
    let socket = TcpSocket::create(IpAddressFamily::Ipv4).unwrap();
    let addr = IpSocketAddress::Ipv4(Ipv4SocketAddress { port: 4080, address: (127, 0, 0, 1) });
    socket.connect(addr).await.unwrap();

    let (mut rx, _rx_done) = socket.receive();
    let (mut tx, tx_rx) = wit_stream::new::<u8>();
    let _send = socket.send(tx_rx);

    // Write: [4-byte total_len][1-byte op_len][op bytes][payload]
    let op_bytes = op.as_bytes();
    let total_len = (1 + op_bytes.len() + body.len()) as u32;
    let mut frame = Vec::with_capacity(4 + 1 + op_bytes.len() + body.len());
    frame.extend_from_slice(&total_len.to_be_bytes());
    frame.push(op_bytes.len() as u8);
    frame.extend_from_slice(op_bytes);
    frame.extend_from_slice(&body);
    tx.write_all(frame).await;
    drop(tx);

    // Read: 4-byte total_len + 2-byte status_code + response body
    let mut buf = Vec::new();
    while buf.len() < 4 {
        let (status, data) = rx.read(Vec::with_capacity(4096)).await;
        match status {
            StreamResult::Complete(0) => return Err("eof".into()),
            StreamResult::Complete(n) => buf.extend_from_slice(&data[..n]),
            _ => return Err("read error".into()),
        }
    }
    let total_resp_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    buf.drain(..4);
    while buf.len() < total_resp_len {
        let (status, data) = rx.read(Vec::with_capacity(4096)).await;
        match status {
            StreamResult::Complete(0) => return Err("eof".into()),
            StreamResult::Complete(n) => buf.extend_from_slice(&data[..n]),
            _ => return Err("read error".into()),
        }
    }

    let status_code = u16::from_be_bytes([buf[0], buf[1]]);
    let resp_payload = &buf[2..total_resp_len];
    let val: serde_json::Value = serde_json::from_slice(resp_payload).map_err(|e| e.to_string())?;
    if status_code != 0 {
        return Err(val.get("error").and_then(|v| v.as_str()).unwrap_or("error").to_string());
    }
    Ok(val)
}
```

## Test

```bash
bash tests/integration.sh             # 190 tests, plain local NATS
bash tests/integration.sh --tls       # against the Kind cluster with mTLS
```

Requires `nats` CLI, `jq`, `base64`. See [TESTING.md](TESTING.md) for Kubernetes setup.

## Building and Running the Benchmark

```bash
# Build storage-service (works with stable Rust on wasm32-wasip2)
cargo build --target wasm32-wasip2 --release -p storage-service

# Build benchmark client
cargo build --target wasm32-wasip2 --release --example bench -p jetcache-client

nats-server -js -p 14222 &
wasmtime run -S inherit-network=y -W component-model-async=y \
  --env NATS_URL=127.0.0.1:14222 \
  target/wasm32-wasip2/release/storage_service.wasm &

wasmtime run -S inherit-network=y -W component-model-async=y \
  --env NATS_URL=127.0.0.1:14222 \
  --env BENCH_DURATION_SECS=10 \
  --env BENCH_CONCURRENCY=64 \
  --env BENCH_TXN_CONCURRENCY=8 \
  target/wasm32-wasip2/release/examples/bench.wasm
```

Tunables: `BENCH_DURATION_SECS` (10), `BENCH_CONCURRENCY` (64), `BENCH_TXN_CONCURRENCY` (8), `BENCH_MSG_SIZE` (256), `BENCH_TABLES` (8), `BENCH_TXN_OPS` (2).

## Project layout

```
storage-service/    # the cache service (wasm component)
  src/main.rs       #   NATS connection, queue subscription, watchers
  src/tcp_server.rs #   localhost TCP listener for co-located component access
  src/handler.rs    #   request dispatch for all {instance}.* operations
  src/state.rs      #   in-memory cache
  src/store.rs      #   NATS KV persistence
jetcache-client/    # typed Rust SDK (published on crates.io)
  examples/bench.rs #   the benchmark used above
deploy/             # Kind + wasmCloud local environment
tests/              # integration test suites
```

## Crates

| Crate | Description |
|---|---|
| `storage-service` | The cache service |
| [`jetcache-client`](jetcache-client/) | Typed Rust SDK |
| [`nats-wasip3`](https://crates.io/crates/nats-wasip3) | NATS client for WASI 0.3 / `wasm32-wasip2` (published separately) |

## License

Apache-2.0
