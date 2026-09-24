# jetcache-client

Typed Rust SDK for [jetcache](https://github.com/Taika-3D-Oy/jetcache) (formerly `lattice-db`) — an in-memory read-through cache for wasmCloud backed by NATS JetStream KV.

Wraps the NATS request/reply wire protocol so you can call `put`, `get`, `create`, `cas`, `prefix`, etc. with strongly-typed Rust methods instead of hand-rolling JSON payloads.

## Requirements

- **Target**: `wasm32-wasip2` (WASI 0.3 Component Model async I/O)
- **Toolchain**: Rust stable ≥ 1.85
- **Runtime**: [wasmCloud](https://wasmcloud.com) ≥ 2.7.0, or [Wasmtime](https://wasmtime.dev) ≥ 47
- **NATS**: A running NATS server with JetStream enabled

## Quick start

Add to your `Cargo.toml`:

```toml
[dependencies]
jetcache-client = "2.0.0-rc.1"
nats-wasip3 = "1.0.0-rc.1"
```

```rust
use nats_wasip3::client::{Client, ConnectConfig};
use jetcache_client::JetCache;

let client = Client::connect(ConnectConfig::default()).await?;
// Optional: auth token (required when server sets JETCACHE_AUTH_TOKEN)
//           instance  (must match JETCACHE_INSTANCE on the server, default "lid")
let cache = JetCache::new(client)
    .with_auth("my-secret-token")
    .with_instance("lid");

// Store and retrieve JSON
cache.put_json("users", "alice", &serde_json::json!({"name": "Alice"})).await?;
let user: serde_json::Value = cache.get_json("users", "alice").await?;

// Prefix scan
let results = cache.prefix("users", "ali").await?;
```

## Operations

| Method | Description |
|--------|-------------|
| `put` / `put_json` | Store a key-value pair |
| `put_with_ttl` | Store with an expiry (seconds) |
| `get` / `get_json` | Retrieve a value by key |
| `prefix` | Fetch all keys & values matching a prefix |
| `delete` | Remove a key |
| `exists` | Check if a key exists |
| `keys` | List all keys in a table |
| `create` / `create_json` | Insert only if key doesn't exist |
| `create_with_ttl` | Insert with an expiry (fails if key exists) |
| `cas` | Compare-and-swap (optimistic concurrency) |
| `cas_with_ttl` | Compare-and-swap with an expiry |
| `cas_delete` | CAS delete — only if revision matches |
| `purge` | Remove all revisions of a key |
| `purge_with_ttl` | Purge with an expiring tombstone |
| `purge_expect_revision` | CAS purge — only if revision matches |
| `get_revision` | Fetch entry at a specific revision (incl. tombstones) |
| `batch_get` / `batch_put` | Bulk operations |
| `set_schema` / `get_schema` / `delete_schema` | JSON schema validation |

## Authentication & instance

```rust
// Authenticate against a server with JETCACHE_AUTH_TOKEN set:
let cache = JetCache::new(client).with_auth("my-secret-token");

// Connect to a non-default instance (must match JETCACHE_INSTANCE on the server):
let cache = JetCache::new(client).with_instance("lid");

// Both together:
let cache = JetCache::new(client)
    .with_auth("my-secret-token")
    .with_instance("lid");
```

The instance name (default `"lid"`, fallback `"ldb"`) matches the server's `JETCACHE_INSTANCE` configuration. This drives all NATS subject prefixes for messaging and change events. If the server is configured with a separate `JETCACHE_DATA_INSTANCE` for storage, the client still only needs to know the messaging `JETCACHE_INSTANCE`. Each deployment on the same cluster remains isolated by these prefixes.

## Session consistency tokens

`JetCache` automatically tracks per-table session watermarks from server responses and sends `consistency.min_revision` on later table-scoped reads.

- No app-side token plumbing is required when requests reuse the same `JetCache` instance.
- This provides session-level read-your-write behavior across queue-grouped replicas.
- For stateless HTTP tiers, forward the returned session state between hops if you create a fresh client per request.

## Building

```sh
cargo build --target wasm32-wasip2
```

## License

Apache-2.0
