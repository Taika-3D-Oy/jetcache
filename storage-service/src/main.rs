//! lattice-db storage service — main entry point (wasip3).
//!
//! Connects to NATS, subscribes to `ldb.>`, and dispatches incoming
//! requests to the handler. All state lives in-memory (backed by NATS KV).

mod handler;
mod log;
mod state;
mod store;
mod tcp_server;
mod tests;
pub mod vault;

use nats_wasi::client::{Client, ConnectConfig};
use nats_wasi::jetstream::JetStream;
use nats_wasi::service::{EndpointConfig, Service, ServiceConfig};
use std::rc::Rc;

// ── wasi:cli/run Export ───────────────────────────────────────────
struct Component;
wasip3::cli::command::export!(Component);

impl wasip3::exports::cli::run::Guest for Component {
    async fn run() -> Result<(), ()> {
        loop {
            if let Err(e) = run_service().await {
                eprintln!("storage-service error: {e} — restarting in 2s");
                wasip3::clocks::monotonic_clock::wait_for(nats_wasi::client::secs(2)).await;
            }
        }
    }
}



fn get_env_opt(keys: &[&str]) -> Option<String> {
    for k in keys {
        if let Ok(v) = std::env::var(k) {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn build_connect_config(address: String, name: &str, use_tls: bool, is_data: bool) -> ConnectConfig {
    let user = if is_data {
        get_env_opt(&["NATS_DATA_USER", "NATS_USER"])
    } else {
        get_env_opt(&["NATS_MSG_USER", "NATS_USER"])
    };

    let pass = if is_data {
        get_env_opt(&["NATS_DATA_PASSWORD", "NATS_PASSWORD", "NATS_DATA_PASS", "NATS_PASS"])
    } else {
        get_env_opt(&["NATS_MSG_PASSWORD", "NATS_PASSWORD", "NATS_MSG_PASS", "NATS_PASS"])
    };

    let auth_token = if is_data {
        get_env_opt(&["NATS_DATA_AUTH_TOKEN", "NATS_AUTH_TOKEN", "NATS_DATA_TOKEN", "NATS_TOKEN"])
    } else {
        get_env_opt(&["NATS_MSG_AUTH_TOKEN", "NATS_AUTH_TOKEN", "NATS_MSG_TOKEN", "NATS_TOKEN"])
    };

    let jwt = if is_data {
        get_env_opt(&["NATS_DATA_JWT", "NATS_JWT"])
    } else {
        get_env_opt(&["NATS_MSG_JWT", "NATS_JWT"])
    };

    ConnectConfig {
        address,
        name: Some(name.to_string()),
        user,
        pass,
        auth_token,
        jwt,
        tls: use_tls,
        ..Default::default()
    }
}


async fn run_service() -> Result<(), Box<dyn std::error::Error>> {
    // Determine which transport modes are enabled.
    let nats_url = get_env_opt(&["JETCACHE_NATS_URL", "CACHE_NATS_URL", "NATS_URL"])
        .or_else(|| std::env::args().nth(1));
    let nats_data_url = get_env_opt(&["JETCACHE_DATA_URL", "CACHE_DATA_URL", "NATS_DATA_URL"]);
    let tcp_port = Some(
        get_env_opt(&["JETCACHE_TCP_PORT", "CACHE_TCP_PORT", "LDB_TCP_PORT", "JETCACHE_PORT", "CACHE_PORT", "TCP_PORT"])
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(4080),
    );

    // At least one of NATS messaging or TCP must be enabled.
    if nats_url.is_none() && tcp_port.is_none() {
        return Err("at least one of NATS_URL or JETCACHE_TCP_PORT/CACHE_TCP_PORT/LDB_TCP_PORT must be set".into());
    }

    // Data connection (JetStream KV) — required for persistence.
    // Uses NATS_DATA_URL if set, otherwise falls back to NATS_URL.
    let nats_data_addr = nats_data_url
        .or_else(|| nats_url.clone())
        .ok_or("NATS_DATA_URL (or NATS_URL) is required for JetStream KV persistence")?;

    // TLS: enabled if NATS_TLS=1 is set.
    // Note: wasip3 uses host-side TLS via wasi:tls — no in-wasm crypto.
    // Custom CA certs are not yet supported by wasi:tls hosts.
    let use_tls = get_env_opt(&["JETCACHE_TLS", "CACHE_TLS", "NATS_TLS"]).map_or(false, |v| v == "1" || v == "true");

    if use_tls {
        log_info!("TLS enabled (host-side wasi:tls)");
    }

    // Connect to NATS for data (always needed for JetStream KV).
    eprintln!("jetcache: connecting to NATS (data) at {nats_data_addr}");
    let data_client = Client::connect(build_connect_config(
        nats_data_addr.to_string(),
        "jetcache-data",
        use_tls,
        true,
    ))
    .await?;

    // Connect to NATS for messaging (req/reply) — only if NATS_URL is set.
    let msg_client = if let Some(ref nats_msg_addr) = nats_url {
        if *nats_msg_addr == nats_data_addr {
            eprintln!("jetcache: NATS messaging using same connection as data");
            Some(data_client.clone())
        } else {
            eprintln!("jetcache: connecting to NATS (messaging) at {nats_msg_addr}");
            Some(
                Client::connect(build_connect_config(
                    nats_msg_addr.to_string(),
                    "jetcache-msg",
                    use_tls,
                    false,
                ))
                .await?,
            )
        }
    } else {
        eprintln!("jetcache: NATS messaging disabled (no NATS_URL)");
        None
    };

    eprintln!(
        "jetcache: connected to data={}",
        data_client.server_info().server_name,
    );

    // Instance names: drive NATS subject and KV bucket prefixes.
    let instance = {
        let raw = get_env_opt(&["JETCACHE_INSTANCE", "CACHE_INSTANCE", "LDB_INSTANCE"]).unwrap_or_else(|| "lid".to_string());
        validate_instance_name("JETCACHE_INSTANCE", &raw)?;
        eprintln!("jetcache: instance (messaging) = {raw}");
        raw
    };
    let data_instance = {
        let raw = get_env_opt(&["JETCACHE_DATA_INSTANCE", "CACHE_DATA_INSTANCE", "LDB_DATA_INSTANCE"]).unwrap_or_else(|| instance.clone());
        validate_instance_name("JETCACHE_DATA_INSTANCE", &raw)?;
        eprintln!("jetcache: instance (data) = {raw}");
        raw
    };

    let shared_state = state::new_shared_state();
    let shared_store = store::new_shared_store(data_client.clone(), data_instance.clone());
    let js = JetStream::new(data_client.clone());

    // Auth config.
    let auth_token = get_env_opt(&["JETCACHE_AUTH_TOKEN", "CACHE_AUTH_TOKEN", "LDB_AUTH_TOKEN"]);
    if auth_token.is_some() {
        eprintln!("jetcache: auth token required (_auth field)");
    }

    // Data epoch: a random identifier stored in `_meta` KV. Each replica
    // writes a fresh epoch on startup; a KV watcher ensures all replicas
    // converge to the latest epoch. After a data restore (or wipe) the next
    // startup naturally rotates the epoch, invalidating stale cookies.
    let meta_kv = store::get_or_create_kv(&shared_store, "_meta").await
        .map_err(|e| format!("meta KV setup: {e}"))?;
    {
        let bytes = wasip3::random::random::get_random_bytes(8);
        let epoch: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        let _ = meta_kv.put("epoch", epoch.as_bytes()).await;
        eprintln!("jetcache: data epoch (written) = {epoch}");
        handler::set_data_epoch(epoch);
    }
    // Watch `_meta` epoch key so all replicas converge when any peer starts.
    {
        let epoch_kv = meta_kv.clone();
        wasip3::spawn(async move {
            let mut since = 0u64;
            loop {
                let watcher_res = if since == 0 {
                    epoch_kv.watch_all().await
                } else {
                    epoch_kv.watch_all_from_revision(since).await
                };
                let watcher = match watcher_res {
                    Ok(w) => w,
                    Err(e) => {
                        eprintln!("lattice-db: _meta watcher setup failed: {e} — retrying");
                        wasip3::clocks::monotonic_clock::wait_for(nats_wasi::client::secs(5))
                            .await;
                        continue;
                    }
                };
                loop {
                    let entry = match watcher.next().await {
                        Ok(e) => e,
                        Err(e) => {
                            eprintln!(
                                "lattice-db: _meta watcher disconnected: {e} — reconnecting"
                            );
                            break;
                        }
                    };
                    since = entry.revision;
                    if entry.key == "epoch" {
                        if let nats_wasi::kv::Operation::Put = entry.operation {
                            let new_epoch = String::from_utf8_lossy(&entry.value).to_string();
                            eprintln!("lattice-db: data epoch updated = {new_epoch}");
                            handler::set_data_epoch(new_epoch);
                        }
                    }
                }
            }
        });
    }

    let config: handler::SharedConfig = Rc::new(handler::Config {
        auth_token,
        instance: instance.clone(),
        data_instance: data_instance.clone(),
    });

    // Start local TCP server for fast loopback requests (e.g. from oidc-gateway).
    if tcp_port.is_some() {
        eprintln!("lattice-db: starting TCP listener");
        tcp_server::start(
            data_client.clone(),
            js.clone(),
            config.clone(),
            shared_state.clone(),
            shared_store.clone(),
        );
    }


    // Load persisted schemas from KV.
    {
        let schema_kv = store::get_or_create_kv(&shared_store, "_schemas").await;
        if let Ok(kv) = schema_kv {
            let mut last_seq = 0u64;
            if let Ok(status) = kv.status().await {
                last_seq = status.last_seq;
            }
            if let Ok(entries) = kv.load_all().await {
                let mut s = shared_state.borrow_mut();
                for entry in &entries {
                    if let Ok(schema) = serde_json::from_slice::<serde_json::Value>(&entry.value) {
                        let enc = schema
                            .get("encrypted")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                        let ts = s.table(&entry.key);
                        ts.schema = Some(schema);
                        ts.encrypted = enc;
                        eprintln!(
                            "jetcache: loaded schema for table {} (encrypted={enc})",
                            entry.key
                        );
                    }
                }
            }
            // Spawn schema watcher for cross-replica sync. Reconnects
            // automatically on disconnect so a NATS hiccup doesn't silently
            // stop schema propagation across replicas.
            let schema_state = shared_state.clone();
            let schema_kv_handle = kv.clone();
            wasip3::spawn(async move {
                let mut since = last_seq;
                loop {
                    let watcher_res = if since == 0 {
                        schema_kv_handle.watch_all().await
                    } else {
                        schema_kv_handle.watch_all_from_revision(since).await
                    };
                    let watcher = match watcher_res {
                        Ok(w) => w,
                        Err(e) => {
                            eprintln!("jetcache: schema watcher setup failed: {e} — retrying");
                            wasip3::clocks::monotonic_clock::wait_for(nats_wasi::client::secs(5))
                                .await;
                            continue;
                        }
                    };
                    eprintln!("jetcache: schema watcher started (after seq {since})");
                    loop {
                        let entry = match watcher.next().await {
                            Ok(e) => e,
                            Err(e) => {
                                eprintln!(
                                    "jetcache: schema watcher disconnected: {e} — reconnecting"
                                );
                                break;
                            }
                        };
                        since = entry.revision;
                        let table_name = entry.key.clone();
                        match entry.operation {
                            nats_wasi::kv::Operation::Put => {
                                if let Ok(schema) =
                                    serde_json::from_slice::<serde_json::Value>(&entry.value)
                                {
                                    let enc = schema
                                        .get("encrypted")
                                        .and_then(|v| v.as_bool())
                                        .unwrap_or(false);
                                    let mut s = schema_state.borrow_mut();
                                    let ts = s.table(&table_name);
                                    ts.schema = Some(schema);
                                    ts.encrypted = enc;
                                    eprintln!(
                                        "jetcache: schema updated for {table_name} (rev {}, encrypted={enc})",
                                        entry.revision
                                    );
                                }
                            }
                            _ => {
                                let mut s = schema_state.borrow_mut();
                                let ts = s.table(&table_name);
                                ts.schema = None;
                                ts.encrypted = false;
                                eprintln!("jetcache: schema removed for {table_name}");
                            }
                        }
                    }
                }
            });
        }
    }

    // Subscribe to all jetcache operations as an ADR-32 Microservice.
    if let Some(ref client) = msg_client {
        let queue_group = format!("{instance}-workers");
        let service_config = ServiceConfig::new(instance.clone(), env!("CARGO_PKG_VERSION"))
            .description("In-memory read-through cache backed by NATS JetStream KV")
            .queue_group(&queue_group)
            .metadata("instance", &instance)
            .metadata("data_instance", &data_instance);

        let service = Service::add(client.clone(), service_config).await?;
        let group = service.group(&instance);

        let endpoints = [
            "get",
            "put",
            "delete",
            "cas",
            "cas_delete",
            "purge",
            "get_revision",
            "create",
            "exists",
            "keys",
            "prefix",
            "batch.get",
            "batch.put",
            "schema.set",
            "schema.get",
            "schema.delete",
        ];

        for op in endpoints {
            let ep_sub = group.add_endpoint(EndpointConfig::new(op)).await?;
            let msg_client = client.clone();
            let js = js.clone();
            let cfg = config.clone();
            let state = shared_state.clone();
            let store = shared_store.clone();
            let op_name = op.to_string();

            wasip3::spawn(async move {
                while let Ok(req) = ep_sub.next().await {
                    let msg_client = msg_client.clone();
                    let js = js.clone();
                    let cfg = cfg.clone();
                    let state = state.clone();
                    let store = store.clone();
                    let op_str = op_name.clone();

                    wasip3::spawn(async move {
                        handler::handle_service_request(
                            &msg_client,
                            &js,
                            &cfg,
                            &state,
                            &store,
                            req,
                            &op_str,
                        )
                        .await;
                    });
                }
            });
        }

        eprintln!(
            "jetcache: ADR-32 microservice '{}' running (endpoints: {}, queue group: {queue_group})",
            service.name(),
            endpoints.len()
        );

        // Keep the main service loop alive.
        loop {
            wasip3::clocks::monotonic_clock::wait_for(nats_wasi::client::secs(3600)).await;
        }
    } else {
        // TCP-only mode: keep the process alive.
        eprintln!("lattice-db: running in TCP-only mode (no NATS messaging)");
        loop {
            wasip3::clocks::monotonic_clock::wait_for(nats_wasi::client::secs(3600)).await;
        }
    }
}

fn validate_instance_name(var: &str, name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > 64 {
        return Err(format!("{var} must be 1–64 characters"));
    }
    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
    {
        return Err(format!(
            "{var} may only contain alphanumeric characters, _ and -"
        ));
    }
    Ok(())
}
