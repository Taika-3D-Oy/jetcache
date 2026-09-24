//! Request dispatch and JSON protocol.
//!
//! ## NATS subject scheme
//!
//! ```text
//! ldb.get             {table, key}                     → {key, value, revision}
//! ldb.put             {table, key, value}              → {revision}
//! ldb.delete          {table, key}                     → {}
//! ldb.cas             {table, key, value, revision}    → {revision}
//! ldb.create          {table, key, value}              → {revision}
//! ldb.exists          {table, key}                     → {exists}
//! ldb.keys            {table, cursor?}                 → {keys, cursor?}
//! ldb.scan            {table, filters, order_by?, limit?, offset?} → {rows, total_count}
//! ldb.count           {table, filters}                 → {count}
//! ldb.index.create    {table, field}                   → {}
//! ldb.index.drop      {table, field}                   → {}
//! ldb.index.list      {table}                          → {indexes}
//! ldb.cas_delete       {table, key, revision}           → {}
//! ldb.purge            {table, key, revision?, ttl_seconds?} → {}
//! ldb.get_revision     {table, key, revision}           → {key, value, revision, operation}
//! ldb.txn             {ops: [{op, table, key, value?}]} → {ok, results}
//! ldb.schedule_put    {table, key, value, at}           → {}
//! ```
//!
//! All values in request/response are base64-encoded when binary.
//! Errors are returned as `{"error": "description"}`.

use base64::Engine;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::OnceLock;

use nats_wasi::client::{secs, Client, Message};
use nats_wasi::jetstream::JetStream;

use crate::state::{self, SharedState};
use crate::store::SharedStore;

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

// ── Request limits (S-04) ─────────────────────────────────────────────

/// Maximum decoded byte size of a value. Rejects oversized writes before they
/// reach NATS KV, preventing single-request memory exhaustion.
pub(crate) const MAX_VALUE_BYTES: usize = 1 * 1024 * 1024; // 1 MiB

/// Maximum byte length of a key.
const MAX_KEY_LEN: usize = 256;

/// Maximum byte length of a table name (after partition prefix is applied).
const MAX_TABLE_LEN: usize = 128;

/// Maximum number of entries in a batch.put request.
const MAX_BATCH_SIZE: usize = 256;

/// Max number of watcher polling intervals before forcing a table reload.
const DEFAULT_CONSISTENCY_WATCHER_WAIT_STEPS: u32 = 2;
/// Duration of each watcher polling interval.
const DEFAULT_CONSISTENCY_WATCHER_WAIT_STEP_SECS: u64 = 1;

// ── Master key (loaded once at startup) ─────────────────────────────────────

/// Global master key for AES-256-GCM envelope encryption.
/// Populated on first encrypted write or read via `master_key()`.
static MASTER_KEY: OnceLock<[u8; 32]> = OnceLock::new();

/// Return the master key, loading it on first call.
pub(crate) fn master_key() -> &'static [u8; 32] {
    MASTER_KEY.get_or_init(crate::vault::load_master_key)
}

/// Encrypt `plaintext` if the table is marked encrypted; otherwise return a clone.
pub(crate) fn maybe_encrypt(table: &str, key: &str, plaintext: &[u8], encrypted: bool) -> Vec<u8> {
    if encrypted {
        crate::vault::encrypt(master_key(), table, key, plaintext)
    } else {
        plaintext.to_vec()
    }
}

/// Decrypt `bytes` if the table is marked encrypted; otherwise return a clone.
pub(crate) fn maybe_decrypt(
    table: &str,
    key: &str,
    bytes: Vec<u8>,
    encrypted: bool,
) -> Result<Vec<u8>, String> {
    if encrypted {
        crate::vault::decrypt(master_key(), table, key, &bytes)
    } else {
        Ok(bytes)
    }
}

fn env_u32(names: &[&str], default: u32, min: u32, max: u32) -> u32 {
    for name in names {
        if let Ok(v) = std::env::var(name) {
            if let Ok(parsed) = v.trim().parse::<u32>() {
                return parsed.clamp(min, max);
            }
        }
    }
    default
}

fn env_u64(names: &[&str], default: u64, min: u64, max: u64) -> u64 {
    for name in names {
        if let Ok(v) = std::env::var(name) {
            if let Ok(parsed) = v.trim().parse::<u64>() {
                return parsed.clamp(min, max);
            }
        }
    }
    default
}

fn consistency_watcher_wait_steps() -> u32 {
    static STEPS: OnceLock<u32> = OnceLock::new();
    *STEPS.get_or_init(|| {
        env_u32(
            &["JETCACHE_CONSISTENCY_WATCHER_WAIT_STEPS", "CACHE_CONSISTENCY_WATCHER_WAIT_STEPS", "LDB_CONSISTENCY_WATCHER_WAIT_STEPS"],
            DEFAULT_CONSISTENCY_WATCHER_WAIT_STEPS,
            0,
            60,
        )
    })
}

fn consistency_watcher_wait_step_secs() -> u64 {
    static SECS: OnceLock<u64> = OnceLock::new();
    *SECS.get_or_init(|| {
        env_u64(
            &["JETCACHE_CONSISTENCY_WATCHER_WAIT_STEP_SECS", "CACHE_CONSISTENCY_WATCHER_WAIT_STEP_SECS", "LDB_CONSISTENCY_WATCHER_WAIT_STEP_SECS"],
            DEFAULT_CONSISTENCY_WATCHER_WAIT_STEP_SECS,
            0,
            30,
        )
    })
}

// ── Config ────────────────────────────────────────────────────────────

pub struct Config {
    pub auth_token: Option<String>,
    /// NATS subject prefix for this instance (matches `LDB_INSTANCE`).
    pub instance: String,
    /// NATS KV bucket and WAL prefix (matches `LDB_DATA_INSTANCE`).
    #[allow(dead_code)]
    pub data_instance: String,
}

pub type SharedConfig = Rc<Config>;

// ── Request types ──────────────────────────────────────────────────

#[derive(Deserialize)]
struct ConsistencyReq {
    #[serde(default)]
    min_revision: Option<u64>,
}

#[derive(Serialize)]
struct SessionToken {
    revisions: HashMap<String, u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    epoch: Option<String>,
}

#[derive(Deserialize)]
struct KeyReq {
    table: String,
    key: String,
    #[serde(default)]
    consistency: Option<ConsistencyReq>,
}

#[derive(Deserialize)]
struct PutReq {
    table: String,
    key: String,
    value: String, // base64
    #[serde(default)]
    ttl_seconds: Option<u64>,
}

#[derive(Deserialize)]
struct CasReq {
    table: String,
    key: String,
    value: String, // base64
    revision: u64,
    #[serde(default)]
    ttl_seconds: Option<u64>,
}

#[derive(Deserialize)]
struct CasDeleteReq {
    table: String,
    key: String,
    revision: u64,
}

#[derive(Deserialize)]
struct PurgeReq {
    table: String,
    key: String,
    #[serde(default)]
    revision: Option<u64>,
    #[serde(default)]
    ttl_seconds: Option<u64>,
}

#[derive(Deserialize)]
struct GetRevisionReq {
    table: String,
    key: String,
    revision: u64,
    #[serde(default)]
    consistency: Option<ConsistencyReq>,
}

#[derive(Deserialize)]
struct TableReq {
    table: String,
}

fn deserialize_cursor<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let opt = Option::<serde_json::Value>::deserialize(deserializer)?;
    match opt {
        None => Ok(None),
        Some(serde_json::Value::Number(n)) => Ok(n.as_u64()),
        Some(serde_json::Value::String(s)) => {
            if s.is_empty() {
                Ok(None)
            } else {
                s.parse::<u64>().map(Some).map_err(D::Error::custom)
            }
        }
        _ => Err(D::Error::custom("invalid cursor format")),
    }
}

#[derive(Deserialize)]
struct KeysReq {
    table: String,
    #[serde(default, deserialize_with = "deserialize_cursor")]
    cursor: Option<u64>,
    #[serde(default)]
    consistency: Option<ConsistencyReq>,
}

#[derive(Deserialize)]
struct PrefixReq {
    table: String,
    prefix: String,
    #[serde(default)]
    consistency: Option<ConsistencyReq>,
}

#[derive(Deserialize)]
struct BatchGetReq {
    table: String,
    keys: Vec<String>,
    #[serde(default)]
    consistency: Option<ConsistencyReq>,
}

#[derive(Deserialize)]
struct BatchPutEntry {
    key: String,
    value: String, // base64
    #[serde(default)]
    ttl_seconds: Option<u64>,
}

#[derive(Deserialize)]
struct BatchPutReq {
    table: String,
    entries: Vec<BatchPutEntry>,
}

#[derive(Deserialize)]
struct SchemaSetReq {
    table: String,
    schema: serde_json::Value,
}

// ── Response types ─────────────────────────────────────────────────

#[derive(Serialize)]
struct RowResp {
    key: String,
    value: String, // base64
    revision: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    session: Option<SessionToken>,
}

#[derive(Serialize)]
struct RevisionResp {
    revision: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    session: Option<SessionToken>,
}

#[derive(Serialize)]
struct ExistsResp {
    exists: bool,
}

#[derive(Serialize)]
struct RevisionEntryResp {
    key: String,
    value: String, // base64
    revision: u64,
    operation: String, // "put", "delete", "purge"
}

#[derive(Serialize)]
struct KeysResp {
    keys: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
}

#[derive(Serialize)]
struct PrefixRow {
    key: String,
    value: String, // base64
    revision: u64,
}

#[derive(Serialize)]
struct PrefixResp {
    rows: Vec<PrefixRow>,
}

#[allow(dead_code)]
#[derive(Serialize)]
struct ErrorResp {
    error: String,
}

#[derive(Serialize)]
struct EmptyResp {}

#[derive(Serialize)]
struct BatchGetResult {
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    revision: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct BatchGetResp {
    results: Vec<BatchGetResult>,
}

#[derive(Serialize)]
struct BatchPutResult {
    key: String,
    revision: u64,
}

#[derive(Serialize)]
struct BatchPutResp {
    results: Vec<BatchPutResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    session: Option<SessionToken>,
}

#[derive(Serialize)]
struct SchemaResp {
    schema: serde_json::Value,
}

#[derive(Serialize)]
struct ChangeEvent {
    op: String,
    table: String,
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    revision: Option<u64>,
}

// ── Dispatch ───────────────────────────────────────────────────────

/// Unified operation dispatcher.
pub async fn dispatch_operation(
    client: &Client,
    _js: &JetStream,
    config: &SharedConfig,
    state: &SharedState,
    store: &SharedStore,
    op: &str,
    payload: &[u8],
) -> Result<Vec<u8>, (u16, String)> {
    // Auth check.
    if let Some(ref token) = config.auth_token {
        if let Err(e) = check_auth(payload, token) {
            return Err((401, e));
        }
    }

    // S-01: reject reserved (_-prefixed) table names.
    if let Err(e) = check_no_reserved_tables(op, payload) {
        return Err((400, e));
    }

    let instance = config.instance.as_str();
    let result = match op {
        "get" => handle_get(state, store, payload).await,
        "put" => handle_put(client, state, store, payload, instance).await,
        "delete" => handle_delete(client, state, store, payload, instance).await,
        "cas" => handle_cas(client, state, store, payload, instance).await,
        "cas_delete" => handle_cas_delete(client, state, store, payload, instance).await,
        "purge" => handle_purge(client, state, store, payload, instance).await,
        "get_revision" => handle_get_revision(state, store, payload).await,
        "create" => handle_create(client, state, store, payload, instance).await,
        "exists" => handle_exists(state, store, payload).await,
        "keys" => handle_keys(state, store, payload).await,
        "prefix" => handle_prefix(state, store, payload).await,
        "batch.get" | "batch_get" => handle_batch_get(state, store, payload).await,
        "batch.put" | "batch_put" => handle_batch_put(client, state, store, payload, instance).await,
        "schema.set" | "schema_set" => handle_schema_set(state, store, payload).await,
        "schema.get" | "schema_get" => handle_schema_get(state, payload),
        "schema.delete" | "schema_delete" => handle_schema_delete(state, store, payload).await,
        _ => Err(format!("unknown operation: {op}")),
    };

    result.map_err(|e| (400, e))
}

/// Handle a NATS ADR-32 Microservice endpoint request.
pub async fn handle_service_request(
    client: &Client,
    js: &JetStream,
    config: &SharedConfig,
    state: &SharedState,
    store: &SharedStore,
    req: nats_wasi::service::ServiceRequest,
    op: &str,
) {
    let result = dispatch_operation(client, js, config, state, store, op, req.payload()).await;
    match result {
        Ok(json_bytes) => {
            let _ = req.respond(&json_bytes);
        }
        Err((code, err_msg)) => {
            let _ = req.respond_error(code, &err_msg);
        }
    }
}

/// Handle an incoming raw request message (fallback/legacy).
#[allow(dead_code)]
pub async fn handle(
    client: &Client,
    js: &JetStream,
    config: &SharedConfig,
    state: &SharedState,
    store: &SharedStore,
    msg: Message,
) {
    let Some(reply_to) = msg.reply_to.as_deref() else {
        return;
    };

    let prefix = format!("{instance}.", instance = config.instance);
    let op = msg.subject.strip_prefix(&prefix).unwrap_or(&msg.subject);
    let result = dispatch_operation(client, js, config, state, store, op, &msg.payload).await;

    let resp_bytes = match result {
        Ok(json) => json,
        Err((_code, e)) => serde_json::to_vec(&ErrorResp { error: e }).unwrap_or_default(),
    };

    let _ = client.publish(reply_to, &resp_bytes);
}

// ── Ensure table is loaded ─────────────────────────────────────────

async fn ensure_loaded(
    table: &str,
    state: &SharedState,
    store: &SharedStore,
) -> Result<(), String> {
    // Two independent invariants must hold after this returns:
    //   1. The table's data is loaded into the in-memory cache.
    //   2. A background watcher is running so future writes from peer
    //      replicas are reflected in this replica's cache.
    // They are tracked separately because txn::recover() can pre-load tables
    // before the dispatcher ever runs ensure_loaded — in that case `loaded`
    // is already true but no watcher exists yet.
    //
    // The `loading` flag prevents concurrent tasks from racing through the
    // async load path and spawning duplicate watchers.
    let (needs_load, needs_watcher, is_loading) = {
        let mut s = state.borrow_mut();
        let t = s.table(table);
        let already_loaded = t.loaded;
        let already_watching = t.watching;
        let currently_loading = t.loading;
        let needs_load = !already_loaded && !currently_loading;
        let needs_watcher = !already_watching;
        if needs_load {
            t.loading = true;
        }
        (needs_load, needs_watcher, currently_loading)
    };

    if !needs_load && !needs_watcher {
        if is_loading {
            // Another task is currently loading this table — wait for it.
            loop {
                let done = {
                    let s = state.borrow();
                    s.tables.get(table).map_or(true, |t| t.loaded || !t.loading)
                };
                if done {
                    break;
                }
                wasip3::clocks::monotonic_clock::wait_for(1_000_000).await; // 1ms yield
            }
        }
        return Ok(());
    }

    // If we only need a watcher (table already loaded by txn recovery), we
    // still need a KV handle but skip the snapshot load.
    if !needs_load && needs_watcher {
        let kv = crate::store::get_or_create_kv(store, table)
            .await
            .map_err(|e| format!("load table: {e}"))?;
        let max_rev = state
            .borrow()
            .tables
            .get(table)
            .map(|t| t.applied_revision)
            .unwrap_or(0);
        state.borrow_mut().table(table).watching = true;
        let watch_state = state.clone();
        let table_name = table.to_string();
        wasip3::spawn(async move {
            run_table_watcher(kv, &table_name, &watch_state, max_rev).await;
        });
        return Ok(());
    }

    // Also need to wait if another task is loading (and we need a watcher).
    if is_loading {
        loop {
            let done = {
                let s = state.borrow();
                s.tables.get(table).map_or(true, |t| t.loaded || !t.loading)
            };
            if done {
                break;
            }
            wasip3::clocks::monotonic_clock::wait_for(1_000_000).await; // 1ms yield
        }
        return Ok(());
    }

    let kv = match crate::store::get_or_create_kv(store, table).await {
        Ok(kv) => kv,
        Err(e) => {
            // Release loading flag on failure.
            state.borrow_mut().table(table).loading = false;
            return Err(format!("load table: {e}"));
        }
    };

    let snapshot = load_table_snapshot(&kv).await;
    let (entries, snapshot_last_seq) = match snapshot {
        Ok(v) => v,
        Err(e) => {
            state.borrow_mut().table(table).loading = false;
            return Err(e);
        }
    };
    let mut s = state.borrow_mut();
    let ts = s.table(table);
    let encrypted = ts.encrypted;
    for (key, value, revision) in entries {
        let plaintext = match maybe_decrypt(table, &key, value, encrypted) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("lattice-db: decrypt failed loading {table}:{key}: {e}");
                continue;
            }
        };
        ts.upsert(&key, plaintext, revision);
    }
    ts.loaded = true;
    ts.loading = false;
    ts.note_applied_revision(snapshot_last_seq);
    let max_rev = snapshot_last_seq;
    drop(s);

    if needs_watcher {
        state.borrow_mut().table(table).watching = true;
        let watch_state = state.clone();
        let table_name = table.to_string();
        wasip3::spawn(async move {
            run_table_watcher(kv, &table_name, &watch_state, max_rev).await;
        });
    }
    Ok(())
}

async fn load_table_snapshot(
    kv: &nats_wasi::kv::KeyValue,
) -> Result<(Vec<(String, Vec<u8>, u64)>, u64), String> {
    // Use status() to get the authoritative last_seq for consistency watermarks.
    let status = kv.status().await.map_err(|e| format!("load table: {e}"))?;
    let last_seq = status.last_seq;

    if status.values == 0 {
        return Ok((Vec::new(), last_seq));
    }

    // Use the consumer-based load_all (nats-wasip3 ≥ 0.8.2) which fetches in
    // batches using DeliverPolicy::LastPerSubject — O(keys) not O(stream_seq).
    let raw_entries = kv
        .load_all()
        .await
        .map_err(|e| format!("load table: {e}"))?;

    let entries = raw_entries
        .into_iter()
        .map(|e| (e.key, e.value, e.revision))
        .collect();

    Ok((entries, last_seq))
}

async fn reload_table_from_kv(
    table: &str,
    state: &SharedState,
    store: &SharedStore,
) -> Result<u64, String> {
    let kv = crate::store::get_or_create_kv(store, table)
        .await
        .map_err(|e| format!("load table: {e}"))?;
    let (entries, snapshot_last_seq) = load_table_snapshot(&kv).await?;

    let mut s = state.borrow_mut();
    let ts = s.table(table);
    ts.data.clear();
    let encrypted = ts.encrypted;
    for (key, value, revision) in entries {
        let plaintext = match maybe_decrypt(table, &key, value, encrypted) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("lattice-db: decrypt failed reloading {table}:{key}: {e}");
                continue;
            }
        };
        ts.upsert(&key, plaintext, revision);
    }
    ts.loaded = true;
    ts.note_applied_revision(snapshot_last_seq);
    Ok(snapshot_last_seq)
}

async fn ensure_min_revision(
    table: &str,
    consistency: &Option<ConsistencyReq>,
    state: &SharedState,
    store: &SharedStore,
) -> Result<(), String> {
    ensure_loaded(table, state, store).await?;

    let min_revision = consistency
        .as_ref()
        .and_then(|c| c.min_revision)
        .unwrap_or(0);
    if min_revision == 0 {
        return Ok(());
    }

    let already_fresh = {
        let s = state.borrow();
        s.tables
            .get(table)
            .map_or(false, |t| t.applied_revision >= min_revision)
    };
    if already_fresh {
        return Ok(());
    }

    for _ in 0..consistency_watcher_wait_steps() {
        let (applied_revision, watching) = {
            let s = state.borrow();
            let t = s.tables.get(table);
            (
                t.map_or(0, |t| t.applied_revision),
                t.map_or(false, |t| t.watching),
            )
        };

        if applied_revision >= min_revision {
            return Ok(());
        }
        if !watching {
            break;
        }

        wasip3::clocks::monotonic_clock::wait_for(secs(consistency_watcher_wait_step_secs())).await;
    }

    let caught_up_after_wait = {
        let s = state.borrow();
        s.tables
            .get(table)
            .map_or(false, |t| t.applied_revision >= min_revision)
    };
    if caught_up_after_wait {
        return Ok(());
    }

    reload_table_from_kv(table, state, store).await?;

    let caught_up = {
        let s = state.borrow();
        s.tables
            .get(table)
            .map_or(false, |t| t.applied_revision >= min_revision)
    };
    if caught_up {
        Ok(())
    } else {
        Err(format!(
            "stale replica: table '{}' not caught up to required revision {}",
            table, min_revision
        ))
    }
}

// ── Data epoch ─────────────────────────────────────────────────────────────

thread_local! {
    /// Data epoch — updated at startup and by the `_meta` watcher whenever
    /// any replica writes a new epoch (e.g. after a restore or fresh start).
    static DATA_EPOCH: RefCell<String> = RefCell::new(String::new());
}

/// Update the in-memory epoch. Called from main.rs on startup and from the
/// `_meta` KV watcher whenever the epoch changes.
pub(crate) fn set_data_epoch(epoch: String) {
    DATA_EPOCH.with(|e| *e.borrow_mut() = epoch);
}

fn data_epoch() -> Option<String> {
    DATA_EPOCH.with(|e| {
        let s = e.borrow();
        if s.is_empty() { None } else { Some(s.clone()) }
    })
}

fn session_for_table(table: &str, revision: u64) -> Option<SessionToken> {
    if revision == 0 {
        return None;
    }
    let mut revisions = HashMap::new();
    revisions.insert(table.to_string(), revision);
    Some(SessionToken {
        revisions,
        epoch: data_epoch(),
    })
}

/// Background loop that watches a KV bucket for changes from other replicas
/// and updates the local in-memory cache + indexes.
async fn run_table_watcher(
    kv: nats_wasi::kv::KeyValue,
    table: &str,
    state: &SharedState,
    start_after: u64,
) {
    let watcher_res = if start_after == 0 {
        kv.watch_all().await
    } else {
        kv.watch_all_from_revision(start_after).await
    };
    let watcher = match watcher_res {
        Ok(w) => w,
        Err(e) => {
            eprintln!("lattice-db: watcher setup failed for {table}: {e}");
            state.borrow_mut().table(table).watching = false;
            return;
        }
    };
    eprintln!("lattice-db: watcher started for table {table} (after seq {start_after})");
    loop {
        let entry = match watcher.next().await {
            Ok(e) => e,
            Err(e) => {
                eprintln!("lattice-db: watcher error for {table}: {e}");
                break;
            }
        };

        let mut s = state.borrow_mut();
        let ts = s.table(table);

        match entry.operation {
            nats_wasi::kv::Operation::Put => {
                // Only apply if this is a newer revision than what we have.
                let dominated = ts
                    .data
                    .get(&entry.key)
                    .map_or(false, |r| r.revision >= entry.revision);
                if !dominated {
                    let plaintext = if ts.encrypted {
                        match crate::vault::decrypt(master_key(), table, &entry.key, &entry.value) {
                            Ok(p) => p,
                            Err(e) => {
                                eprintln!(
                                    "lattice-db: watcher decrypt failed {table}:{}: {e}",
                                    entry.key
                                );
                                ts.note_applied_revision(entry.revision);
                                continue;
                            }
                        }
                    } else {
                        entry.value
                    };
                    ts.upsert(&entry.key, plaintext, entry.revision);
                }
                ts.note_applied_revision(entry.revision);
            }
            nats_wasi::kv::Operation::Delete | nats_wasi::kv::Operation::Purge => {
                // Remove from cache. Check revision to avoid deleting a newer write.
                let dominated = ts
                    .data
                    .get(&entry.key)
                    .map_or(false, |r| r.revision > entry.revision);
                if !dominated {
                    ts.remove(&entry.key);
                }
                ts.note_applied_revision(entry.revision);
            }
            // `Operation` is `#[non_exhaustive]` since nats-wasip3 0.7; ignore unknown ops.
            _ => {}
        }
    }
    // Watcher disconnected. Reset flags so the next request to this
    // table triggers a full reload from KV + fresh watcher. Without resetting
    // `loaded`, the cache silently diverges and never re-syncs.
    {
        let mut s = state.borrow_mut();
        let ts = s.table(table);
        ts.watching = false;
        ts.loaded = false;
        ts.loading = false;
        ts.applied_revision = 0;
    }
    eprintln!("lattice-db: watcher stopped for table {table} — will reload on next request");
}

// ── Operation handlers ─────────────────────────────────────────────

pub(crate) async fn handle_get(
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: KeyReq = parse_req(payload)?;
    ensure_min_revision(&req.table, &req.consistency, state, store).await?;

    let s = state.borrow();
    let table = s.tables.get(&req.table).ok_or("table not loaded")?;
    let row = table.data.get(&req.key).ok_or("not found")?;

    ok_json(&RowResp {
        key: req.key,
        value: B64.encode(&row.value),
        revision: row.revision,
        session: session_for_table(&req.table, row.revision),
    })
}

pub(crate) async fn handle_put(
    client: &Client,
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
    instance: &str,
) -> Result<Vec<u8>, String> {
    let req: PutReq = parse_req(payload)?;
    let value = B64.decode(&req.value).map_err(|e| format!("base64: {e}"))?;
    validate_write_bounds(&req.table, &req.key, &value)?;
    ensure_loaded(&req.table, state, store).await?;

    // Schema validation.
    {
        let s = state.borrow();
        if let Some(schema) = s.tables.get(&req.table).and_then(|t| t.schema.as_ref()) {
            state::validate_schema(&value, schema)?;
        }
    }

    let kv = crate::store::get_or_create_kv(store, &req.table)
        .await
        .map_err(|e| format!("{e}"))?;
    let encrypted = state.borrow().is_encrypted(&req.table);
    let store_bytes = maybe_encrypt(&req.table, &req.key, &value, encrypted);
    let revision = match req.ttl_seconds {
        Some(ttl) => kv
            .put_with_ttl(&req.key, &store_bytes, secs(ttl))
            .await
            .map_err(|e| format!("{e}"))?,
        None => kv
            .put(&req.key, &store_bytes)
            .await
            .map_err(|e| format!("{e}"))?,
    };

    state
        .borrow_mut()
        .table(&req.table)
        .upsert(&req.key, value, revision);

    publish_change(
        client,
        "put",
        &req.table,
        &req.key,
        if encrypted { None } else { Some(&req.value) },
        Some(revision),
        instance,
    );
    ok_json(&RevisionResp {
        revision,
        session: session_for_table(&req.table, revision),
    })
}

pub(crate) async fn handle_delete(
    client: &Client,
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
    instance: &str,
) -> Result<Vec<u8>, String> {
    let req: KeyReq = parse_req(payload)?;
    ensure_min_revision(&req.table, &req.consistency, state, store).await?;

    let kv = crate::store::get_or_create_kv(store, &req.table)
        .await
        .map_err(|e| format!("{e}"))?;
    kv.delete(&req.key).await.map_err(|e| format!("{e}"))?;

    // Update cache.
    state.borrow_mut().table(&req.table).remove(&req.key);

    publish_change(client, "delete", &req.table, &req.key, None, None, instance);
    ok_json(&EmptyResp {})
}

pub(crate) async fn handle_cas_delete(
    client: &Client,
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
    instance: &str,
) -> Result<Vec<u8>, String> {
    let req: CasDeleteReq = parse_req(payload)?;
    ensure_loaded(&req.table, state, store).await?;

    let kv = crate::store::get_or_create_kv(store, &req.table)
        .await
        .map_err(|e| format!("{e}"))?;
    // CAS delete — only tombstone if the current revision matches.
    match kv.delete_expect_revision(&req.key, req.revision).await {
        Ok(()) => {}
        Err(e) => {
            // On CAS failure, re-fetch the winning value so the client's next
            // retry sees the current revision immediately.
            if let Ok(Some(current)) = kv.get(&req.key).await {
                state.borrow_mut().table(&req.table).upsert(
                    &req.key,
                    current.value,
                    current.revision,
                );
            }
            return Err(format!("{e}"));
        }
    }

    state.borrow_mut().table(&req.table).remove(&req.key);

    publish_change(client, "delete", &req.table, &req.key, None, None, instance);
    ok_json(&EmptyResp {})
}

pub(crate) async fn handle_purge(
    client: &Client,
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
    instance: &str,
) -> Result<Vec<u8>, String> {
    let req: PurgeReq = parse_req(payload)?;
    ensure_loaded(&req.table, state, store).await?;

    let kv = crate::store::get_or_create_kv(store, &req.table)
        .await
        .map_err(|e| format!("{e}"))?;

    match (req.revision, req.ttl_seconds) {
        (Some(rev), Some(ttl)) => kv
            .purge_expect_revision_with_ttl(&req.key, rev, secs(ttl))
            .await
            .map_err(|e| format!("{e}"))?,
        (Some(rev), None) => kv
            .purge_expect_revision(&req.key, rev)
            .await
            .map_err(|e| format!("{e}"))?,
        (None, Some(ttl)) => kv
            .purge_with_ttl(&req.key, secs(ttl))
            .await
            .map_err(|e| format!("{e}"))?,
        (None, None) => kv.purge(&req.key).await.map_err(|e| format!("{e}"))?,
    };

    state.borrow_mut().table(&req.table).remove(&req.key);

    publish_change(client, "purge", &req.table, &req.key, None, None, instance);
    ok_json(&EmptyResp {})
}

pub(crate) async fn handle_get_revision(
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: GetRevisionReq = parse_req(payload)?;
    ensure_min_revision(&req.table, &req.consistency, state, store).await?;

    let kv = crate::store::get_or_create_kv(store, &req.table)
        .await
        .map_err(|e| format!("{e}"))?;

    let entry = kv
        .entry_for_revision(&req.key, req.revision)
        .await
        .map_err(|e| format!("{e}"))?
        .ok_or("not found")?;

    let operation = match entry.operation {
        nats_wasi::kv::Operation::Put => "put",
        nats_wasi::kv::Operation::Delete => "delete",
        nats_wasi::kv::Operation::Purge => "purge",
        _ => "unknown",
    };

    ok_json(&RevisionEntryResp {
        key: entry.key,
        value: B64.encode(&entry.value),
        revision: entry.revision,
        operation: operation.to_string(),
    })
}

pub(crate) async fn handle_cas(
    client: &Client,
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
    instance: &str,
) -> Result<Vec<u8>, String> {
    let req: CasReq = parse_req(payload)?;
    let value = B64.decode(&req.value).map_err(|e| format!("base64: {e}"))?;
    validate_write_bounds(&req.table, &req.key, &value)?;
    ensure_loaded(&req.table, state, store).await?;

    // Schema validation.
    {
        let s = state.borrow();
        if let Some(schema) = s.tables.get(&req.table).and_then(|t| t.schema.as_ref()) {
            state::validate_schema(&value, schema)?;
        }
    }

    let kv = crate::store::get_or_create_kv(store, &req.table)
        .await
        .map_err(|e| format!("{e}"))?;
    let encrypted = state.borrow().is_encrypted(&req.table);
    let store_bytes = maybe_encrypt(&req.table, &req.key, &value, encrypted);
    let result = match req.ttl_seconds {
        Some(ttl) => {
            kv.update_with_ttl(&req.key, &store_bytes, req.revision, secs(ttl))
                .await
        }
        None => kv.update(&req.key, &store_bytes, req.revision).await,
    };
    // On success, update the local cache. On failure, proactively re-fetch
    // the winning value from NATS so the client's next retry sees the current
    // revision immediately, without waiting for the KV watcher to deliver it.
    let revision = match result {
        Ok(r) => r,
        Err(e) => {
            if let Ok(Some(current)) = kv.get(&req.key).await {
                let plaintext = if encrypted {
                    match crate::vault::decrypt(master_key(), &req.table, &req.key, &current.value)
                    {
                        Ok(p) => p,
                        Err(e) => {
                            eprintln!(
                                "lattice-db: cas fallback decrypt failed {table}:{key}: {e}",
                                table = req.table,
                                key = req.key
                            );
                            return Err(format!("{e}"));
                        }
                    }
                } else {
                    current.value
                };
                state
                    .borrow_mut()
                    .table(&req.table)
                    .upsert(&req.key, plaintext, current.revision);
            }
            return Err(format!("{e}"));
        }
    };
    state
        .borrow_mut()
        .table(&req.table)
        .upsert(&req.key, value, revision);

    publish_change(
        client,
        "cas",
        &req.table,
        &req.key,
        if encrypted { None } else { Some(&req.value) },
        Some(revision),
        instance,
    );
    ok_json(&RevisionResp {
        revision,
        session: session_for_table(&req.table, revision),
    })
}

pub(crate) async fn handle_create(
    client: &Client,
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
    instance: &str,
) -> Result<Vec<u8>, String> {
    let req: PutReq = parse_req(payload)?;
    let value = B64.decode(&req.value).map_err(|e| format!("base64: {e}"))?;
    validate_write_bounds(&req.table, &req.key, &value)?;
    ensure_loaded(&req.table, state, store).await?;

    // Schema validation.
    {
        let s = state.borrow();
        if let Some(schema) = s.tables.get(&req.table).and_then(|t| t.schema.as_ref()) {
            state::validate_schema(&value, schema)?;
        }
    }

    let kv = crate::store::get_or_create_kv(store, &req.table)
        .await
        .map_err(|e| format!("{e}"))?;
    let encrypted = state.borrow().is_encrypted(&req.table);
    let store_bytes = maybe_encrypt(&req.table, &req.key, &value, encrypted);
    let revision = match req.ttl_seconds {
        Some(ttl) => kv
            .create_with_ttl(&req.key, &store_bytes, secs(ttl))
            .await
            .map_err(|e| format!("{e}"))?,
        None => kv
            .create(&req.key, &store_bytes)
            .await
            .map_err(|e| format!("{e}"))?,
    };

    state
        .borrow_mut()
        .table(&req.table)
        .upsert(&req.key, value, revision);

    publish_change(
        client,
        "create",
        &req.table,
        &req.key,
        if encrypted { None } else { Some(&req.value) },
        Some(revision),
        instance,
    );
    ok_json(&RevisionResp {
        revision,
        session: session_for_table(&req.table, revision),
    })
}

pub(crate) async fn handle_exists(
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: KeyReq = parse_req(payload)?;
    ensure_min_revision(&req.table, &req.consistency, state, store).await?;

    let exists = {
        let s = state.borrow();
        s.tables
            .get(&req.table)
            .map_or(false, |t| t.data.contains_key(&req.key))
    };

    ok_json(&ExistsResp { exists })
}

pub(crate) async fn handle_keys(
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: KeysReq = parse_req(payload)?;
    ensure_min_revision(&req.table, &req.consistency, state, store).await?;

    let s = state.borrow();
    let table = s.tables.get(&req.table);
    let mut keys: Vec<String> = table.map_or_else(Vec::new, |t| t.data.keys().cloned().collect());
    keys.sort();

    let page_size = 100usize;
    let offset = req.cursor.unwrap_or(0) as usize;
    let page: Vec<String> = keys.into_iter().skip(offset).take(page_size).collect();
    let next_cursor = if page.len() == page_size {
        Some((offset + page_size) as u64)
    } else {
        None
    };

    ok_json(&KeysResp {
        keys: page,
        cursor: next_cursor,
        next_cursor: next_cursor.map(|c| c.to_string()),
    })
}

pub(crate) async fn handle_prefix(
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: PrefixReq = parse_req(payload)?;
    ensure_min_revision(&req.table, &req.consistency, state, store).await?;
    ensure_loaded(&req.table, state, store).await?;

    let s = state.borrow();
    let table = s.tables.get(&req.table);
    let mut rows: Vec<PrefixRow> = match table {
        Some(t) => t
            .data
            .iter()
            .filter(|(k, _)| k.starts_with(&req.prefix))
            .map(|(k, row)| PrefixRow {
                key: k.clone(),
                value: B64.encode(&row.value),
                revision: row.revision,
            })
            .collect(),
        None => vec![],
    };
    rows.sort_by(|a, b| a.key.cmp(&b.key));

    ok_json(&PrefixResp { rows })
}

// ── Batch operations ───────────────────────────────────────────────

pub(crate) async fn handle_batch_get(
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: BatchGetReq = parse_req(payload)?;
    ensure_min_revision(&req.table, &req.consistency, state, store).await?;

    let s = state.borrow();
    let table = s.tables.get(&req.table);
    let results: Vec<BatchGetResult> = req
        .keys
        .iter()
        .map(|key| match table.and_then(|t| t.data.get(key)) {
            Some(row) => BatchGetResult {
                key: key.clone(),
                value: Some(B64.encode(&row.value)),
                revision: Some(row.revision),
                error: None,
            },
            _ => BatchGetResult {
                key: key.clone(),
                value: None,
                revision: None,
                error: Some("not found".into()),
            },
        })
        .collect();

    ok_json(&BatchGetResp { results })
}

pub(crate) async fn handle_batch_put(
    client: &Client,
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
    instance: &str,
) -> Result<Vec<u8>, String> {
    let req: BatchPutReq = parse_req(payload)?;
    if req.entries.len() > MAX_BATCH_SIZE {
        return Err(format!("batch exceeds maximum of {MAX_BATCH_SIZE} entries"));
    }
    ensure_loaded(&req.table, state, store).await?;

    // Schema validation for all entries upfront.
    {
        let s = state.borrow();
        if let Some(schema) = s.tables.get(&req.table).and_then(|t| t.schema.as_ref()) {
            for entry in &req.entries {
                let value = B64
                    .decode(&entry.value)
                    .map_err(|e| format!("base64 for {}: {e}", entry.key))?;
                state::validate_schema(&value, schema)?;
            }
        }
    }

    let kv = crate::store::get_or_create_kv(store, &req.table)
        .await
        .map_err(|e| format!("{e}"))?;
    let encrypted = state.borrow().is_encrypted(&req.table);

    let mut results = Vec::with_capacity(req.entries.len());
    for entry in &req.entries {
        let value = B64
            .decode(&entry.value)
            .map_err(|e| format!("base64: {e}"))?;
        validate_write_bounds(&req.table, &entry.key, &value)?;
        let store_bytes = maybe_encrypt(&req.table, &entry.key, &value, encrypted);
        let revision = match entry.ttl_seconds {
            Some(ttl) => kv
                .put_with_ttl(&entry.key, &store_bytes, secs(ttl))
                .await
                .map_err(|e| format!("{e}"))?,
            None => kv
                .put(&entry.key, &store_bytes)
                .await
                .map_err(|e| format!("{e}"))?,
        };

        state
            .borrow_mut()
            .table(&req.table)
            .upsert(&entry.key, value, revision);

        publish_change(
            client,
            "put",
            &req.table,
            &entry.key,
            if encrypted { None } else { Some(&entry.value) },
            Some(revision),
            instance,
        );
        results.push(BatchPutResult {
            key: entry.key.clone(),
            revision,
        });
    }

    let max_revision = results.iter().map(|r| r.revision).max().unwrap_or(0);
    ok_json(&BatchPutResp {
        results,
        session: session_for_table(&req.table, max_revision),
    })
}



// ── Schema management ──────────────────────────────────────────────

pub(crate) async fn handle_schema_set(
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: SchemaSetReq = parse_req(payload)?;
    if let Some(fields) = req.schema.get("fields") {
        if !fields.is_object() {
            return Err("schema.fields must be an object".into());
        }
    }
    let kv = crate::store::get_or_create_kv(store, "_schemas")
        .await
        .map_err(|e| format!("{e}"))?;
    let schema_bytes = serde_json::to_vec(&req.schema).map_err(|e| format!("{e}"))?;
    kv.put(&req.table, &schema_bytes)
        .await
        .map_err(|e| format!("{e}"))?;

    state.borrow_mut().table(&req.table).schema = Some(req.schema.clone());
    state.borrow_mut().table(&req.table).encrypted = req
        .schema
        .get("encrypted")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    ok_json(&EmptyResp {})
}

pub(crate) fn handle_schema_get(state: &SharedState, payload: &[u8]) -> Result<Vec<u8>, String> {
    let req: TableReq = parse_req(payload)?;
    let s = state.borrow();
    let schema = s
        .tables
        .get(&req.table)
        .and_then(|t| t.schema.clone())
        .unwrap_or(serde_json::Value::Null);
    ok_json(&SchemaResp { schema })
}


pub(crate) async fn handle_schema_delete(
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: TableReq = parse_req(payload)?;
    let kv = crate::store::get_or_create_kv(store, "_schemas")
        .await
        .map_err(|e| format!("{e}"))?;
    let _ = kv.delete(&req.table).await;

    state.borrow_mut().table(&req.table).schema = None;
    ok_json(&EmptyResp {})
}

// ── Helpers ────────────────────────────────────────────────────────

fn parse_req<T: for<'de> Deserialize<'de>>(payload: &[u8]) -> Result<T, String> {
    serde_json::from_slice(payload).map_err(|e| format!("invalid request: {e}"))
}

fn ok_json<T: Serialize>(val: &T) -> Result<Vec<u8>, String> {
    serde_json::to_vec(val).map_err(|e| format!("serialize: {e}"))
}

/// Publish a change event for watch/notification subscribers.
fn publish_change(
    client: &Client,
    op: &str,
    table: &str,
    key: &str,
    value: Option<&str>,
    revision: Option<u64>,
    instance: &str,
) {
    let event = ChangeEvent {
        op: op.to_string(),
        table: table.to_string(),
        key: key.to_string(),
        value: value.map(|v| v.to_string()),
        revision,
    };
    if let Ok(bytes) = serde_json::to_vec(&event) {
        // Subscribers can scope to `{instance}-events.{table}.>` to receive
        // only events from this instance.
        let subject = format!("{instance}-events.{table}.{key}");
        let _ = client.publish(&subject, &bytes);
    }
}

/// Returns true if the table name is reserved.
/// `_sql_catalog` is allowed as the catalog store for lattice-sql.
pub(crate) fn is_reserved_table(table: &str) -> bool {
    table.starts_with('_') && table != "_sql_catalog"
}

/// S-01: reject requests targeting reserved (_-prefixed) table names.
///
/// Checked against the raw user-supplied payload, before any partition prefix
/// transformation, so `_indexes` and `_schemas` are always blocked.
pub(crate) fn check_no_reserved_tables(_op: &str, payload: &[u8]) -> Result<(), String> {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return Ok(()); // malformed JSON is caught later by parse_req
    };
    if let Some(table) = v.get("table").and_then(|t| t.as_str()) {
        if is_reserved_table(table) {
            return Err(format!("table name '{table}' is reserved"));
        }
    }
    Ok(())
}

/// S-04: validate key and decoded-value sizes before writing to NATS KV.
pub(crate) fn validate_write_bounds(table: &str, key: &str, value: &[u8]) -> Result<(), String> {
    if table.len() > MAX_TABLE_LEN {
        return Err(format!(
            "table name exceeds maximum length of {MAX_TABLE_LEN}"
        ));
    }
    if key.len() > MAX_KEY_LEN {
        return Err(format!("key exceeds maximum length of {MAX_KEY_LEN}"));
    }
    if value.len() > MAX_VALUE_BYTES {
        return Err(format!(
            "value exceeds maximum size of {} bytes",
            MAX_VALUE_BYTES
        ));
    }
    Ok(())
}

/// Check auth token in the request payload.
///
/// S-03: uses constant-time comparison to prevent timing-based token oracle attacks.
pub(crate) fn check_auth(payload: &[u8], expected: &str) -> Result<(), String> {
    let v: serde_json::Value =
        serde_json::from_slice(payload).map_err(|_| "unauthorized".to_string())?;
    let provided = v.get("_auth").and_then(|v| v.as_str()).unwrap_or("");
    if !ct_eq(provided.as_bytes(), expected.as_bytes()) {
        return Err("unauthorized".into());
    }
    Ok(())
}

/// Constant-time byte-slice equality. Prevents timing-based token oracle attacks (S-03).
pub(crate) fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut acc = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        acc |= x ^ y;
    }
    acc == 0
}
