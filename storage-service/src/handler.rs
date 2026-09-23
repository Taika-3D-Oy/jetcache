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

use crate::state::{self, FieldFilter, SharedState};
use crate::store::SharedStore;
use crate::txn;

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

fn env_u32(name: &str, default: u32, min: u32, max: u32) -> u32 {
    let raw = match std::env::var(name) {
        Ok(v) => v,
        Err(_) => return default,
    };
    let parsed = raw.parse::<u32>().ok().unwrap_or(default);
    parsed.clamp(min, max)
}

fn env_u64(name: &str, default: u64, min: u64, max: u64) -> u64 {
    let raw = match std::env::var(name) {
        Ok(v) => v,
        Err(_) => return default,
    };
    let parsed = raw.parse::<u64>().ok().unwrap_or(default);
    parsed.clamp(min, max)
}

fn consistency_watcher_wait_steps() -> u32 {
    static STEPS: OnceLock<u32> = OnceLock::new();
    *STEPS.get_or_init(|| {
        env_u32(
            "LDB_CONSISTENCY_WATCHER_WAIT_STEPS",
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
            "LDB_CONSISTENCY_WATCHER_WAIT_STEP_SECS",
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

#[derive(Deserialize)]
struct KeysReq {
    table: String,
    #[serde(default)]
    cursor: Option<u64>,
    #[serde(default)]
    consistency: Option<ConsistencyReq>,
}

#[derive(Deserialize)]
struct FieldReq {
    table: String,
    field: String,
}

#[derive(Deserialize)]
struct ScanReq {
    table: String,
    #[serde(default)]
    filters: Vec<FieldFilter>,
    #[serde(default)]
    order_by: Option<SortByReq>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    offset: Option<u32>,
    #[serde(default)]
    key_prefix: Option<String>,
    #[serde(default)]
    consistency: Option<ConsistencyReq>,
}

#[derive(Deserialize)]
struct SortByReq {
    field: String,
    #[serde(default = "default_asc")]
    order: String,
}

fn default_asc() -> String {
    "asc".to_string()
}

#[derive(Deserialize)]
struct CountReq {
    table: String,
    #[serde(default)]
    filters: Vec<FieldFilter>,
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
struct AggregateReq {
    table: String,
    #[serde(default)]
    filters: Vec<FieldFilter>,
    #[serde(default)]
    group_by: Option<String>,
    ops: Vec<AggOpReq>,
    #[serde(default)]
    consistency: Option<ConsistencyReq>,
}

#[derive(Deserialize)]
struct AggOpReq {
    #[serde(rename = "fn")]
    fn_name: String,
    #[serde(default)]
    field: Option<String>,
}

#[derive(Deserialize)]
struct SchemaSetReq {
    table: String,
    schema: serde_json::Value,
}

#[derive(Deserialize)]
struct SchedulePutReq {
    table: String,
    key: String,
    value: String, // base64
    /// RFC 3339 UTC timestamp at which the write should fire.
    at: String,
    #[serde(default)]
    ttl_seconds: Option<u64>,
}

/// Body stored in the schedule message and re-delivered to the fire subject.
#[derive(Serialize, Deserialize)]
struct ScheduleFireBody {
    /// base64-encoded value to write.
    v: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    ttl: Option<u64>,
}

#[derive(Deserialize)]
struct IndexCreateReq {
    table: String,
    /// Single field index.
    #[serde(default)]
    field: Option<String>,
    /// Compound index (multiple fields).
    #[serde(default)]
    fields: Option<Vec<String>>,
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
}

#[derive(Serialize)]
struct ScanResp {
    rows: Vec<RowResp>,
    total_count: u64,
}

#[derive(Serialize)]
struct CountResp {
    count: u64,
}

#[derive(Serialize)]
struct IndexesResp {
    indexes: Vec<String>,
}

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
struct AggregateResp {
    groups: Vec<state::AggGroup>,
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
    js: &JetStream,
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
        "scan" => handle_scan(state, store, payload).await,
        "count" => handle_count(state, store, payload).await,
        "index.create" | "index_create" => handle_index_create(state, store, payload).await,
        "index.drop" | "index_drop" => handle_index_drop(state, store, payload).await,
        "index.list" | "index_list" => handle_index_list(state, payload),
        "txn" => handle_txn(js, state, store, payload, config.data_instance.as_str()).await,
        "batch.get" | "batch_get" => handle_batch_get(state, store, payload).await,
        "batch.put" | "batch_put" => handle_batch_put(client, state, store, payload, instance).await,
        "aggregate" => handle_aggregate(state, store, payload).await,
        "schema.set" | "schema_set" => handle_schema_set(state, store, payload).await,
        "schema.get" | "schema_get" => handle_schema_get(state, payload),
        "schema.delete" | "schema_delete" => handle_schema_delete(state, store, payload).await,
        "schedule_put" => {
            handle_schedule_put(js, state, store, payload, config.data_instance.as_str()).await
        }
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
    let existing_index_fields: Vec<String> = ts.indexes.keys().cloned().collect();
    let existing_compound_defs: Vec<(String, Vec<String>)> = ts
        .compound_indexes
        .iter()
        .map(|(name, idx)| (name.clone(), idx.fields.clone()))
        .collect();
    ts.data.clear();
    ts.indexes.clear();
    for field in existing_index_fields {
        ts.indexes.insert(field, std::collections::BTreeMap::new());
    }
    ts.compound_indexes.clear();
    for (name, fields) in existing_compound_defs {
        ts.compound_indexes.insert(
            name,
            state::CompoundIndex {
                fields,
                data: std::collections::BTreeMap::new(),
            },
        );
    }
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
    })
}

pub(crate) async fn handle_scan(
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: ScanReq = parse_req(payload)?;
    ensure_min_revision(&req.table, &req.consistency, state, store).await?;

    let s = state.borrow();
    let table = s.tables.get(&req.table);
    let Some(table) = table else {
        return ok_json(&ScanResp {
            rows: vec![],
            total_count: 0,
        });
    };

    // Try index scan, fall back to full scan.
    let matching_keys = match state::index_scan(table, &req.filters) {
        Some(keys) => keys,
        None => table
            .data
            .iter()
            .filter(|(k, row)| {
                req.key_prefix
                    .as_ref()
                    .map_or(true, |pfx| k.starts_with(pfx.as_str()))
                    && state::matches_filters(&row.value, &req.filters)
            })
            .map(|(k, _)| k.clone())
            .collect(),
    };

    // Post-filter by key_prefix for index-scanned keys.
    let matching_keys: Vec<String> = matching_keys
        .into_iter()
        .filter(|k| {
            req.key_prefix
                .as_ref()
                .map_or(true, |pfx| k.starts_with(pfx.as_str()))
                && table.data.contains_key(k.as_str())
        })
        .collect();

    let total_count = matching_keys.len() as u64;

    // Sort if requested.
    let mut sorted_keys = matching_keys;
    if let Some(ref order_by) = req.order_by {
        sorted_keys.sort_by(|a, b| {
            let va = table
                .data
                .get(a)
                .and_then(|r| state::extract_json_field(&r.value, &order_by.field));
            let vb = table
                .data
                .get(b)
                .and_then(|r| state::extract_json_field(&r.value, &order_by.field));
            let cmp = va.as_deref().cmp(&vb.as_deref());
            if order_by.order == "desc" {
                cmp.reverse()
            } else {
                cmp
            }
        });
    }

    // Paginate.
    let offset = req.offset.unwrap_or(0) as usize;
    let limit = req.limit.unwrap_or(100) as usize;
    let page: Vec<RowResp> = sorted_keys
        .into_iter()
        .skip(offset)
        .take(limit)
        .filter_map(|k| {
            let row = table.data.get(&k)?;
            Some(RowResp {
                key: k,
                value: B64.encode(&row.value),
                revision: row.revision,
                session: None,
            })
        })
        .collect();

    ok_json(&ScanResp {
        rows: page,
        total_count,
    })
}

pub(crate) async fn handle_count(
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: CountReq = parse_req(payload)?;
    ensure_min_revision(&req.table, &req.consistency, state, store).await?;

    let s = state.borrow();
    let table = s.tables.get(&req.table);
    let count = match table {
        None => 0,
        Some(table) => match state::index_scan(table, &req.filters) {
            Some(keys) => keys
                .iter()
                .filter(|k| table.data.contains_key(k.as_str()))
                .count() as u64,
            None => table
                .data
                .values()
                .filter(|row| state::matches_filters(&row.value, &req.filters))
                .count() as u64,
        },
    };

    ok_json(&CountResp { count })
}

pub(crate) async fn handle_index_create(
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: IndexCreateReq = parse_req(payload)?;
    ensure_loaded(&req.table, state, store).await?;

    let index_name = if let Some(fields) = &req.fields {
        if fields.len() < 2 {
            return Err("compound index requires at least 2 fields".into());
        }
        state
            .borrow_mut()
            .table(&req.table)
            .create_compound_index(fields);
        fields.join("+")
    } else if let Some(ref field) = req.field {
        state.borrow_mut().table(&req.table).create_index(field);
        field.clone()
    } else {
        return Err("either 'field' or 'fields' is required".into());
    };

    // Persist index definition to _indexes bucket.
    let kv = crate::store::get_or_create_kv(store, "_indexes")
        .await
        .map_err(|e| format!("{e}"))?;
    let index_key = format!("{}.{}", req.table, index_name);
    let fields_list = if let Some(fields) = &req.fields {
        fields.clone()
    } else if let Some(field) = &req.field {
        vec![field.clone()]
    } else {
        return Err("no fields for index".into());
    };
    let index_def = serde_json::json!({
        "table": req.table,
        "name": index_name,
        "fields": fields_list,
    });
    let index_bytes = serde_json::to_vec(&index_def).map_err(|e| format!("{e}"))?;
    kv.put(&index_key, &index_bytes)
        .await
        .map_err(|e| format!("{e}"))?;

    ok_json(&EmptyResp {})
}

pub(crate) async fn handle_index_drop(
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: FieldReq = parse_req(payload)?;
    let mut s = state.borrow_mut();
    let t = s.table(&req.table);
    t.drop_index(&req.field);
    t.drop_compound_index(&req.field);
    drop(s); // explicitly drop to release borrow

    // Delete from _indexes bucket.
    let kv = crate::store::get_or_create_kv(store, "_indexes")
        .await
        .map_err(|e| format!("{e}"))?;
    let index_key = format!("{}.{}", req.table, req.field);
    let _ = kv.delete(&index_key).await;

    ok_json(&EmptyResp {})
}

pub(crate) fn handle_index_list(state: &SharedState, payload: &[u8]) -> Result<Vec<u8>, String> {
    let req: TableReq = parse_req(payload)?;
    let s = state.borrow();
    let mut indexes: Vec<String> = s.tables.get(&req.table).map_or_else(Vec::new, |t| {
        let mut v: Vec<String> = t.indexes.keys().cloned().collect();
        v.extend(t.compound_indexes.keys().cloned());
        v
    });
    indexes.sort();
    ok_json(&IndexesResp { indexes })
}

pub(crate) async fn handle_txn(
    js: &JetStream,
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
    instance: &str,
) -> Result<Vec<u8>, String> {
    let req: txn::TxnRequest = parse_req(payload)?;
    let resp = txn::execute(js, state, store, req, instance).await?;
    ok_json(&resp)
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

// ── Aggregation ────────────────────────────────────────────────────

pub(crate) async fn handle_aggregate(
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: AggregateReq = parse_req(payload)?;
    ensure_min_revision(&req.table, &req.consistency, state, store).await?;

    let ops: Vec<state::AggOp> = req
        .ops
        .iter()
        .map(|o| state::AggOp {
            fn_name: o.fn_name.clone(),
            field: o.field.clone(),
        })
        .collect();

    let s = state.borrow();
    let table = s.tables.get(&req.table);
    let Some(table) = table else {
        return ok_json(&AggregateResp { groups: vec![] });
    };

    let groups = state::aggregate(table, &req.filters, req.group_by.as_deref(), &ops);
    ok_json(&AggregateResp { groups })
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

/// Registers a one-shot delayed write via NATS 2.14+ message scheduling (ADR-51).
///
/// Validates the request, encodes the value into the schedule body, and
/// publishes a schedule message. The NATS server fires the body to the
/// fire subject when `at` arrives; `handle_schedule_fire` performs the KV put.
pub(crate) async fn handle_schedule_put(
    js: &JetStream,
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
    data_instance: &str,
) -> Result<Vec<u8>, String> {
    let req: SchedulePutReq = parse_req(payload)?;
    let value = B64.decode(&req.value).map_err(|e| format!("base64: {e}"))?;
    validate_write_bounds(&req.table, &req.key, &value)?;

    // Validate the `at` timestamp is at least superficially RFC 3339-shaped.
    // The NATS server validates the full semantics.
    if req.at.len() < 10 || !req.at.starts_with(|c: char| c.is_ascii_digit()) {
        return Err("at must be an RFC 3339 timestamp (e.g. 2026-06-01T09:00:00Z)".into());
    }

    // Schema validation against in-memory schema (if any).
    {
        let s = state.borrow();
        if let Some(schema) = s.tables.get(&req.table).and_then(|t| t.schema.as_ref()) {
            state::validate_schema(&value, schema)?;
        }
    }

    // Ensure the table's KV bucket exists so the fire handler can write to it.
    let _ = crate::store::get_or_create_kv(store, &req.table)
        .await
        .map_err(|e| format!("{e}"))?;

    let fire_body = ScheduleFireBody {
        v: req.value.clone(),
        ttl: req.ttl_seconds,
    };
    let body = serde_json::to_vec(&fire_body).map_err(|e| format!("serialize: {e}"))?;

    crate::schedule::publish_schedule_at(js, data_instance, &req.table, &req.key, &body, &req.at)
        .await
        .map_err(|e| format!("{e}"))?;

    ok_json(&EmptyResp {})
}

/// Called when NATS fires a scheduled write to `{data_instance}-sched-fire.{table}.{key}`.
///
/// Extracts table and key from the subject, decodes the fire body, and
/// performs the KV put. Also publishes the standard change event.
pub(crate) async fn handle_schedule_fire(
    client: &Client,
    store: &SharedStore,
    state: &SharedState,
    subject: &str,
    payload: &[u8],
    instance: &str,
) {
    // Subject format: `{data_instance}-sched-fire.{table}.{key}`.
    // Find the first '.' after the prefix to extract table and key.
    let fire_part = match subject.find("-sched-fire.") {
        Some(idx) => &subject[idx + "-sched-fire.".len()..],
        None => {
            eprintln!("lattice-db: schedule fire: unrecognised subject {subject}");
            return;
        }
    };
    let (table, key) = match fire_part.split_once('.') {
        Some(pair) => pair,
        None => {
            eprintln!("lattice-db: schedule fire: missing key in subject {subject}");
            return;
        }
    };

    // S-01: reject reserved tables.
    if is_reserved_table(table) {
        eprintln!("lattice-db: schedule fire: rejected write targeting reserved table {table}");
        return;
    }

    let fire_body: ScheduleFireBody = match serde_json::from_slice(payload) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("lattice-db: schedule fire: bad body on {subject}: {e}");
            return;
        }
    };

    let value = match B64.decode(&fire_body.v) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("lattice-db: schedule fire: base64 decode error on {subject}: {e}");
            return;
        }
    };

    // S-04: validate write bounds before writing.
    if let Err(e) = validate_write_bounds(table, key, &value) {
        eprintln!("lattice-db: schedule fire: bounds validation failed for {table}/{key}: {e}");
        return;
    }

    // Validate schema if defined for this table.
    if let Some(schema) = state
        .borrow()
        .tables
        .get(table)
        .and_then(|t| t.schema.as_ref())
    {
        if let Err(e) = crate::state::validate_schema(&value, schema) {
            eprintln!("lattice-db: schedule fire: schema validation failed for {table}/{key}: {e}");
            return;
        }
    }

    let kv = match crate::store::get_or_create_kv(store, table).await {
        Ok(kv) => kv,
        Err(e) => {
            eprintln!("lattice-db: schedule fire: kv error for table {table}: {e}");
            return;
        }
    };

    let encrypted = state.borrow().is_encrypted(table);
    let store_bytes = maybe_encrypt(table, key, &value, encrypted);
    let result = match fire_body.ttl {
        Some(ttl) => kv.put_with_ttl(key, &store_bytes, secs(ttl)).await,
        None => kv.put(key, &store_bytes).await,
    };

    match result {
        Ok(revision) => {
            state.borrow_mut().table(table).upsert(key, value, revision);
            publish_change(
                client,
                "put",
                table,
                key,
                if encrypted { None } else { Some(&fire_body.v) },
                Some(revision),
                instance,
            );
        }
        Err(e) => {
            eprintln!("lattice-db: schedule fire: kv put failed for {table}/{key}: {e}");
        }
    }
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
pub(crate) fn check_no_reserved_tables(op: &str, payload: &[u8]) -> Result<(), String> {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return Ok(()); // malformed JSON is caught later by parse_req
    };
    if op == "txn" {
        if let Some(ops) = v.get("ops").and_then(|o| o.as_array()) {
            for entry in ops {
                if let Some(table) = entry.get("table").and_then(|t| t.as_str()) {
                    if is_reserved_table(table) {
                        return Err(format!("table name '{table}' is reserved"));
                    }
                }
            }
        }
    } else if let Some(table) = v.get("table").and_then(|t| t.as_str()) {
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
