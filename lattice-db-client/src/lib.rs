//! # lattice-db-client
//!
//! Typed Rust SDK for [lattice-db](https://github.com/Taika-3D-Oy/lattice-db) — a
//! NATS-native distributed database for wasmCloud.
//!
//! Wraps the NATS request/reply wire protocol with ergonomic Rust methods.
//! Values are transparently base64-encoded on the wire but exposed as raw
//! bytes (`&[u8]` / `Vec<u8>`) to the caller. JSON convenience methods are
//! provided via [`LatticeDb::get_json`] and [`LatticeDb::put_json`].
//!
//! ## Quick start
//!
//! ```rust,no_run
//! use nats_wasi::client::{Client, ConnectConfig};
//! use lattice_db_client::LatticeDb;
//!
//! # async fn example() -> Result<(), lattice_db_client::Error> {
//! let client = Client::connect(ConnectConfig::default()).await?;
//! let db = LatticeDb::new(client)
//!     .with_auth("my-token")          // matches LDB_AUTH_TOKEN on the server
//!     .with_instance("acme");          // matches LDB_INSTANCE=acme on the server
//!
//! // Store and retrieve JSON
//! db.put_json("users", "alice", &serde_json::json!({"name": "Alice", "age": 30})).await?;
//! let user: serde_json::Value = db.get_json("users", "alice").await?;
//!
//! // Scan with filters
//! use lattice_db_client::{Filter, ScanQuery};
//! let results = db.scan("users", ScanQuery::new()
//!     .filter("age", "gte", "25")
//!     .order_by("name", "asc")
//!     .limit(10)
//! ).await?;
//!
//! // Atomic transactions
//! use lattice_db_client::TxnOp;
//! db.transaction(vec![
//!     TxnOp::put("accounts", "alice", b"{\"balance\":90}"),
//!     TxnOp::put("accounts", "bob",   b"{\"balance\":110}"),
//! ]).await?;
//! # Ok(())
//! # }
//! ```

use base64::Engine;
use nats_wasi::client::{Client, Duration, secs};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::cell::RefCell;
use std::collections::HashMap;

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

// ── Error type ─────────────────────────────────────────────────────

/// Errors returned by the lattice-db client.
#[derive(Debug)]
pub enum Error {
    /// NATS transport error.
    Nats(nats_wasi::Error),
    /// lattice-db returned an error response.
    Db(String),
    /// JSON serialization / deserialization error.
    Json(String),
    /// Base64 decoding error.
    Base64(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Nats(e) => write!(f, "nats: {e}"),
            Error::Db(e) => write!(f, "lattice-db: {e}"),
            Error::Json(e) => write!(f, "json: {e}"),
            Error::Base64(e) => write!(f, "base64: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<nats_wasi::Error> for Error {
    fn from(e: nats_wasi::Error) -> Self {
        Error::Nats(e)
    }
}

// ── Public types ───────────────────────────────────────────────────

/// A row returned from get / scan operations.
#[derive(Debug, Clone)]
pub struct Row {
    pub key: String,
    pub value: Vec<u8>,
    pub revision: u64,
}

/// An entry at a specific revision, including tombstones.
/// Returned by [`LatticeDb::get_revision`].
#[derive(Debug, Clone)]
pub struct RevisionEntry {
    pub key: String,
    pub value: Vec<u8>,
    /// The NATS stream sequence (revision) of this entry.
    pub revision: u64,
    /// The operation that produced this entry: `"put"`, `"delete"`, or `"purge"`.
    pub operation: String,
}

/// A page of keys from the keys operation.
#[derive(Debug, Clone)]
pub struct KeysPage {
    pub keys: Vec<String>,
    /// If `Some`, pass this to the next call to get more keys.
    pub cursor: Option<u64>,
}

/// Scan query builder.
#[derive(Debug, Clone, Default)]
pub struct ScanQuery {
    filters: Vec<Filter>,
    order_by: Option<(String, String)>,
    limit: Option<u32>,
    offset: Option<u32>,
    key_prefix: Option<String>,
}

impl ScanQuery {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a filter: `field` `op` `value`.
    /// Supported ops: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `prefix`.
    pub fn filter(mut self, field: &str, op: &str, value: &str) -> Self {
        self.filters.push(Filter {
            field: field.into(),
            op: op.into(),
            value: value.into(),
        });
        self
    }

    /// Sort results by `field` in `order` (`asc` or `desc`).
    pub fn order_by(mut self, field: &str, order: &str) -> Self {
        self.order_by = Some((field.into(), order.into()));
        self
    }

    /// Limit the number of results returned.
    pub fn limit(mut self, n: u32) -> Self {
        self.limit = Some(n);
        self
    }

    /// Skip the first `n` results.
    pub fn offset(mut self, n: u32) -> Self {
        self.offset = Some(n);
        self
    }

    /// Only include keys starting with this prefix.
    pub fn key_prefix(mut self, prefix: &str) -> Self {
        self.key_prefix = Some(prefix.into());
        self
    }
}

/// A scan/count filter.
#[derive(Debug, Clone, Serialize)]
pub struct Filter {
    pub field: String,
    pub op: String,
    pub value: String,
}

/// Result of a scan operation.
#[derive(Debug, Clone)]
pub struct ScanResult {
    pub rows: Vec<Row>,
    pub total_count: u64,
}

/// Result of a batch get for a single key.
#[derive(Debug, Clone)]
pub struct BatchGetResult {
    pub key: String,
    pub value: Option<Vec<u8>>,
    pub revision: Option<u64>,
    pub error: Option<String>,
}

/// Result of a batch put for a single key.
#[derive(Debug, Clone)]
pub struct BatchPutResult {
    pub key: String,
    pub revision: u64,
}

/// An entry for batch put.
pub struct BatchPutEntry<'a> {
    pub key: &'a str,
    pub value: &'a [u8],
    pub ttl_seconds: Option<u64>,
}

/// A transaction operation.
#[derive(Debug, Clone, Serialize)]
pub struct TxnOp {
    pub op: String,
    pub table: String,
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

impl TxnOp {
    pub fn put(table: &str, key: &str, value: &[u8]) -> Self {
        Self { op: "put".into(), table: table.into(), key: key.into(), value: Some(B64.encode(value)) }
    }
    pub fn create(table: &str, key: &str, value: &[u8]) -> Self {
        Self { op: "create".into(), table: table.into(), key: key.into(), value: Some(B64.encode(value)) }
    }
    pub fn delete(table: &str, key: &str) -> Self {
        Self { op: "delete".into(), table: table.into(), key: key.into(), value: None }
    }
}

/// Result of a transaction.
#[derive(Debug, Clone)]
pub struct TxnResult {
    pub ok: bool,
    pub results: Vec<TxnOpResult>,
}

/// Per-op result from a transaction.
#[derive(Debug, Clone, Deserialize)]
pub struct TxnOpResult {
    pub op: String,
    pub table: String,
    pub key: String,
    pub revision: Option<u64>,
}

/// An aggregation operation.
#[derive(Debug, Clone, Serialize)]
pub struct AggOp {
    #[serde(rename = "fn")]
    pub fn_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}

impl AggOp {
    pub fn count() -> Self {
        Self { fn_name: "count".into(), field: None }
    }
    pub fn sum(field: &str) -> Self {
        Self { fn_name: "sum".into(), field: Some(field.into()) }
    }
    pub fn avg(field: &str) -> Self {
        Self { fn_name: "avg".into(), field: Some(field.into()) }
    }
    pub fn min(field: &str) -> Self {
        Self { fn_name: "min".into(), field: Some(field.into()) }
    }
    pub fn max(field: &str) -> Self {
        Self { fn_name: "max".into(), field: Some(field.into()) }
    }
}

/// A group in an aggregation result.
#[derive(Debug, Clone, Deserialize)]
pub struct AggGroup {
    pub key: Option<String>,
    pub results: Vec<AggResult>,
}

/// A single aggregation result value.
#[derive(Debug, Clone, Deserialize)]
pub struct AggResult {
    pub op: String,
    pub field: Option<String>,
    pub value: serde_json::Value,
}

// ── Wire types (private) ───────────────────────────────────────────

#[derive(Serialize)]
struct KeyReq<'a> { table: &'a str, key: &'a str }

#[derive(Serialize)]
struct PutReq<'a> {
    table: &'a str,
    key: &'a str,
    value: String, // base64
    #[serde(skip_serializing_if = "Option::is_none")]
    ttl_seconds: Option<u64>,
}

#[derive(Serialize)]
struct CasReq<'a> {
    table: &'a str,
    key: &'a str,
    value: String, // base64
    revision: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    ttl_seconds: Option<u64>,
}

#[derive(Serialize)]
struct CasDeleteReqW<'a> {
    table: &'a str,
    key: &'a str,
    revision: u64,
}

#[derive(Serialize)]
struct PurgeReqW<'a> {
    table: &'a str,
    key: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    revision: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ttl_seconds: Option<u64>,
}

#[derive(Serialize)]
struct GetRevisionReqW<'a> {
    table: &'a str,
    key: &'a str,
    revision: u64,
}

#[derive(Serialize)]
struct TableReq<'a> { table: &'a str }

#[derive(Serialize)]
struct KeysReqW<'a> {
    table: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<u64>,
}

#[derive(Serialize)]
struct ScanReqW<'a> {
    table: &'a str,
    filters: &'a [Filter],
    #[serde(skip_serializing_if = "Option::is_none")]
    order_by: Option<SortByW>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    offset: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    key_prefix: Option<&'a str>,
}

#[derive(Serialize)]
struct SortByW { field: String, order: String }

#[derive(Serialize)]
struct CountReqW<'a> {
    table: &'a str,
    filters: &'a [Filter],
}

#[derive(Serialize)]
struct BatchGetReqW<'a> {
    table: &'a str,
    keys: &'a [&'a str],
}

#[derive(Serialize)]
struct BatchPutEntryW { key: String, value: String, #[serde(skip_serializing_if = "Option::is_none")] ttl_seconds: Option<u64> }

#[derive(Serialize)]
struct BatchPutReqW<'a> {
    table: &'a str,
    entries: Vec<BatchPutEntryW>,
}

#[derive(Serialize)]
struct FieldReqW<'a> { table: &'a str, field: &'a str }

#[derive(Serialize)]
struct IndexCreateReqW<'a> {
    table: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    field: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fields: Option<Vec<&'a str>>,
}

#[derive(Serialize)]
struct TxnReqW { ops: Vec<TxnOp> }

#[derive(Serialize)]
struct AggReqW<'a> {
    table: &'a str,
    filters: &'a [Filter],
    #[serde(skip_serializing_if = "Option::is_none")]
    group_by: Option<&'a str>,
    ops: &'a [AggOp],
}

#[derive(Serialize)]
struct SchemaSetReqW<'a> { table: &'a str, schema: &'a serde_json::Value }

#[derive(Serialize)]
struct SchedulePutReqW<'a> {
    table: &'a str,
    key: &'a str,
    value: String, // base64
    at: &'a str,   // RFC 3339 UTC
    #[serde(skip_serializing_if = "Option::is_none")]
    ttl_seconds: Option<u64>,
}

// Responses
#[derive(Deserialize)] struct RowR { key: String, value: String, revision: u64 }
#[derive(Deserialize)] struct RevisionR { revision: u64 }
#[derive(Deserialize)] struct ExistsR { exists: bool }
#[derive(Deserialize)] struct KeysR { keys: Vec<String>, cursor: Option<u64> }
#[derive(Deserialize)] struct ScanR { rows: Vec<RowR>, total_count: u64 }
#[derive(Deserialize)] struct CountR { count: u64 }
#[derive(Deserialize)] struct IndexesR { indexes: Vec<String> }
#[derive(Deserialize)] struct BatchGetR { results: Vec<BatchGetEntryR> }
#[derive(Deserialize)] struct BatchGetEntryR { key: String, value: Option<String>, revision: Option<u64>, error: Option<String> }
#[derive(Deserialize)] struct BatchPutR { results: Vec<BatchPutEntryR> }
#[derive(Deserialize)] struct BatchPutEntryR { key: String, revision: u64 }
#[derive(Deserialize)] struct TxnR { ok: bool, results: Vec<TxnOpResult> }
#[derive(Deserialize)] struct AggR { groups: Vec<AggGroup> }
#[derive(Deserialize)] struct SchemaR { schema: serde_json::Value }
#[derive(Deserialize)] struct RevisionEntryR { key: String, value: String, revision: u64, operation: String }
#[derive(Deserialize)] struct ErrorR { error: Option<String> }
#[derive(Deserialize)] struct SessionR { revisions: HashMap<String, u64> }

// ── Client ─────────────────────────────────────────────────────────

/// Typed client for lattice-db. Wraps a NATS client and forwards
/// requests to the `{instance}.>` subject tree.
pub struct LatticeDb {
    client: Client,
    timeout: Duration,
    /// Value sent as `_auth` on every request. Must match `LDB_AUTH_TOKEN`.
    auth_token: Option<String>,
    /// NATS subject prefix. Must match `LDB_INSTANCE` on the server (default `ldb`).
    instance: String,
    /// Session-level per-table minimum revisions for read-your-write behavior.
    session_revisions: RefCell<HashMap<String, u64>>,
}

impl LatticeDb {
    /// Create a new client with the default 5-second timeout.
    pub fn new(client: Client) -> Self {
        Self {
            client,
            timeout: secs(5),
            auth_token: None,
            instance: "ldb".to_string(),
            session_revisions: RefCell::new(HashMap::new()),
        }
    }

    /// Create a new client with a custom timeout (nanoseconds).
    pub fn with_timeout(client: Client, timeout: Duration) -> Self {
        Self {
            client,
            timeout,
            auth_token: None,
            instance: "ldb".to_string(),
            session_revisions: RefCell::new(HashMap::new()),
        }
    }

    /// Attach a shared auth token. Sent as `_auth` in every request.
    ///
    /// Required when the server is configured with `LDB_AUTH_TOKEN`.
    pub fn with_auth(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Set the instance prefix. Must match `LDB_INSTANCE` on the server.
    ///
    /// Defaults to `"ldb"`. Change this when connecting to a non-default
    /// lattice-db deployment (e.g., one started with `LDB_INSTANCE=orders`).
    pub fn with_instance(mut self, instance: impl Into<String>) -> Self {
        self.instance = instance.into();
        self
    }

    // ── Core CRUD ──────────────────────────────────────────────

    /// Get a row by key. Returns raw bytes.
    pub async fn get(&self, table: &str, key: &str) -> Result<Row, Error> {
        let resp: RowR = self.req(&self.subj("get"), &KeyReq { table, key }).await?;
        Ok(Row {
            key: resp.key,
            value: B64.decode(&resp.value).map_err(|e| Error::Base64(e.to_string()))?,
            revision: resp.revision,
        })
    }

    /// Get a row and deserialize the value as JSON.
    pub async fn get_json<T: DeserializeOwned>(&self, table: &str, key: &str) -> Result<T, Error> {
        let row = self.get(table, key).await?;
        serde_json::from_slice(&row.value).map_err(|e| Error::Json(e.to_string()))
    }

    /// Put a raw byte value. Returns the new revision.
    pub async fn put(&self, table: &str, key: &str, value: &[u8]) -> Result<u64, Error> {
        let resp: RevisionR = self.req(&self.subj("put"), &PutReq {
            table, key, value: B64.encode(value), ttl_seconds: None,
        }).await?;
        Ok(resp.revision)
    }

    /// Put a raw byte value with a TTL. Returns the new revision.
    pub async fn put_with_ttl(&self, table: &str, key: &str, value: &[u8], ttl_seconds: u64) -> Result<u64, Error> {
        let resp: RevisionR = self.req(&self.subj("put"), &PutReq {
            table, key, value: B64.encode(value), ttl_seconds: Some(ttl_seconds),
        }).await?;
        Ok(resp.revision)
    }

    /// Put a JSON-serializable value. Returns the new revision.
    pub async fn put_json<T: Serialize>(&self, table: &str, key: &str, value: &T) -> Result<u64, Error> {
        let bytes = serde_json::to_vec(value).map_err(|e| Error::Json(e.to_string()))?;
        self.put(table, key, &bytes).await
    }

    /// Delete a key.
    pub async fn delete(&self, table: &str, key: &str) -> Result<(), Error> {
        let _: serde_json::Value = self.req(&self.subj("delete"), &KeyReq { table, key }).await?;
        Ok(())
    }

    /// Create a key (fails if it already exists). Returns the revision.
    pub async fn create(&self, table: &str, key: &str, value: &[u8]) -> Result<u64, Error> {
        let resp: RevisionR = self.req(&self.subj("create"), &PutReq {
            table, key, value: B64.encode(value), ttl_seconds: None,
        }).await?;
        Ok(resp.revision)
    }

    /// Create a key with an expiry (fails if it already exists). Returns the revision.
    pub async fn create_with_ttl(&self, table: &str, key: &str, value: &[u8], ttl_seconds: u64) -> Result<u64, Error> {
        let resp: RevisionR = self.req(&self.subj("create"), &PutReq {
            table, key, value: B64.encode(value), ttl_seconds: Some(ttl_seconds),
        }).await?;
        Ok(resp.revision)
    }

    /// Create a key with a JSON value.
    pub async fn create_json<T: Serialize>(&self, table: &str, key: &str, value: &T) -> Result<u64, Error> {
        let bytes = serde_json::to_vec(value).map_err(|e| Error::Json(e.to_string()))?;
        self.create(table, key, &bytes).await
    }

    /// Compare-and-swap: update only if the current revision matches.
    pub async fn cas(&self, table: &str, key: &str, value: &[u8], revision: u64) -> Result<u64, Error> {
        let resp: RevisionR = self.req(&self.subj("cas"), &CasReq {
            table, key, value: B64.encode(value), revision, ttl_seconds: None,
        }).await?;
        Ok(resp.revision)
    }

    /// Compare-and-swap with an expiry. Only updates if revision matches;  
    /// the new value expires after `ttl_seconds`.
    pub async fn cas_with_ttl(&self, table: &str, key: &str, value: &[u8], revision: u64, ttl_seconds: u64) -> Result<u64, Error> {
        let resp: RevisionR = self.req(&self.subj("cas"), &CasReq {
            table, key, value: B64.encode(value), revision, ttl_seconds: Some(ttl_seconds),
        }).await?;
        Ok(resp.revision)
    }

    /// Compare-and-swap delete: tombstone a key only if the current revision matches.
    ///
    /// Prevents race conditions where one client deletes data that another
    /// client has since updated.
    pub async fn cas_delete(&self, table: &str, key: &str, revision: u64) -> Result<(), Error> {
        let _: serde_json::Value = self.req(&self.subj("cas_delete"), &CasDeleteReqW {
            table, key, revision,
        }).await?;
        Ok(())
    }

    /// Purge a key — remove all revisions (not just tombstone).
    ///
    /// Unlike [`LatticeDb::delete`], this removes all history for the key.
    pub async fn purge(&self, table: &str, key: &str) -> Result<(), Error> {
        let _: serde_json::Value = self.req(&self.subj("purge"), &PurgeReqW {
            table, key, revision: None, ttl_seconds: None,
        }).await?;
        Ok(())
    }

    /// Purge a key with an expiring tombstone. The tombstone itself expires
    /// after `ttl_seconds`.
    pub async fn purge_with_ttl(&self, table: &str, key: &str, ttl_seconds: u64) -> Result<(), Error> {
        let _: serde_json::Value = self.req(&self.subj("purge"), &PurgeReqW {
            table, key, revision: None, ttl_seconds: Some(ttl_seconds),
        }).await?;
        Ok(())
    }

    /// Compare-and-swap purge: remove all revisions only if the current
    /// revision matches.
    pub async fn purge_expect_revision(&self, table: &str, key: &str, revision: u64) -> Result<(), Error> {
        let _: serde_json::Value = self.req(&self.subj("purge"), &PurgeReqW {
            table, key, revision: Some(revision), ttl_seconds: None,
        }).await?;
        Ok(())
    }

    /// Compare-and-swap purge with an expiring tombstone.
    pub async fn purge_expect_revision_with_ttl(&self, table: &str, key: &str, revision: u64, ttl_seconds: u64) -> Result<(), Error> {
        let _: serde_json::Value = self.req(&self.subj("purge"), &PurgeReqW {
            table, key, revision: Some(revision), ttl_seconds: Some(ttl_seconds),
        }).await?;
        Ok(())
    }

    /// Fetch the entry at a specific stream sequence (revision), including
    /// delete and purge tombstones. Useful for history inspection and auditing.
    pub async fn get_revision(&self, table: &str, key: &str, revision: u64) -> Result<RevisionEntry, Error> {
        let resp: RevisionEntryR = self.req(&self.subj("get_revision"), &GetRevisionReqW {
            table, key, revision,
        }).await?;
        Ok(RevisionEntry {
            key: resp.key,
            value: B64.decode(&resp.value).map_err(|e| Error::Base64(e.to_string()))?,
            revision: resp.revision,
            operation: resp.operation,
        })
    }

    /// Check if a key exists.
    pub async fn exists(&self, table: &str, key: &str) -> Result<bool, Error> {
        let resp: ExistsR = self.req(&self.subj("exists"), &KeyReq { table, key }).await?;
        Ok(resp.exists)
    }

    // ── Listing ────────────────────────────────────────────────

    /// List all keys in a table (unpaged — collects all pages).
    pub async fn keys(&self, table: &str) -> Result<Vec<String>, Error> {
        let mut all = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.keys_paged(table, cursor).await?;
            all.extend(page.keys);
            cursor = page.cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(all)
    }

    /// List keys with pagination.
    pub async fn keys_paged(&self, table: &str, cursor: Option<u64>) -> Result<KeysPage, Error> {
        let resp: KeysR = self.req(&self.subj("keys"), &KeysReqW { table, cursor }).await?;
        Ok(KeysPage { keys: resp.keys, cursor: resp.cursor })
    }

    // ── Query ──────────────────────────────────────────────────

    /// Scan rows with filters, sorting, and pagination.
    pub async fn scan(&self, table: &str, query: ScanQuery) -> Result<ScanResult, Error> {
        let order_by = query.order_by.map(|(f, o)| SortByW { field: f, order: o });
        let resp: ScanR = self.req(&self.subj("scan"), &ScanReqW {
            table,
            filters: &query.filters,
            order_by,
            limit: query.limit,
            offset: query.offset,
            key_prefix: query.key_prefix.as_deref(),
        }).await?;
        let rows = resp.rows.into_iter().map(|r| -> Result<Row, Error> {
            Ok(Row {
                key: r.key,
                value: B64.decode(&r.value).map_err(|e| Error::Base64(e.to_string()))?,
                revision: r.revision,
            })
        }).collect::<Result<Vec<_>, _>>()?;
        Ok(ScanResult { rows, total_count: resp.total_count })
    }

    /// Count rows matching filters.
    pub async fn count(&self, table: &str, filters: &[Filter]) -> Result<u64, Error> {
        let resp: CountR = self.req(&self.subj("count"), &CountReqW { table, filters }).await?;
        Ok(resp.count)
    }

    // ── Batch operations ───────────────────────────────────────

    /// Get multiple keys in one request.
    pub async fn batch_get(&self, table: &str, keys: &[&str]) -> Result<Vec<BatchGetResult>, Error> {
        let resp: BatchGetR = self.req(&self.subj("batch.get"), &BatchGetReqW { table, keys }).await?;
        let results = resp.results.into_iter().map(|r| {
            let value = r.value.as_ref().and_then(|v| B64.decode(v).ok());
            BatchGetResult { key: r.key, value, revision: r.revision, error: r.error }
        }).collect();
        Ok(results)
    }

    /// Put multiple entries in one request.
    pub async fn batch_put(&self, table: &str, entries: &[BatchPutEntry<'_>]) -> Result<Vec<BatchPutResult>, Error> {
        let wire_entries: Vec<BatchPutEntryW> = entries.iter().map(|e| BatchPutEntryW {
            key: e.key.to_string(),
            value: B64.encode(e.value),
            ttl_seconds: e.ttl_seconds,
        }).collect();
        let resp: BatchPutR = self.req(&self.subj("batch.put"), &BatchPutReqW { table, entries: wire_entries }).await?;
        Ok(resp.results.into_iter().map(|r| BatchPutResult { key: r.key, revision: r.revision }).collect())
    }

    // ── Indexes ────────────────────────────────────────────────

    /// Create a single-field secondary index.
    pub async fn create_index(&self, table: &str, field: &str) -> Result<(), Error> {
        let _: serde_json::Value = self.req(&self.subj("index.create"), &IndexCreateReqW {
            table, field: Some(field), fields: None,
        }).await?;
        Ok(())
    }

    /// Create a compound (multi-field) index.
    pub async fn create_compound_index(&self, table: &str, fields: &[&str]) -> Result<(), Error> {
        let _: serde_json::Value = self.req(&self.subj("index.create"), &IndexCreateReqW {
            table, field: None, fields: Some(fields.to_vec()),
        }).await?;
        Ok(())
    }

    /// Drop an index.
    pub async fn drop_index(&self, table: &str, field: &str) -> Result<(), Error> {
        let _: serde_json::Value = self.req(&self.subj("index.drop"), &FieldReqW { table, field }).await?;
        Ok(())
    }

    /// List all indexes on a table.
    pub async fn list_indexes(&self, table: &str) -> Result<Vec<String>, Error> {
        let resp: IndexesR = self.req(&self.subj("index.list"), &TableReq { table }).await?;
        Ok(resp.indexes)
    }

    // ── Transactions ───────────────────────────────────────────

    /// Execute an atomic multi-key transaction.
    pub async fn transaction(&self, ops: Vec<TxnOp>) -> Result<TxnResult, Error> {
        let resp: TxnR = self.req(&self.subj("txn"), &TxnReqW { ops }).await?;
        Ok(TxnResult { ok: resp.ok, results: resp.results })
    }

    // ── Aggregation ────────────────────────────────────────────

    /// Run aggregation operations, optionally grouped.
    pub async fn aggregate(
        &self,
        table: &str,
        filters: &[Filter],
        group_by: Option<&str>,
        ops: &[AggOp],
    ) -> Result<Vec<AggGroup>, Error> {
        let resp: AggR = self.req(&self.subj("aggregate"), &AggReqW {
            table, filters, group_by, ops,
        }).await?;
        Ok(resp.groups)
    }

    // ── Schema ─────────────────────────────────────────────────

    /// Set a JSON schema for a table.
    pub async fn set_schema(&self, table: &str, schema: &serde_json::Value) -> Result<(), Error> {
        let _: serde_json::Value = self.req(&self.subj("schema.set"), &SchemaSetReqW { table, schema }).await?;
        Ok(())
    }

    /// Get the schema for a table. Returns `None` if no schema is set.
    pub async fn get_schema(&self, table: &str) -> Result<Option<serde_json::Value>, Error> {
        let resp: SchemaR = self.req(&self.subj("schema.get"), &TableReq { table }).await?;
        if resp.schema.is_null() { Ok(None) } else { Ok(Some(resp.schema)) }
    }

    /// Delete the schema for a table.
    pub async fn delete_schema(&self, table: &str) -> Result<(), Error> {
        let _: serde_json::Value = self.req(&self.subj("schema.delete"), &TableReq { table }).await?;
        Ok(())
    }

    /// Register a one-shot delayed write (NATS 2.14+ message scheduling).
    ///
    /// The NATS server persists the `value` and performs the write to `table`/`key`
    /// when the RFC 3339 UTC timestamp `at` arrives.  Requires the server to run
    /// NATS ≥ 2.14.
    ///
    /// # Example
    /// ```rust,no_run
    /// # async fn example(db: lattice_db_client::LatticeDb) -> Result<(), lattice_db_client::Error> {
    /// db.schedule_put("orders", "order-42", b"{\"status\":\"shipped\"}", "2026-06-01T09:00:00Z").await?;
    /// # Ok(()) }
    /// ```
    pub async fn schedule_put(&self, table: &str, key: &str, value: &[u8], at: &str) -> Result<(), Error> {
        let _: serde_json::Value = self.req(&self.subj("schedule_put"), &SchedulePutReqW {
            table,
            key,
            value: B64.encode(value),
            at,
            ttl_seconds: None,
        }).await?;
        Ok(())
    }

    /// Like [`schedule_put`][Self::schedule_put] but the written key also gets a TTL,
    /// so the key expires `ttl_seconds` after it is written.
    pub async fn schedule_put_with_ttl(&self, table: &str, key: &str, value: &[u8], at: &str, ttl_seconds: u64) -> Result<(), Error> {
        let _: serde_json::Value = self.req(&self.subj("schedule_put"), &SchedulePutReqW {
            table,
            key,
            value: B64.encode(value),
            at,
            ttl_seconds: Some(ttl_seconds),
        }).await?;
        Ok(())
    }

    // ── Internal ───────────────────────────────────────────────

    /// Build the full NATS subject for an operation.
    fn subj(&self, op: &str) -> String {
        format!("{}.{op}", self.instance)
    }

    /// Send a request and deserialize the response, checking for error.
    ///
    /// Injects `_auth` into every request when configured.
    async fn req<Q: Serialize, R: DeserializeOwned>(&self, subject: &str, payload: &Q) -> Result<R, Error> {
        // Serialize to Value so we can inject the auth field.
        let mut value = serde_json::to_value(payload).map_err(|e| Error::Json(e.to_string()))?;
        if let Some(obj) = value.as_object_mut() {
            if let Some(ref token) = self.auth_token {
                obj.insert("_auth".to_string(), serde_json::Value::String(token.clone()));
            }
            if let Some(table) = obj.get("table").and_then(|t| t.as_str()) {
                if let Some(min_revision) = self
                    .session_revisions
                    .borrow()
                    .get(table)
                    .copied()
                {
                    obj.insert(
                        "consistency".to_string(),
                        serde_json::json!({ "min_revision": min_revision }),
                    );
                }
            }
        }
        let body = serde_json::to_vec(&value).map_err(|e| Error::Json(e.to_string()))?;
        let reply = self.client.request(subject, &body, self.timeout).await?;

        // Check for ADR-32 service error headers first.
        if let Some(headers) = &reply.headers {
            if let Some(err_msg) = headers.get(nats_wasi::service::HEADER_ERROR_DESCRIPTION) {
                return Err(Error::Db(err_msg.to_string()));
            }
        }

        let reply_value: serde_json::Value =
            serde_json::from_slice(&reply.payload).map_err(|e| Error::Json(e.to_string()))?;

        // Check for error response in JSON body fallback.
        if let Ok(err) = serde_json::from_value::<ErrorR>(reply_value.clone()) {
            if let Some(msg) = err.error {
                return Err(Error::Db(msg));
            }
        }

        if let Some(obj) = reply_value.as_object() {
            if let Some(session_val) = obj.get("session") {
                if let Ok(session) = serde_json::from_value::<SessionR>(session_val.clone()) {
                    let mut local = self.session_revisions.borrow_mut();
                    for (table, revision) in session.revisions {
                        let entry = local.entry(table).or_insert(0);
                        if revision > *entry {
                            *entry = revision;
                        }
                    }
                }
            }

            // Backward-compatible fallback: infer from {table, revision} responses.
            if let (Some(table), Some(revision)) = (
                value
                    .as_object()
                    .and_then(|o| o.get("table"))
                    .and_then(|v| v.as_str()),
                obj.get("revision").and_then(|v| v.as_u64()),
            ) {
                let mut local = self.session_revisions.borrow_mut();
                let entry = local.entry(table.to_string()).or_insert(0);
                if revision > *entry {
                    *entry = revision;
                }
            }
        }

        serde_json::from_value(reply_value).map_err(|e| Error::Json(e.to_string()))
    }
}
