//! # lattice-sql-client
//!
//! Typed Rust SDK for [lattice-sql](https://github.com/Taika-3D-Oy/lattice-db) —
//! the SQL frontend for lattice-db.
//!
//! Sends SQL strings to the `ldb.sql.query` NATS subject and maps the JSON
//! responses back to strongly-typed Rust values.
//!
//! ## Quick start
//!
//! ```rust,no_run
//! use nats_wasi::client::{Client, ConnectConfig};
//! use lattice_sql_client::LatticeSql;
//!
//! # async fn example() -> Result<(), lattice_sql_client::Error> {
//! let client = Client::connect(ConnectConfig::default()).await?;
//! let db = LatticeSql::new(client)
//!     .with_auth("my-token"); // matches LDB_AUTH_TOKEN on the server
//!
//! // DDL — create a table
//! db.ddl("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL, age INTEGER)").await?;
//!
//! // DML — insert rows
//! let affected = db.exec("INSERT INTO users (id, name, age) VALUES ('alice', 'Alice', 30)").await?;
//! assert_eq!(affected, 1);
//!
//! // Query — SELECT with filters, sorting, pagination
//! let result = db.query("SELECT * FROM users WHERE age >= 25 ORDER BY name ASC LIMIT 10").await?;
//! println!("columns: {:?}", result.columns);
//! for row in &result.rows {
//!     println!("{:?}", row);
//! }
//!
//! // Lookup a cell by column name
//! if let Some(name) = result.cell(0, "name") {
//!     println!("first row name: {name}");
//! }
//!
//! // Aggregates
//! let agg = db.query("SELECT COUNT(*), SUM(age) FROM users").await?;
//! let count = &agg.rows[0][0]; // COUNT(*)
//!
//! // Cleanup
//! db.ddl("DROP TABLE users").await?;
//! # Ok(())
//! # }
//! ```

use nats_wasi::client::{Client, Duration, secs};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

// ── Error ──────────────────────────────────────────────────────────

/// Errors returned by the lattice-sql client.
#[derive(Debug)]
pub enum Error {
    /// NATS transport error (connection refused, timeout, etc.).
    Nats(nats_wasi::Error),
    /// lattice-sql returned an application-level error in its response.
    ///
    /// This includes SQL parse errors, missing tables, constraint violations,
    /// and any other error the service returns in `{"error":"..."}`.
    Db(String),
    /// JSON serialisation / deserialisation error.
    Json(String),
    /// A typed helper (`query`, `exec`, `ddl`) received the wrong response shape.
    ///
    /// For example, calling [`LatticeSql::query`] with an INSERT statement.
    WrongResultType(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Nats(e) => write!(f, "nats: {e}"),
            Error::Db(e) => write!(f, "lattice-sql: {e}"),
            Error::Json(e) => write!(f, "json: {e}"),
            Error::WrongResultType(e) => write!(f, "wrong result type: {e}"),
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

/// The result of a SELECT query.
///
/// Rows are ordered as returned by the engine. Each row is a `Vec` of
/// [`serde_json::Value`] aligned with [`columns`](QueryResult::columns).
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Column names in projection order.
    pub columns: Vec<String>,
    /// Rows as arrays of JSON values, one per projected column.
    pub rows: Vec<Vec<serde_json::Value>>,
}

impl QueryResult {
    /// Number of rows in the result set.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Find the zero-based index of a column by name.
    /// Returns `None` if the column is not in the projection.
    pub fn col_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c == name)
    }

    /// Get a single cell value by row index and column name.
    ///
    /// Returns `None` if the row index is out of bounds or the column name
    /// is not in the projection.
    pub fn cell(&self, row: usize, col: &str) -> Option<&serde_json::Value> {
        let col_idx = self.col_index(col)?;
        self.rows.get(row)?.get(col_idx)
    }

    /// Deserialise every row into a typed Rust struct.
    ///
    /// Each row is first converted to a JSON object keyed by column name,
    /// then deserialized via `serde_json`. Useful when the columns match a
    /// `#[derive(Deserialize)]` struct.
    ///
    /// ```rust,ignore
    /// #[derive(serde::Deserialize)]
    /// struct User { id: String, name: String, age: i64 }
    ///
    /// let users: Vec<User> = result.deserialize_rows().map_err(|e| Error::Json(e))?;
    /// ```
    pub fn deserialize_rows<T: DeserializeOwned>(&self) -> Result<Vec<T>, String> {
        self.rows
            .iter()
            .map(|row| {
                let mut map = serde_json::Map::new();
                for (i, col) in self.columns.iter().enumerate() {
                    map.insert(
                        col.clone(),
                        row.get(i).cloned().unwrap_or(serde_json::Value::Null),
                    );
                }
                serde_json::from_value(serde_json::Value::Object(map))
                    .map_err(|e| e.to_string())
            })
            .collect()
    }
}

/// The auto-detected result of any SQL statement sent via [`LatticeSql::sql`].
#[derive(Debug, Clone)]
pub enum SqlResult {
    /// A SELECT query returned columnar data.
    Query(QueryResult),
    /// An INSERT, UPDATE, DELETE, or DDL statement completed.
    Exec {
        /// Rows modified. Zero for DDL statements.
        affected_rows: u64,
    },
}

impl SqlResult {
    /// Unwrap as a [`QueryResult`], returning [`Error::WrongResultType`] if
    /// the statement was DML/DDL.
    pub fn into_query(self) -> Result<QueryResult, Error> {
        match self {
            SqlResult::Query(r) => Ok(r),
            SqlResult::Exec { .. } => Err(Error::WrongResultType(
                "expected SELECT result but got exec/DDL result".into(),
            )),
        }
    }

    /// Unwrap `affected_rows`, returning [`Error::WrongResultType`] if the
    /// statement was a SELECT.
    pub fn into_affected_rows(self) -> Result<u64, Error> {
        match self {
            SqlResult::Exec { affected_rows } => Ok(affected_rows),
            SqlResult::Query(_) => Err(Error::WrongResultType(
                "expected exec/DDL result but got SELECT result".into(),
            )),
        }
    }

    /// Returns `true` if this is a SELECT result.
    pub fn is_query(&self) -> bool {
        matches!(self, SqlResult::Query(_))
    }

    /// Returns `true` if this is a DML or DDL result.
    pub fn is_exec(&self) -> bool {
        matches!(self, SqlResult::Exec { .. })
    }
}

// ── Wire types (private) ───────────────────────────────────────────

/// Outgoing request payload.
#[derive(Serialize)]
struct SqlReq<'a> {
    sql: &'a str,
    #[serde(rename = "_auth", skip_serializing_if = "Option::is_none")]
    auth: Option<&'a str>,
}

/// Unified incoming response.
///
/// lattice-sql sends one of three JSON shapes depending on what the SQL did:
/// - `{"columns":[…],"rows":[[…],…]}` — SELECT result
/// - `{"affected_rows":N}` — DML/DDL result
/// - `{"error":"…"}` — any error
///
/// We deserialise into this single struct first, then dispatch.
#[derive(Deserialize)]
struct AnyResp {
    columns: Option<Vec<String>>,
    rows: Option<Vec<Vec<serde_json::Value>>>,
    affected_rows: Option<u64>,
    error: Option<String>,
}

// ── Client ─────────────────────────────────────────────────────────

/// Typed client for the lattice-sql SQL frontend.
///
/// All SQL statements are forwarded to [`SUBJECT`](LatticeSql::SUBJECT) as
/// `{"sql":"…"}` JSON payloads. The response is parsed and returned as a
/// strongly typed Rust value.
///
/// # Requirements
///
/// - A running NATS server (JetStream not required for SQL queries)
/// - `storage-service` running and listening on `ldb.*`
/// - `lattice-sql` running and subscribed to `ldb.sql.>`
///
/// # Example
///
/// ```rust,no_run
/// use lattice_sql_client::LatticeSql;
/// use nats_wasi::client::{Client, ConnectConfig};
///
/// # async fn example() -> Result<(), lattice_sql_client::Error> {
/// let client = Client::connect(ConnectConfig::default()).await?;
/// let db = LatticeSql::new(client);
/// db.ddl("CREATE TABLE items (id TEXT PRIMARY KEY, label TEXT)").await?;
/// # Ok(())
/// # }
/// ```
pub struct LatticeSql {
    client: Client,
    timeout: Duration,
    /// Value sent as `_auth` on every request. Must match `LDB_AUTH_TOKEN`.
    auth_token: Option<String>,
    /// NATS subject prefix. Must match `LDB_INSTANCE` on the server (default `ldb`).
    instance: String,
}

impl LatticeSql {
    /// Create a new client with the default 10-second timeout.
    ///
    /// The timeout is higher than `LatticeDb`'s default (5s) because a single
    /// SQL query may require several round-trips to the storage service.
    pub fn new(client: Client) -> Self {
        Self { client, timeout: secs(10), auth_token: None, instance: "ldb".to_string() }
    }

    /// Create a new client with a custom timeout (nanoseconds).
    pub fn with_timeout(client: Client, timeout: Duration) -> Self {
        Self { client, timeout, auth_token: None, instance: "ldb".to_string() }
    }

    /// Attach a shared auth token. Sent as `_auth` in every request.
    ///
    /// Required when the server is configured with `LDB_AUTH_TOKEN`.
    pub fn with_auth(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Set the instance prefix. Must match `LDB_INSTANCE` on the storage-service
    /// and lattice-sql deployments this client is targeting.
    ///
    /// Defaults to `"ldb"`.
    pub fn with_instance(mut self, instance: impl Into<String>) -> Self {
        self.instance = instance.into();
        self
    }

    /// Returns the NATS subject this client sends SQL requests to.
    pub fn subject(&self) -> String {
        format!("{}.sql.query", self.instance)
    }

    // ── Typed shortcuts ────────────────────────────────────────

    /// Execute a SELECT statement and return [`QueryResult`].
    ///
    /// Returns [`Error::WrongResultType`] if the SQL is not a SELECT.
    pub async fn query(&self, sql: &str) -> Result<QueryResult, Error> {
        self.sql(sql).await?.into_query()
    }

    /// Execute an INSERT, UPDATE, or DELETE and return the affected row count.
    ///
    /// Returns [`Error::WrongResultType`] if the SQL is a SELECT.
    pub async fn exec(&self, sql: &str) -> Result<u64, Error> {
        self.sql(sql).await?.into_affected_rows()
    }

    /// Execute a DDL statement (CREATE TABLE, DROP TABLE, CREATE INDEX, …).
    ///
    /// DDL always reports zero affected rows, so the count is discarded.
    /// Returns `()` on success, [`Error::Db`] if the DDL fails.
    pub async fn ddl(&self, sql: &str) -> Result<(), Error> {
        self.exec(sql).await?;
        Ok(())
    }

    // ── Auto-detect ────────────────────────────────────────────

    /// Send any SQL statement and return an auto-detected [`SqlResult`].
    ///
    /// Response shapes:
    /// - `{"columns":[…],"rows":[[…],…]}` → [`SqlResult::Query`]
    /// - `{"affected_rows":N}`             → [`SqlResult::Exec`]
    /// - `{"error":"…"}`                   → [`Error::Db`]
    pub async fn sql(&self, sql: &str) -> Result<SqlResult, Error> {
        let resp: AnyResp = self.send(sql).await?;

        if let Some(msg) = resp.error {
            return Err(Error::Db(msg));
        }

        // SELECT responses always carry a `columns` array.
        if let Some(columns) = resp.columns {
            return Ok(SqlResult::Query(QueryResult {
                columns,
                rows: resp.rows.unwrap_or_default(),
            }));
        }

        // DML / DDL responses carry `affected_rows` (may be 0 for DDL).
        Ok(SqlResult::Exec {
            affected_rows: resp.affected_rows.unwrap_or(0),
        })
    }

    // ── Internal ───────────────────────────────────────────────

    async fn send<R: DeserializeOwned>(&self, sql: &str) -> Result<R, Error> {
        let body =
            serde_json::to_vec(&SqlReq { sql, auth: self.auth_token.as_deref() })
                .map_err(|e| Error::Json(e.to_string()))?;
        let subject = self.subject();
        let reply = self
            .client
            .request(&subject, &body, self.timeout)
            .await?;
        serde_json::from_slice(&reply.payload).map_err(|e| Error::Json(e.to_string()))
    }
}
