//! In-memory key-value cache.
//!
//! Each table keeps a `HashMap` of rows (keyed by primary key).
//! The in-memory cache is the fast-path for all reads; writes go through
//! NATS KV first, then update the cache on success.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

/// A cached row: value bytes + NATS KV revision.
///
/// TTL-based expiry is handled solely by the NATS server (per-message TTL via
/// the `Nats-TTL` header, requires NATS 2.11+ with `allow_msg_ttl: true`).
/// When a key expires the server emits `Operation::Delete` to every KV watcher,
/// so all replicas remove the key uniformly. Local expiry tracking would create
/// an inconsistency window where the originating replica hid a key that peers
/// were still serving.
#[derive(Clone, Debug)]
pub struct CachedRow {
    pub value: Vec<u8>,
    pub revision: u64,
}

/// Per-table in-memory state.
pub struct TableState {
    /// Primary data: key → cached row.
    pub data: HashMap<String, CachedRow>,
    /// Optional JSON schema for validation on writes.
    pub schema: Option<serde_json::Value>,
    /// Whether values in this table are stored encrypted (AES-256-GCM envelope).
    /// Derived from the schema `"encrypted": true` field and cached here so the
    /// hot write/read paths avoid a JSON lookup on every operation.
    pub encrypted: bool,
    /// Whether this table has been fully loaded from NATS KV.
    pub loaded: bool,
    /// Whether a load is currently in-flight (prevents concurrent loads).
    pub loading: bool,
    /// Whether a background KV watcher is running for this table.
    pub watching: bool,
    /// Highest KV revision known to be applied to this table state.
    pub applied_revision: u64,
}

impl TableState {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            schema: None,
            encrypted: false,
            loaded: false,
            loading: false,
            watching: false,
            applied_revision: 0,
        }
    }

    /// Insert or update a row in the cache.
    pub fn upsert(&mut self, key: &str, value: Vec<u8>, revision: u64) {
        self.data
            .insert(key.to_string(), CachedRow { value, revision });
        self.note_applied_revision(revision);
    }

    /// Remove a row from the cache.
    pub fn remove(&mut self, key: &str) {
        self.data.remove(key);
    }

    /// Track that this table has observed and applied at least `revision`.
    pub fn note_applied_revision(&mut self, revision: u64) {
        if revision > self.applied_revision {
            self.applied_revision = revision;
        }
    }
}

/// Global state: all tables.
pub struct State {
    pub tables: HashMap<String, TableState>,
}

impl State {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Get or create a table's state.
    pub fn table(&mut self, name: &str) -> &mut TableState {
        self.tables
            .entry(name.to_string())
            .or_insert_with(TableState::new)
    }

    /// Return true if the named table is marked encrypted.
    pub fn is_encrypted(&self, table: &str) -> bool {
        self.tables.get(table).map_or(false, |t| t.encrypted)
    }
}

/// Shared state handle (single-threaded, Rc<RefCell> is fine).
pub type SharedState = Rc<RefCell<State>>;

pub fn new_shared_state() -> SharedState {
    Rc::new(RefCell::new(State::new()))
}

/// Validate a JSON value against a schema definition.
pub fn validate_schema(data: &[u8], schema: &serde_json::Value) -> Result<(), String> {
    let obj: serde_json::Value =
        serde_json::from_slice(data).map_err(|e| format!("value is not valid JSON: {e}"))?;
    let serde_json::Value::Object(ref map) = obj else {
        return Err("value must be a JSON object".into());
    };
    let Some(fields) = schema.get("fields").and_then(|v| v.as_object()) else {
        return Ok(());
    };
    for (field_name, field_def) in fields {
        let required = field_def
            .get("required")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let Some(val) = map.get(field_name) else {
            if required {
                return Err(format!("missing required field: {field_name}"));
            }
            continue;
        };
        if let Some(expected_type) = field_def.get("type").and_then(|v| v.as_str()) {
            let actual_type = match val {
                serde_json::Value::String(_) => "string",
                serde_json::Value::Number(_) => "number",
                serde_json::Value::Bool(_) => "boolean",
                serde_json::Value::Array(_) => "array",
                serde_json::Value::Object(_) => "object",
                serde_json::Value::Null => "null",
            };
            if actual_type != expected_type {
                return Err(format!(
                    "field {field_name}: expected type {expected_type}, got {actual_type}"
                ));
            }
        }
    }
    Ok(())
}
