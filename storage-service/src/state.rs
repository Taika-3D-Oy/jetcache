//! In-memory cache and secondary indexes.
//!
//! Each table keeps a `HashMap` of rows (keyed by primary key) and an optional
//! set of `BTreeMap` secondary indexes (keyed by JSON field value → list of
//! primary keys). The cache is the fast-path for all reads; writes go through
//! NATS KV first, then update the cache on success.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
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

/// A compound (multi-field) secondary index.
pub struct CompoundIndex {
    pub fields: Vec<String>,
    /// Composite key (field values joined by \x1f) → set of primary keys.
    pub data: BTreeMap<String, Vec<String>>,
}

/// Per-table in-memory state.
pub struct TableState {
    /// Primary data: key → cached row.
    pub data: HashMap<String, CachedRow>,
    /// Secondary indexes: field_name → (field_value → set of primary keys).
    pub indexes: HashMap<String, BTreeMap<String, Vec<String>>>,
    /// Compound (multi-field) secondary indexes: name → CompoundIndex.
    pub compound_indexes: HashMap<String, CompoundIndex>,
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
            indexes: HashMap::new(),
            compound_indexes: HashMap::new(),
            schema: None,
            encrypted: false,
            loaded: false,
            loading: false,
            watching: false,
            applied_revision: 0,
        }
    }

    /// Insert or update a row in the cache and maintain indexes.
    pub fn upsert(&mut self, key: &str, value: Vec<u8>, revision: u64) {
        // Remove old index entries if updating.
        if self.data.contains_key(key) {
            self.remove_from_indexes(key);
            self.remove_from_compound_indexes(key);
        }

        // Add to indexes.
        self.add_to_indexes(key, &value);
        self.add_to_compound_indexes(key, &value);

        self.data
            .insert(key.to_string(), CachedRow { value, revision });
        self.note_applied_revision(revision);
    }

    /// Remove a row from the cache and all indexes.
    pub fn remove(&mut self, key: &str) {
        self.remove_from_indexes(key);
        self.remove_from_compound_indexes(key);
        self.data.remove(key);
    }

    /// Track that this table has observed and applied at least `revision`.
    pub fn note_applied_revision(&mut self, revision: u64) {
        if revision > self.applied_revision {
            self.applied_revision = revision;
        }
    }

    /// Create a secondary index on a JSON field and backfill it.
    pub fn create_index(&mut self, field: &str) {
        if self.indexes.contains_key(field) {
            return;
        }
        let mut idx = BTreeMap::new();

        for (key, row) in &self.data {
            if let Some(val) = extract_json_field(&row.value, field) {
                idx.entry(val).or_insert_with(Vec::new).push(key.clone());
            }
        }

        self.indexes.insert(field.to_string(), idx);
    }

    /// Drop a secondary index.
    pub fn drop_index(&mut self, field: &str) {
        self.indexes.remove(field);
    }

    /// Create a compound (multi-field) index and backfill it.
    pub fn create_compound_index(&mut self, fields: &[String]) {
        let name = fields.join("+");
        if self.compound_indexes.contains_key(&name) {
            return;
        }
        let mut data = BTreeMap::new();
        for (key, row) in &self.data {
            if let Some(composite) = compound_index_key(&row.value, fields) {
                data.entry(composite)
                    .or_insert_with(Vec::new)
                    .push(key.clone());
            }
        }
        self.compound_indexes.insert(
            name,
            CompoundIndex {
                fields: fields.to_vec(),
                data,
            },
        );
    }

    /// Drop a compound index by name (e.g., "field1+field2").
    pub fn drop_compound_index(&mut self, name: &str) {
        self.compound_indexes.remove(name);
    }

    // ── Internal index maintenance ─────────────────────────────

    fn add_to_indexes(&mut self, key: &str, value: &[u8]) {
        for (field, idx) in &mut self.indexes {
            if let Some(val) = extract_json_field(value, field) {
                idx.entry(val)
                    .or_insert_with(Vec::new)
                    .push(key.to_string());
            }
        }
    }

    fn remove_from_indexes(&mut self, key: &str) {
        let old_value = match self.data.get(key) {
            Some(row) => row.value.clone(),
            None => return,
        };
        for (field, idx) in &mut self.indexes {
            if let Some(val) = extract_json_field(&old_value, field) {
                if let Some(keys) = idx.get_mut(&val) {
                    keys.retain(|k| k != key);
                }
            }
        }
    }

    fn add_to_compound_indexes(&mut self, key: &str, value: &[u8]) {
        for ci in self.compound_indexes.values_mut() {
            if let Some(composite) = compound_index_key(value, &ci.fields) {
                ci.data
                    .entry(composite)
                    .or_insert_with(Vec::new)
                    .push(key.to_string());
            }
        }
    }

    fn remove_from_compound_indexes(&mut self, key: &str) {
        let old_value = match self.data.get(key) {
            Some(row) => row.value.clone(),
            None => return,
        };
        for ci in self.compound_indexes.values_mut() {
            if let Some(composite) = compound_index_key(&old_value, &ci.fields) {
                if let Some(keys) = ci.data.get_mut(&composite) {
                    keys.retain(|k| k != key);
                }
            }
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

// ── JSON field extraction ──────────────────────────────────────────

/// Extract a top-level field value from a JSON byte slice as a string.
pub fn extract_json_field(data: &[u8], field: &str) -> Option<String> {
    let obj: serde_json::Value = serde_json::from_slice(data).ok()?;
    let val = obj.get(field)?;
    match val {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Build a composite index key from multiple fields.
pub fn compound_index_key(data: &[u8], fields: &[String]) -> Option<String> {
    let parts: Vec<String> = fields
        .iter()
        .map(|f| extract_json_field(data, f))
        .collect::<Option<Vec<_>>>()?;
    Some(parts.join("\x1f"))
}

/// Validate a JSON value against a schema definition.
/// Schemas without a `fields` map (e.g. encryption-only schemas) impose no
/// constraints on the value's shape.
pub fn validate_schema(data: &[u8], schema: &serde_json::Value) -> Result<(), String> {
    let Some(fields) = schema.get("fields").and_then(|v| v.as_object()) else {
        return Ok(());
    };
    let obj: serde_json::Value =
        serde_json::from_slice(data).map_err(|e| format!("value is not valid JSON: {e}"))?;
    let serde_json::Value::Object(ref map) = obj else {
        return Err("value must be a JSON object".into());
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

// ── Query helpers ──────────────────────────────────────────────────

/// Filter comparison operators.
#[derive(Debug, serde::Deserialize)]
#[serde(tag = "op", content = "value")]
pub enum Comparison {
    #[serde(rename = "eq")]
    Eq(String),
    #[serde(rename = "neq")]
    Neq(String),
    #[serde(rename = "gt")]
    Gt(String),
    #[serde(rename = "gte")]
    Gte(String),
    #[serde(rename = "lt")]
    Lt(String),
    #[serde(rename = "lte")]
    Lte(String),
    #[serde(rename = "prefix")]
    Prefix(String),
}

#[derive(Debug, serde::Deserialize)]
pub struct FieldFilter {
    pub field: String,
    #[serde(flatten)]
    pub cmp: Comparison,
}

fn cmp_match(actual: &str, cmp: &Comparison) -> bool {
    match cmp {
        Comparison::Eq(v) => actual == v,
        Comparison::Neq(v) => actual != v,
        Comparison::Gt(v) => actual > v.as_str(),
        Comparison::Gte(v) => actual >= v.as_str(),
        Comparison::Lt(v) => actual < v.as_str(),
        Comparison::Lte(v) => actual <= v.as_str(),
        Comparison::Prefix(v) => actual.starts_with(v.as_str()),
    }
}

/// Check if a row's value matches all filters.
pub fn matches_filters(value: &[u8], filters: &[FieldFilter]) -> bool {
    if filters.is_empty() {
        return true;
    }
    for f in filters {
        let Some(actual) = extract_json_field(value, &f.field) else {
            return false;
        };
        if !cmp_match(&actual, &f.cmp) {
            return false;
        }
    }
    true
}

/// Collect matching keys using an index if one covers the first filters.
/// Tries compound indexes first (if multiple filters), then single-field indexes.
pub fn index_scan(table: &TableState, filters: &[FieldFilter]) -> Option<Vec<String>> {
    if filters.is_empty() {
        return None;
    }

    // Try compound indexes if we have multiple filters.
    if filters.len() > 1 {
        for (idx_name, compound_idx) in &table.compound_indexes {
            // Check if this compound index covers the first N filters.
            let field_count = compound_idx.fields.len();
            if field_count <= filters.len()
                && filters[..field_count]
                    .iter()
                    .enumerate()
                    .all(|(i, f)| f.field == compound_idx.fields[i])
                && filters[..field_count]
                    .iter()
                    .all(|f| matches!(f.cmp, Comparison::Eq(_)))
            {
                // All filters up to field_count are Eq comparisons on the right fields.
                // Build the composite key and look it up.
                let mut parts = Vec::new();
                for (i, f) in filters[..field_count].iter().enumerate() {
                    if let Comparison::Eq(v) = &f.cmp {
                        parts.push(v.clone());
                    } else {
                        return None; // Should not happen given the check above
                    }
                }
                let composite_key = parts.join("\x1f");
                let mut keys = compound_idx
                    .data
                    .get(&composite_key)
                    .cloned()
                    .unwrap_or_default();

                // Filter the remaining filters post-index (if any).
                if field_count < filters.len() {
                    let remaining = &filters[field_count..];
                    keys.retain(|k| {
                        table
                            .data
                            .get(k)
                            .map_or(false, |row| matches_filters(&row.value, remaining))
                    });
                }

                return Some(keys);
            }
        }
    }

    // Fall back to single-field indexes.
    let first = filters.first()?;
    let idx = table.indexes.get(&first.field)?;

    let candidate_keys: Vec<String> = match &first.cmp {
        Comparison::Eq(v) => idx.get(v).cloned().unwrap_or_default(),
        Comparison::Prefix(v) => {
            let range = idx.range(v.clone()..);
            let mut keys = Vec::new();
            for (field_val, ks) in range {
                if !field_val.starts_with(v.as_str()) {
                    break;
                }
                keys.extend(ks.iter().cloned());
            }
            keys
        }
        Comparison::Gte(v) => idx
            .range(v.clone()..)
            .flat_map(|(_, ks)| ks.iter().cloned())
            .collect(),
        Comparison::Gt(v) => {
            let start = format!("{v}\0");
            idx.range(start..)
                .flat_map(|(_, ks)| ks.iter().cloned())
                .collect()
        }
        Comparison::Lt(v) => idx
            .range(..v.clone())
            .flat_map(|(_, ks)| ks.iter().cloned())
            .collect(),
        Comparison::Lte(v) => idx
            .range(..=v.clone())
            .flat_map(|(_, ks)| ks.iter().cloned())
            .collect(),
        Comparison::Neq(_) => return None,
    };

    if filters.len() <= 1 {
        return Some(candidate_keys);
    }
    let remaining = &filters[1..];
    let filtered = candidate_keys
        .into_iter()
        .filter(|k| {
            table
                .data
                .get(k)
                .map_or(false, |row| matches_filters(&row.value, remaining))
        })
        .collect();
    Some(filtered)
}

// ── Aggregation ────────────────────────────────────────────────────

pub struct AggOp {
    pub fn_name: String,
    pub field: Option<String>,
}

#[derive(serde::Serialize)]
pub struct AggResult {
    pub op: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    pub value: serde_json::Value,
}

#[derive(serde::Serialize)]
pub struct AggGroup {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub results: Vec<AggResult>,
}

pub fn aggregate(
    table: &TableState,
    filters: &[FieldFilter],
    group_by: Option<&str>,
    ops: &[AggOp],
) -> Vec<AggGroup> {
    let matching: Vec<(&String, &CachedRow)> = table
        .data
        .iter()
        .filter(|(_, row)| matches_filters(&row.value, filters))
        .collect();

    let mut groups: HashMap<Option<String>, Vec<(&String, &CachedRow)>> = HashMap::new();
    for (key, row) in &matching {
        let group_key = group_by.and_then(|f| extract_json_field(&row.value, f));
        groups.entry(group_key).or_default().push((key, row));
    }

    let mut result: Vec<AggGroup> = groups
        .into_iter()
        .map(|(group_key, rows)| {
            let results = ops.iter().map(|op| compute_agg(op, &rows)).collect();
            AggGroup {
                key: group_key,
                results,
            }
        })
        .collect();
    result.sort_by(|a, b| a.key.cmp(&b.key));
    result
}

fn compute_agg(op: &AggOp, rows: &[(&String, &CachedRow)]) -> AggResult {
    match op.fn_name.as_str() {
        "count" => AggResult {
            op: "count".into(),
            field: op.field.clone(),
            value: serde_json::Value::Number(serde_json::Number::from(rows.len() as u64)),
        },
        "sum" | "avg" => {
            let field = op.field.as_deref().unwrap_or("");
            let mut sum = 0.0f64;
            let mut count = 0u64;
            for (_, row) in rows {
                if let Some(val) = extract_json_field(&row.value, field) {
                    if let Ok(n) = val.parse::<f64>() {
                        sum += n;
                        count += 1;
                    }
                }
            }
            let result = if op.fn_name == "avg" && count > 0 {
                sum / count as f64
            } else {
                sum
            };
            AggResult {
                op: op.fn_name.clone(),
                field: op.field.clone(),
                value: serde_json::Number::from_f64(result)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null),
            }
        }
        "min" | "max" => {
            let field = op.field.as_deref().unwrap_or("");
            let mut best: Option<String> = None;
            for (_, row) in rows {
                if let Some(val) = extract_json_field(&row.value, field) {
                    best = Some(match best {
                        None => val,
                        Some(b) if op.fn_name == "min" && val < b => val,
                        Some(b) if op.fn_name == "max" && val > b => val,
                        Some(b) => b,
                    });
                }
            }
            AggResult {
                op: op.fn_name.clone(),
                field: op.field.clone(),
                value: best
                    .map(serde_json::Value::String)
                    .unwrap_or(serde_json::Value::Null),
            }
        }
        _ => AggResult {
            op: op.fn_name.clone(),
            field: op.field.clone(),
            value: serde_json::Value::Null,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::validate_schema;
    use serde_json::json;

    #[test]
    fn schema_without_fields_accepts_any_json_value() {
        let schema = json!({"encrypted": true});
        assert!(validate_schema(br#""plain string""#, &schema).is_ok());
        assert!(validate_schema(br#"["a","b"]"#, &schema).is_ok());
        assert!(validate_schema(br#"{"a":1}"#, &schema).is_ok());
        assert!(validate_schema(b"1", &schema).is_ok());
        assert!(validate_schema(b"not json", &schema).is_ok());
    }

    #[test]
    fn schema_with_fields_requires_object_and_enforces_rules() {
        let schema = json!({"fields": {"email": {"type": "string", "required": true}}});
        assert!(validate_schema(br#"{"email":"a@b.c"}"#, &schema).is_ok());
        assert!(validate_schema(br#""plain string""#, &schema).is_err());
        assert!(validate_schema(br#"{"other":1}"#, &schema).is_err());
        assert!(validate_schema(br#"{"email":42}"#, &schema).is_err());
    }
}
