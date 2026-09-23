//! NATS request handler — dispatches incoming SQL requests.
//!
//! Listens on:
//!   - `ldb.sql.query`  — SELECT statements → returns {columns, rows}
//!   - `ldb.sql.exec`   — INSERT/UPDATE/DELETE → returns {affected_rows}
//!   - `ldb.sql.ddl`    — CREATE/DROP TABLE/INDEX → returns {}
//!   - `ldb.sql`        — auto-detect statement type

use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::rc::Rc;

use lattice_db_client::LatticeDb;
use nats_wasi::client::{Client, Message};

use crate::catalog::Catalog;
use crate::executor::{self, ResultSet};
use crate::parser;
use crate::planner;

// ── Request / response types ───────────────────────────────────────

#[derive(Deserialize)]
struct SqlRequest {
    sql: String,
    #[serde(default)]
    _auth: Option<String>,
}

#[derive(Serialize)]
struct QueryResponse {
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
}

#[derive(Serialize)]
struct ExecResponse {
    affected_rows: u64,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

// ── Shared state ───────────────────────────────────────────────────

pub type SharedCatalog = Rc<RefCell<Catalog>>;

pub fn new_shared_catalog(catalog: Catalog) -> SharedCatalog {
    Rc::new(RefCell::new(catalog))
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

// ── Handler ────────────────────────────────────────────────────────

/// Handle a single incoming NATS message.
pub async fn handle(
    client: &Client,
    db: &LatticeDb,
    catalog: &SharedCatalog,
    msg: Message,
    expected_auth: Option<&str>,
) {
    let Some(reply_to) = msg.reply_to.as_deref() else {
        return; // no reply subject, nothing to respond to
    };

    // Parse the SQL request.
    let req: SqlRequest = match serde_json::from_slice(&msg.payload) {
        Ok(r) => r,
        Err(e) => {
            reply_error(client, reply_to, &format!("invalid request: {e}"));
            return;
        }
    };

    // Check auth if configured (LDB-04).
    if let Some(expected) = expected_auth {
        let provided = req._auth.as_deref().unwrap_or("");
        if !ct_eq(provided.as_bytes(), expected.as_bytes()) {
            reply_error(client, reply_to, "unauthorized");
            return;
        }
    }

    // Parse SQL.
    let stmt = match parser::parse(&req.sql) {
        Ok(s) => s,
        Err(e) => {
            reply_error(client, reply_to, &e);
            return;
        }
    };

    // Plan.
    let plan = {
        let cat = catalog.borrow();
        match planner::plan(stmt, &cat) {
            Ok(p) => p,
            Err(e) => {
                reply_error(client, reply_to, &e);
                return;
            }
        }
    };

    // Execute.
    let result = {
        let mut cat = catalog.borrow_mut();
        executor::execute(plan, db, &mut cat).await
    };

    match result {
        Ok(rs) => reply_result(client, reply_to, &rs),
        Err(e) => reply_error(client, reply_to, &e),
    }
}

fn reply_result(client: &Client, reply_to: &str, rs: &ResultSet) {
    // DDL statements or exec-type statements.
    if rs.columns.is_empty() && rs.rows.is_empty() {
        let resp = ExecResponse {
            affected_rows: rs.affected_rows,
        };
        let payload = serde_json::to_vec(&resp).unwrap_or_default();
        let _ = client.publish(reply_to, &payload);
        return;
    }

    // Query: return columns + rows.
    let resp = QueryResponse {
        columns: rs.columns.clone(),
        rows: rs.rows.clone(),
    };
    let payload = serde_json::to_vec(&resp).unwrap_or_default();
    let _ = client.publish(reply_to, &payload);
}

fn reply_error(client: &Client, reply_to: &str, error: &str) {
    let resp = ErrorResponse {
        error: error.to_string(),
    };
    let payload = serde_json::to_vec(&resp).unwrap_or_default();
    let _ = client.publish(reply_to, &payload);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ct_eq_equal() {
        assert!(ct_eq(b"secret-token", b"secret-token"));
        assert!(ct_eq(b"", b""));
    }

    #[test]
    fn test_ct_eq_different() {
        assert!(!ct_eq(b"secret-token", b"wrong-token!"));
        assert!(!ct_eq(b"secret", b"secrets"));
        assert!(!ct_eq(b"secret", b""));
    }
}
