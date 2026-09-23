//! lattice-sql — SQL frontend for lattice-db.
//!
//! Connects to NATS, loads the SQL catalog from lattice-db, and serves
//! SQL queries on `{instance}.sql.>` subjects.

mod catalog;
mod executor;
mod handler;
mod join;
mod parser;
mod planner;

use lattice_db_client::LatticeDb;
use nats_wasi::client::{Client, ConnectConfig};

// ── wasip3 command entry point ─────────────────────────────────────

wasip3::cli::command::export!(LatticeSql);

struct LatticeSql;

impl wasip3::exports::cli::run::Guest for LatticeSql {
    async fn run() -> Result<(), ()> {
        if let Err(e) = run().await {
            eprintln!("fatal: {e}");
        }
        Ok(())
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let nats_addr = std::env::var("NATS_URL")
        .ok()
        .or_else(|| std::env::args().nth(1))
        .unwrap_or_else(|| "127.0.0.1:4222".to_string());

    let use_tls = std::env::var("NATS_TLS").map_or(false, |v| v == "1" || v == "true");

    // Instance name: must match the lattice-db storage-service deployment
    // this SQL frontend is paired with.
    let instance = std::env::var("LDB_INSTANCE").unwrap_or_else(|_| "ldb".to_string());

    eprintln!("lattice-sql: connecting to NATS at {nats_addr} (instance={instance})");

    let client = Client::connect(ConnectConfig {
        address: nats_addr.to_string(),
        name: Some("lattice-sql".to_string()),
        tls: use_tls,
        ..Default::default()
    })
    .await?;

    eprintln!(
        "lattice-sql: connected to {} ({})",
        client.server_info().server_name,
        client.server_info().version,
    );

    // Optional auth token configured for lattice-db (LDB-04).
    let auth_token: Option<String> = std::env::var("LDB_AUTH_TOKEN").ok();

    // Create lattice-db client (talks to storage-service over NATS).
    let mut db = LatticeDb::new(client.clone()).with_instance(instance.clone());
    if let Some(ref token) = auth_token {
        db = db.with_auth(token);
    }

    // Load SQL catalog from lattice-db.
    let catalog = catalog::Catalog::load(&db)
        .await
        .map_err(|e| format!("catalog load: {e}"))?;
    let tables = catalog.table_names();
    eprintln!(
        "lattice-sql: loaded catalog ({} table{})",
        tables.len(),
        if tables.len() == 1 { "" } else { "s" }
    );
    for t in &tables {
        eprintln!("  - {t}");
    }

    let shared_catalog = handler::new_shared_catalog(catalog);

    // Subscribe to SQL subjects (queue group for horizontal scaling).
    let sql_subject = format!("{instance}.sql.>");
    let sql_queue = format!("{instance}-sql-workers");
    let sub = client.subscribe_queue(&sql_subject, &sql_queue)?;

    eprintln!("lattice-sql: listening on {sql_subject} (queue group: {sql_queue})");

    loop {
        let msg = sub.next().await?;

        let client = client.clone();
        let mut db_handle = LatticeDb::new(client.clone()).with_instance(instance.clone());
        if let Some(ref token) = auth_token {
            db_handle = db_handle.with_auth(token);
        }
        let cat = shared_catalog.clone();
        let token_clone = auth_token.clone();
        wasip3::spawn(async move {
            handler::handle(&client, &db_handle, &cat, msg, token_clone.as_deref()).await;
        });
    }
}
