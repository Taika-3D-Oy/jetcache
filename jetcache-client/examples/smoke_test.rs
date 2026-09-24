//! Smoke tests for lattice-db-client SDK.
//!
//! Requires:
//!   - A NATS server running with JetStream (no TLS)
//!   - storage-service-p3 running against it
//!   - wasmtime ≥ 45
//!
//! Run:
//!   nats-server -js -p 14222 &
//!   wasmtime run ... storage_service_p3.wasm &
//!   cargo build --target wasm32-wasip3 --example smoke_test
//!   ~/.cargo/bin/wasmtime run \
//!     -S p3=y -S inherit-network=y \
//!     -W component-model=y -W component-model-async=y \
//!     --env NATS_URL=127.0.0.1:14222 \
//!     target/wasm32-wasip3/debug/examples/smoke_test.wasm

use jetcache_client::*;
use nats_wasi::client::{Client, ConnectConfig};

fn nats_address() -> String {
    std::env::var("NATS_URL").unwrap_or_else(|_| "127.0.0.1:4222".to_string())
}

async fn connect() -> LatticeDb {
    let client = Client::connect(ConnectConfig {
        address: nats_address(),
        ..Default::default()
    })
    .await
    .unwrap();
    LatticeDb::new(client)
}

// ── Test runner ────────────────────────────────────────────────────

wasip3::cli::command::export!(TestRunner);

struct TestRunner;

impl wasip3::exports::cli::run::Guest for TestRunner {
    async fn run() -> Result<(), ()> {
        run_tests().await;
        Ok(())
    }
}

macro_rules! run_test {
    ($name:expr, $body:expr) => {{
        let start = wasip3::clocks::monotonic_clock::now();
        print!("--- {}", $name);
        $body;
        let elapsed_ms = (wasip3::clocks::monotonic_clock::now() - start) / 1_000_000;
        println!("    PASS  ({elapsed_ms} ms)");
    }};
}

async fn run_tests() {
    let t0 = wasip3::clocks::monotonic_clock::now();
    let db = connect().await;

    // Use unique table names to avoid collisions with previous runs.
    let ts = wasip3::clocks::monotonic_clock::now();
    let table = format!("sdk-test-{ts}");

    // ── CRUD ───────────────────────────────────────────────────

    run_test!("put", {
        let rev = db.put(&table, "k1", b"{\"name\":\"Alice\",\"age\":30}").await.unwrap();
        assert!(rev > 0, "revision should be > 0");
    });

    run_test!("get", {
        let row = db.get(&table, "k1").await.unwrap();
        assert_eq!(row.key, "k1");
        assert!(row.revision > 0);
        let v: serde_json::Value = serde_json::from_slice(&row.value).unwrap();
        assert_eq!(v["name"], "Alice");
    });

    run_test!("get_json", {
        let v: serde_json::Value = db.get_json(&table, "k1").await.unwrap();
        assert_eq!(v["age"], 30);
    });

    run_test!("put_json", {
        let rev = db.put_json(&table, "k2", &serde_json::json!({"name": "Bob", "age": 25})).await.unwrap();
        assert!(rev > 0);
    });

    run_test!("exists true", {
        assert!(db.exists(&table, "k1").await.unwrap());
    });

    run_test!("exists false", {
        assert!(!db.exists(&table, "missing").await.unwrap());
    });

    run_test!("create", {
        let rev = db.create(&table, "k3", b"{\"name\":\"Charlie\",\"age\":35}").await.unwrap();
        assert!(rev > 0);
    });

    run_test!("create duplicate fails", {
        let err = db.create(&table, "k3", b"{}").await;
        assert!(err.is_err(), "duplicate create should fail");
    });

    run_test!("cas", {
        let row = db.get(&table, "k1").await.unwrap();
        let new_rev = db.cas(&table, "k1", b"{\"name\":\"Alice\",\"age\":31}", row.revision).await.unwrap();
        assert!(new_rev > row.revision);
    });

    run_test!("cas wrong revision fails", {
        let err = db.cas(&table, "k1", b"{}", 1).await;
        assert!(err.is_err(), "wrong revision CAS should fail");
    });

    run_test!("delete", {
        db.delete(&table, "k3").await.unwrap();
        assert!(!db.exists(&table, "k3").await.unwrap());
    });

    // ── Keys ───────────────────────────────────────────────────

    run_test!("keys", {
        let keys = db.keys(&table).await.unwrap();
        assert_eq!(keys.len(), 2); // k1, k2
        assert!(keys.contains(&"k1".to_string()));
        assert!(keys.contains(&"k2".to_string()));
    });

    // ── Scan ───────────────────────────────────────────────────

    run_test!("scan all", {
        let result = db.scan(&table, ScanQuery::new()).await.unwrap();
        assert_eq!(result.total_count, 2);
        assert_eq!(result.rows.len(), 2);
    });

    run_test!("scan with filter", {
        let result = db.scan(&table, ScanQuery::new().filter("name", "eq", "Bob")).await.unwrap();
        assert_eq!(result.total_count, 1);
        assert_eq!(result.rows[0].key, "k2");
    });

    run_test!("scan sorted", {
        let result = db.scan(&table, ScanQuery::new().order_by("name", "asc")).await.unwrap();
        assert_eq!(result.rows[0].key, "k1"); // Alice < Bob
    });

    run_test!("scan limit + offset", {
        let result = db.scan(&table, ScanQuery::new().order_by("name", "asc").limit(1).offset(1)).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].key, "k2"); // Bob
    });

    // ── Count ──────────────────────────────────────────────────

    run_test!("count all", {
        let n = db.count(&table, &[]).await.unwrap();
        assert_eq!(n, 2);
    });

    run_test!("count filtered", {
        let n = db.count(&table, &[Filter { field: "age".into(), op: "gte".into(), value: "30".into() }]).await.unwrap();
        assert_eq!(n, 1); // Alice age=31
    });

    // ── Batch ──────────────────────────────────────────────────

    run_test!("batch_put", {
        let results = db.batch_put(&table, &[
            BatchPutEntry { key: "b1", value: b"{\"x\":1}", ttl_seconds: None },
            BatchPutEntry { key: "b2", value: b"{\"x\":2}", ttl_seconds: None },
        ]).await.unwrap();
        assert_eq!(results.len(), 2);
        assert!(results[0].revision > 0);
    });

    run_test!("batch_get", {
        let results = db.batch_get(&table, &["b1", "b2", "missing"]).await.unwrap();
        assert_eq!(results.len(), 3);
        assert!(results[0].value.is_some());
        assert!(results[2].error.is_some()); // missing
    });

    // ── Indexes ────────────────────────────────────────────────

    run_test!("create_index", {
        db.create_index(&table, "name").await.unwrap();
    });

    run_test!("list_indexes", {
        let indexes = db.list_indexes(&table).await.unwrap();
        assert!(indexes.contains(&"name".to_string()));
    });

    run_test!("drop_index", {
        db.drop_index(&table, "name").await.unwrap();
        let indexes = db.list_indexes(&table).await.unwrap();
        assert!(!indexes.contains(&"name".to_string()));
    });

    // ── Transactions ───────────────────────────────────────────

    run_test!("transaction", {
        let result = db.transaction(vec![
            TxnOp::put(&table, "tx1", b"{\"val\":\"a\"}"),
            TxnOp::put(&table, "tx2", b"{\"val\":\"b\"}"),
        ]).await.unwrap();
        assert!(result.ok);
        assert_eq!(result.results.len(), 2);
    });

    run_test!("transaction values exist", {
        assert!(db.exists(&table, "tx1").await.unwrap());
        assert!(db.exists(&table, "tx2").await.unwrap());
    });

    // ── Aggregation ────────────────────────────────────────────

    // Set up data for aggregation
    let agg_table = format!("agg-{ts}");
    db.put_json(&agg_table, "a1", &serde_json::json!({"region":"north","amount":100})).await.unwrap();
    db.put_json(&agg_table, "a2", &serde_json::json!({"region":"south","amount":200})).await.unwrap();
    db.put_json(&agg_table, "a3", &serde_json::json!({"region":"south","amount":150})).await.unwrap();

    run_test!("aggregate count", {
        let groups = db.aggregate(&agg_table, &[], None, &[AggOp::count()]).await.unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].results[0].value, 3);
    });

    run_test!("aggregate sum", {
        let groups = db.aggregate(&agg_table, &[], None, &[AggOp::sum("amount")]).await.unwrap();
        let sum = groups[0].results[0].value.as_f64().unwrap();
        assert_eq!(sum, 450.0);
    });

    run_test!("aggregate group_by", {
        let groups = db.aggregate(&agg_table, &[], Some("region"), &[AggOp::count(), AggOp::sum("amount")]).await.unwrap();
        assert_eq!(groups.len(), 2); // north, south
    });

    // ── Schema ─────────────────────────────────────────────────

    let schema_table = format!("schema-{ts}");

    run_test!("set_schema", {
        let schema = serde_json::json!({
            "fields": {
                "name": {"type": "string", "required": true},
                "age": {"type": "number"}
            }
        });
        db.set_schema(&schema_table, &schema).await.unwrap();
    });

    run_test!("get_schema", {
        let schema = db.get_schema(&schema_table).await.unwrap();
        assert!(schema.is_some());
        assert!(schema.unwrap()["fields"]["name"]["required"].as_bool().unwrap());
    });

    run_test!("schema validates put", {
        let rev = db.put_json(&schema_table, "s1", &serde_json::json!({"name":"Test","age":10})).await.unwrap();
        assert!(rev > 0);
    });

    run_test!("schema rejects invalid", {
        let err = db.put_json(&schema_table, "s2", &serde_json::json!({"age":10})).await;
        assert!(err.is_err(), "missing required field should fail");
    });

    run_test!("delete_schema", {
        db.delete_schema(&schema_table).await.unwrap();
        assert!(db.get_schema(&schema_table).await.unwrap().is_none());
    });

    // ── TTL ────────────────────────────────────────────────────

    let ttl_table = format!("ttl-{ts}");

    run_test!("put_with_ttl", {
        let rev = db.put_with_ttl(&ttl_table, "ephemeral", b"{\"temp\":true}", 2).await.unwrap();
        assert!(rev > 0);
        assert!(db.exists(&ttl_table, "ephemeral").await.unwrap());
    });

    // ── Done ───────────────────────────────────────────────────

    let total_ms = (wasip3::clocks::monotonic_clock::now() - t0) / 1_000_000;
    println!("\nAll SDK tests passed! (total: {total_ms} ms)");
}
