//! Rust benchmark for lattice-db — measures real service throughput with
//! persistent connections (no per-request fork/TLS overhead).
//!
//! Four scenarios match the shell bench.sh but give meaningful numbers:
//!
//!   1. Reads  — `ldb.get` on a hot key (in-memory cache fast path).
//!   2. Writes — `ldb.put` to a single table (one JetStream leader).
//!   3. Multi-table writes — `ldb.put` spread across N tables.
//!   4. Transactions — `ldb.txn` (2-op) through the global WAL stream.
//!
//! Build & run (plain NATS):
//!   cargo build --target wasm32-wasip3 --example bench
//!   wasmtime run -S p3=y -S inherit-network=y \
//!     --env NATS_URL=127.0.0.1:4222 \
//!     target/wasm32-wasip3/debug/examples/bench.wasm
//!
//! With Kind/mTLS (use the nodeport NATS address):
//!   wasmtime run -S p3=y -S inherit-network=y \
//!     --env NATS_URL=nats:4222 \
//!     --env NATS_TLS=1 \
//!     target/wasm32-wasip3/debug/examples/bench.wasm
//!
//! Tunables (env vars):
//!   BENCH_DURATION_SECS    seconds per scenario           (default: 10)
//!   BENCH_CONCURRENCY      in-flight requests at once     (default: 64)
//!   BENCH_MSG_SIZE         value payload bytes            (default: 256)
//!   BENCH_TABLES           tables for multi-table write   (default: 8)
//!   BENCH_TXN_OPS          ops per transaction            (default: 2)

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use jetcache_client::{JetCache as LatticeDb, TxnOp};
use nats_wasi::client::{Client, ConnectConfig};
use std::pin::Pin;

wasip3::cli::command::export!(Bench);
struct Bench;

impl wasip3::exports::cli::run::Guest for Bench {
    async fn run() -> Result<(), ()> {
        if let Err(e) = run_bench().await {
            eprintln!("bench error: {e}");
            return Err(());
        }
        Ok(())
    }
}

// ── Config ───────────────────────────────────────────────────────────

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct Config {
    duration_ns: u64,
    concurrency: usize,
    txn_concurrency: usize,
    msg_size: usize,
    tables: usize,
    txn_ops: usize,
}

impl Config {
    fn from_env() -> Self {
        let secs = env_u64("BENCH_DURATION_SECS", 10);
        let concurrency = env_usize("BENCH_CONCURRENCY", 64);
        Self {
            duration_ns: secs * 1_000_000_000,
            concurrency,
            // WAL serialises through one stream leader; keep txn concurrency
            // much lower than general concurrency to avoid timeout storms.
            txn_concurrency: env_usize("BENCH_TXN_CONCURRENCY", 8),
            msg_size: env_usize("BENCH_MSG_SIZE", 256),
            tables: env_usize("BENCH_TABLES", 8),
            txn_ops: env_usize("BENCH_TXN_OPS", 2),
        }
    }
    fn duration_secs(&self) -> u64 {
        self.duration_ns / 1_000_000_000
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

fn now_ns() -> u64 {
    wasip3::clocks::monotonic_clock::now()
}

/// Build a deterministic value payload of `size` bytes.
fn make_value(size: usize, seed: usize) -> Vec<u8> {
    let mut state = (seed as u32) ^ 0xdeadbeefu32;
    (0..size)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            (state & 0xff) as u8
        })
        .collect()
}

/// Run N concurrent requests for `duration_ns` nanoseconds.
/// Returns (total_requests, sorted_latencies_ns).
async fn run_scenario<F>(
    concurrency: usize,
    duration_ns: u64,
    mut make_req: F,
) -> (u64, Vec<u64>)
where
    F: FnMut(u64) -> Pin<Box<dyn std::future::Future<Output = Result<(), Box<dyn std::error::Error>>>>>,
{
    let deadline = now_ns() + duration_ns;
    let mut inflight: FuturesUnordered<Pin<Box<dyn std::future::Future<Output = (u64, Result<(), Box<dyn std::error::Error>>)>>>> = FuturesUnordered::new();
    let mut counter: u64 = 0;
    let mut latencies: Vec<u64> = Vec::new();

    // Seed the pool.
    for _ in 0..concurrency {
        let t0 = now_ns();
        let fut = make_req(counter);
        inflight.push(Box::pin(async move { (t0, fut.await) }));
        counter += 1;
    }

    loop {
        match inflight.next().await {
            None => break,
            Some((t0, result)) => {
                let lat = now_ns().saturating_sub(t0);
                if result.is_ok() {
                    latencies.push(lat);
                }
                if now_ns() < deadline {
                    let t1 = now_ns();
                    let fut = make_req(counter);
                    inflight.push(Box::pin(async move { (t1, fut.await) }));
                    counter += 1;
                }
            }
        }
    }

    latencies.sort_unstable();
    (latencies.len() as u64, latencies)
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 * p) as usize).min(sorted.len() - 1);
    sorted[idx] as f64 / 1_000_000.0 // ns → ms
}

fn print_result(label: &str, total: u64, dur_secs: u64, latencies: &[u64]) {
    let rps = if dur_secs > 0 { total / dur_secs } else { total };
    let p50 = percentile(latencies, 0.50);
    let p95 = percentile(latencies, 0.95);
    let p99 = percentile(latencies, 0.99);
    println!("── {label}");
    println!("  total reqs : {total}");
    println!("  throughput : {rps} req/s");
    println!("  latency p50: {p50:.2} ms");
    println!("  latency p95: {p95:.2} ms");
    println!("  latency p99: {p99:.2} ms");
    println!();
}

// ── Main ─────────────────────────────────────────────────────────────

async fn run_bench() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = Config::from_env();

    let nats_url = std::env::var("NATS_URL")
        .unwrap_or_else(|_| "127.0.0.1:4222".to_string());
    let use_tls = std::env::var("NATS_TLS")
        .map_or(false, |v| v == "1" || v == "true");

    println!("── lattice-db Rust benchmark ────────────────────────────────────");
    println!("  NATS URL    : {nats_url}");
    println!("  TLS         : {use_tls}");
    println!("  duration    : {}s per scenario", cfg.duration_secs());
    println!("  concurrency : {}", cfg.concurrency);
    println!("  msg size    : {} B", cfg.msg_size);
    println!("  multi-table : {} tables", cfg.tables);
    println!("  txn ops     : {}", cfg.txn_ops);    println!("  txn concurr : {} (BENCH_TXN_CONCURRENCY)", cfg.txn_concurrency);    println!();

    // One shared NATS connection — persistent, TLS handshake done once.
    let client = Client::connect(ConnectConfig {
        address: nats_url.clone(),
        name: Some("lattice-db-bench".to_string()),
        tls: use_tls,
        ..Default::default()
    })
    .await?;

    let run_id = now_ns();
    let db = std::rc::Rc::new(LatticeDb::new(client));

    // ── Scenario 1: reads (cache-hit fast path) ─────────────────────
    // Pre-load one key so the service caches the table, then hammer get.

    let read_table = format!("bench_read_{run_id}");
    let seed_value = make_value(cfg.msg_size, 0);
    db.put(&read_table, "hot", &seed_value).await?;
    // A second put forces the watcher to be established before we start timing.
    db.put(&read_table, "warm", &seed_value).await?;

    {
        let db = db.clone();
        let table = read_table.clone();
        let (total, lats) = run_scenario(cfg.concurrency, cfg.duration_ns, |_i| {
            let db = db.clone();
            let table = table.clone();
            Box::pin(async move {
                db.get(&table, "hot").await?;
                Ok(())
            })
        })
        .await;
        print_result("1. Reads (cache hit, single hot key)", total, cfg.duration_secs(), &lats);
    }

    // ── Scenario 2: writes to a single table ────────────────────────

    let write_table = format!("bench_write_single_{run_id}");
    {
        let db = db.clone();
        let table = write_table.clone();
        let msg_size = cfg.msg_size;
        let (total, lats) = run_scenario(cfg.concurrency, cfg.duration_ns, |i| {
            let db = db.clone();
            let table = table.clone();
            let value = make_value(msg_size, i as usize);
            Box::pin(async move {
                db.put(&table, &format!("k{i}"), &value).await?;
                Ok(())
            })
        })
        .await;
        print_result("2. Writes (single table, one JS leader)", total, cfg.duration_secs(), &lats);
    }

    // ── Scenario 3: writes spread across N tables ───────────────────

    let ntables = cfg.tables;
    let multi_prefix = format!("bench_write_multi_{run_id}");
    {
        let db = db.clone();
        let prefix = multi_prefix.clone();
        let msg_size = cfg.msg_size;
        let (total, lats) = run_scenario(cfg.concurrency, cfg.duration_ns, |i| {
            let db = db.clone();
            let table = format!("{}_{}", prefix, i as usize % ntables);
            let value = make_value(msg_size, i as usize);
            Box::pin(async move {
                db.put(&table, &format!("k{i}"), &value).await?;
                Ok(())
            })
        })
        .await;
        print_result(
            &format!("3. Writes ({ntables} tables, parallel JS leaders)"),
            total,
            cfg.duration_secs(),
            &lats,
        );
    }

    // ── Scenario 4: transactions ────────────────────────────────────

    let txn_table = format!("bench_txn_{run_id}");
    let txn_ops = cfg.txn_ops;

    // Pre-warm: one plain put to create the KV bucket and populate the cache
    // before any txn fires. Without this, all txn_concurrency goroutines race
    // to create the bucket simultaneously on the first request, most fail, and
    // the WAL fills with orphaned PREPAREs before the bench even starts.
    db.put(&txn_table, "warmup", &make_value(cfg.msg_size, 0)).await?;
    {
        let db = db.clone();
        let table = txn_table.clone();
        let msg_size = cfg.msg_size;
        let (total, lats) = run_scenario(cfg.txn_concurrency, cfg.duration_ns, |i| {
            let db = db.clone();
            let table = table.clone();
            let value = make_value(msg_size, i as usize);
            Box::pin(async move {
                let ops: Vec<TxnOp> = (0..txn_ops)
                    .map(|j| TxnOp::put(&table, &format!("k{i}_{j}"), &value))
                    .collect();
                db.transaction(ops).await?;
                Ok(())
            })
        })
        .await;
        print_result(
            &format!("4. Transactions ({txn_ops}-op, global WAL)"),
            total,
            cfg.duration_secs(),
            &lats,
        );
    }

    println!("── done ─────────────────────────────────────────────────────────");
    println!("Bench tables (delete with: nats kv del <name> --force):");
    println!("  ldb-{read_table}");
    println!("  ldb-{write_table}");
    println!("  ldb-{txn_table}");
    for t in 0..ntables {
        println!("  ldb-{multi_prefix}_{t}");
    }

    Ok(())
}
