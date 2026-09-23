//! TCP server for localhost (workload-internal) access.
//!
//! Listens on `127.0.0.1:PORT` and accepts length-prefixed JSON frames.
//! Wire protocol:
//!   Request:  [4 bytes: u32 BE payload length] [JSON payload with `_op` field]
//!   Response: [4 bytes: u32 BE payload length] [JSON payload]
//!
//! This allows co-located components to bypass NATS and talk directly to
//! the in-memory cache via virtual pipes (sub-millisecond latency).

use wasip3::sockets::types::{IpAddressFamily, IpSocketAddress, Ipv4SocketAddress, TcpSocket};
use wasip3::wit_bindgen::{StreamReader, StreamResult};
use wasip3::wit_stream;

use crate::handler::{self, SharedConfig};
use crate::state::SharedState;
use crate::store::SharedStore;
use nats_wasi::client::Client;
use nats_wasi::jetstream::JetStream;

/// Default TCP port for the localhost listener.
const DEFAULT_PORT: u16 = 4080;

/// Maximum allowed frame length over TCP (~1.06 MiB). Prevents unbounded allocation (LDB-06).
const MAX_TCP_FRAME_LEN: usize = crate::handler::MAX_VALUE_BYTES + 64 * 1024;

/// Start the TCP listener. Spawns a background task that never returns.
pub fn start(
    client: Client,
    js: JetStream,
    config: SharedConfig,
    state: SharedState,
    store: SharedStore,
) {
    let port = std::env::var("LDB_TCP_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(DEFAULT_PORT);

    wasip3::spawn(async move {
        if let Err(e) = run_listener(port, client, js, config, state, store).await {
            eprintln!("lattice-db: tcp listener fatal: {e}");
        }
    });
}

async fn run_listener(
    port: u16,
    client: Client,
    js: JetStream,
    config: SharedConfig,
    state: SharedState,
    store: SharedStore,
) -> Result<(), String> {
    let socket =
        TcpSocket::create(IpAddressFamily::Ipv4).map_err(|e| format!("tcp create: {e:?}"))?;

    let addr = IpSocketAddress::Ipv4(Ipv4SocketAddress {
        port,
        address: (127, 0, 0, 1),
    });
    socket.bind(addr).map_err(|e| format!("tcp bind: {e:?}"))?;

    let mut incoming: StreamReader<TcpSocket> =
        socket.listen().map_err(|e| format!("tcp listen: {e:?}"))?;
    std::mem::forget(socket);

    eprintln!("lattice-db: tcp listening on 127.0.0.1:{port}");

    loop {
        let read_buf = Vec::with_capacity(16);
        let (status, sockets) = incoming.read(read_buf).await;
        match status {
            StreamResult::Complete(n) => {
                for conn in sockets.into_iter().take(n) {
                    let client = client.clone();
                    let js = js.clone();
                    let cfg = config.clone();
                    let st = state.clone();
                    let sto = store.clone();
                    handle_connection(conn, client, js, cfg, st, sto).await;
                }
            }
            StreamResult::Dropped | StreamResult::Cancelled => {
                return Err("listener stream closed".into());
            }
        }
    }
}

async fn handle_connection(
    conn: TcpSocket,
    client: Client,
    js: JetStream,
    config: SharedConfig,
    state: SharedState,
    store: SharedStore,
) {
    eprintln!("lattice-db: tcp connection accepted");
    let (mut rx, _rx_done) = conn.receive();

    let mut buf = Vec::new();

    // Read 4-byte length prefix.
    while buf.len() < 4 {
        if stream_read_u8(&mut rx, &mut buf).await == 0 {
            eprintln!("lattice-db: tcp connection closed before 4-byte length prefix");
            return;
        }
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    buf.drain(..4);

    if len > MAX_TCP_FRAME_LEN {
        eprintln!("lattice-db: tcp frame length {len} exceeds limit of {MAX_TCP_FRAME_LEN}");
        return;
    }

    // Read full payload.
    while buf.len() < len {
        if stream_read_u8(&mut rx, &mut buf).await == 0 {
            eprintln!("lattice-db: tcp connection closed mid-payload");
            return;
        }
    }
    let frame: Vec<u8> = buf.drain(..len).collect();

    // Dispatch the request.
    let (status_code, resp_bytes) = dispatch(&client, &js, &config, &state, &store, &frame).await;

    // Write framed response: [4 bytes BE total_len (2 + body_len)] [2 bytes BE status_code] [body]
    let total_len = (2 + resp_bytes.len()) as u32;
    let mut out_frame = Vec::with_capacity(4 + 2 + resp_bytes.len());
    out_frame.extend_from_slice(&total_len.to_be_bytes());
    out_frame.extend_from_slice(&status_code.to_be_bytes());
    out_frame.extend_from_slice(&resp_bytes);

    let (mut tx, tx_rx) = wit_stream::new::<u8>();
    let _send_fut = conn.send(tx_rx);
    std::mem::forget(_send_fut);

    tx.write_all(out_frame).await;
    drop(tx);
    drop(rx);
    drop(_rx_done);
    drop(conn);

    // Yield to the WASI host event loop to flush the send stream to the OS socket.
    wasip3::clocks::monotonic_clock::wait_for(1_000_000).await;
}

/// Dispatch a framed request. Supports both:
/// 1. Unified frame: [1 byte op_len] [op ASCII bytes] [JSON payload]
/// 2. JSON fallback: `{"_op": "...", ...}`
pub(crate) async fn dispatch(
    client: &Client,
    js: &JetStream,
    config: &SharedConfig,
    state: &SharedState,
    store: &SharedStore,
    frame: &[u8],
) -> (u16, Vec<u8>) {
    if frame.is_empty() {
        return (
            400,
            serde_json::to_vec(&serde_json::json!({"error": "empty request frame"}))
                .unwrap_or_default(),
        );
    }

    let (op, payload) = if frame[0] < 32 {
        // Framed command: [1-byte op_len][op][payload]
        let op_len = frame[0] as usize;
        if frame.len() < 1 + op_len {
            return (
                400,
                serde_json::to_vec(&serde_json::json!({"error": "truncated op header"}))
                    .unwrap_or_default(),
            );
        }
        let op = match std::str::from_utf8(&frame[1..1 + op_len]) {
            Ok(s) => s,
            Err(_) => {
                return (
                    400,
                    serde_json::to_vec(&serde_json::json!({"error": "invalid op encoding"}))
                        .unwrap_or_default(),
                );
            }
        };
        (op.to_string(), &frame[1 + op_len..])
    } else {
        // Fallback: parse `_op` field from the JSON payload.
        let val: serde_json::Value = match serde_json::from_slice(frame) {
            Ok(v) => v,
            Err(e) => {
                return (
                    400,
                    serde_json::to_vec(&serde_json::json!({"error": format!("parse: {e}")}))
                        .unwrap_or_default(),
                );
            }
        };

        let op = match val.get("_op").and_then(|v| v.as_str()) {
            Some(op) => op.to_string(),
            None => {
                return (
                    400,
                    serde_json::to_vec(&serde_json::json!({"error": "missing _op field"}))
                        .unwrap_or_default(),
                );
            }
        };
        (op, frame)
    };

    let result =
        handler::dispatch_operation(client, js, config, state, store, &op, payload).await;

    match result {
        Ok(json) => (0, json),
        Err((code, err_msg)) => (
            code,
            serde_json::to_vec(&serde_json::json!({"error": err_msg})).unwrap_or_default(),
        ),
    }
}

async fn stream_read_u8(rx: &mut StreamReader<u8>, buf: &mut Vec<u8>) -> usize {
    let read_buf = Vec::with_capacity(8192);
    let (status, data) = rx.read(read_buf).await;
    match status {
        StreamResult::Complete(n) => {
            buf.extend_from_slice(&data[..n]);
            n
        }
        StreamResult::Dropped | StreamResult::Cancelled => 0,
    }
}
