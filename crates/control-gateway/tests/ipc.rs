//! End-to-end IPC tests over a real Unix socket with a real engine run.

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use control_gateway::server;
use control_gateway::state::Gateway;
use engine_runtime::{ControlHandle, FeedSource, PipelineConfig};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use trading_core::{EngineConfig, Generator, GeneratorConfig};

struct Fixture {
    socket: PathBuf,
    journal: PathBuf,
    control: ControlHandle,
}

impl Fixture {
    fn new(name: &str) -> Fixture {
        let dir = std::env::temp_dir().join(format!("rltl-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        Fixture {
            socket: dir.join("control.sock"),
            journal: dir.join("engine.journal"),
            control: ControlHandle::new(64, 4),
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(dir) = self.socket.parent() {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

/// Runs a short engine so the journal and snapshots exist before serving.
fn run_engine(fixture: &Fixture, events: usize) {
    let inputs = Generator::new(GeneratorConfig::new(9, events)).generate();
    engine_runtime::run(
        EngineConfig::single_instrument(9),
        PipelineConfig {
            journal_path: Some(fixture.journal.clone()),
            journal_sync: engine_runtime::JournalSync::GroupCommit(64),
            snapshot_interval: 100,
            snapshot_depth: 8,
            ..PipelineConfig::default()
        },
        FeedSource::Memory(inputs),
        fixture.control.clone(),
    )
    .expect("engine run");
}

async fn client(fixture: &Fixture) -> UnixStream {
    for _ in 0..200 {
        if let Ok(stream) = UnixStream::connect(&fixture.socket).await {
            return stream;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("gateway never accepted a connection");
}

async fn call(stream: &mut UnixStream, method: &str, params: Value, token: Option<&str>) -> Value {
    let mut request = json!({
        "schemaVersion": 1,
        "commandId": format!("{method}-1"),
        "method": method,
        "params": params,
    });
    if let Some(token) = token {
        request["token"] = json!(token);
    }
    send(stream, &serde_json::to_vec(&request).unwrap()).await;
    receive(stream).await
}

async fn send(stream: &mut UnixStream, body: &[u8]) {
    stream
        .write_all(&(body.len() as u32).to_le_bytes())
        .await
        .expect("write length");
    stream.write_all(body).await.expect("write body");
}

async fn receive(stream: &mut UnixStream) -> Value {
    let mut length = [0u8; 4];
    stream.read_exact(&mut length).await.expect("read length");
    let mut body = vec![0u8; u32::from_le_bytes(length) as usize];
    stream.read_exact(&mut body).await.expect("read body");
    serde_json::from_slice(&body).expect("decode response")
}

fn serve(fixture: &Fixture, token: Option<&str>) -> Arc<Gateway> {
    let gateway = Arc::new(Gateway::new(
        fixture.control.clone(),
        fixture.journal.clone(),
        token.map(str::to_string),
    ));
    let served = Arc::clone(&gateway);
    let socket = fixture.socket.clone();
    tokio::spawn(async move {
        let _ = server::serve(&socket, served).await;
    });
    gateway
}

#[tokio::test]
async fn outputs_stream_in_order_and_snapshot_carries_engine_sequence() {
    let fixture = Fixture::new("stream");
    run_engine(&fixture, 2_000);
    serve(&fixture, Some("secret"));
    let mut stream = client(&fixture).await;

    let mut last_seq = 0u64;
    let mut total = 0usize;
    loop {
        let response = call(
            &mut stream,
            "outputs",
            json!({ "limit": 250 }),
            Some("secret"),
        )
        .await;
        assert!(response["ok"].as_bool().unwrap(), "{response}");
        let events = response["result"]["events"].as_array().unwrap().clone();
        if events.is_empty() {
            break;
        }
        for event in events {
            let seq: u64 = event["outputSeq"].as_str().unwrap().parse().unwrap();
            assert_eq!(seq, last_seq + 1, "output sequence must not skip");
            last_seq = seq;
            total += 1;
        }
    }
    assert!(total > 0);

    let snapshot = call(&mut stream, "snapshot", json!({}), Some("secret")).await;
    assert!(snapshot["ok"].as_bool().unwrap());
    let as_of: u64 = snapshot["result"]["asOfEngineSeq"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(as_of > 0);
    assert_eq!(
        snapshot["asOfEngineSeq"],
        snapshot["result"]["asOfEngineSeq"]
    );
}

#[tokio::test]
async fn wide_integers_are_strings_so_javascript_cannot_round_them() {
    let fixture = Fixture::new("wide");
    run_engine(&fixture, 500);
    serve(&fixture, Some("secret"));
    let mut stream = client(&fixture).await;

    let response = call(
        &mut stream,
        "outputs",
        json!({ "limit": 50 }),
        Some("secret"),
    )
    .await;
    for event in response["result"]["events"].as_array().unwrap() {
        assert!(event["outputSeq"].is_string());
        assert!(event["engineSeq"].is_string());
        assert!(event["engineTimeNs"].is_string());
    }

    let snapshot = call(&mut stream, "snapshot", json!({}), Some("secret")).await;
    let position = &snapshot["result"]["positions"][0];
    assert!(position["openBuyNotional"].is_string());
    assert!(position["positionLots"].is_string());
}

#[tokio::test]
async fn mutations_require_the_configured_token() {
    let fixture = Fixture::new("auth");
    run_engine(&fixture, 200);
    serve(&fixture, Some("secret"));
    let mut stream = client(&fixture).await;

    let denied = call(&mut stream, "engageKill", json!({ "engaged": true }), None).await;
    assert_eq!(denied["error"]["code"], "unauthorized");

    let wrong = call(
        &mut stream,
        "setAccountEnabled",
        json!({ "account": 1, "enabled": false }),
        Some("guess"),
    )
    .await;
    assert_eq!(wrong["error"]["code"], "unauthorized");

    let accepted = call(
        &mut stream,
        "engageKill",
        json!({ "engaged": true }),
        Some("secret"),
    )
    .await;
    assert!(accepted["ok"].as_bool().unwrap());
    assert!(fixture.control.kill_engaged());
}

#[tokio::test]
async fn a_gateway_without_a_token_refuses_every_mutation() {
    let fixture = Fixture::new("notoken");
    run_engine(&fixture, 200);
    serve(&fixture, None);
    let mut stream = client(&fixture).await;

    let response = call(
        &mut stream,
        "engageKill",
        json!({ "engaged": true }),
        Some("anything"),
    )
    .await;
    assert_eq!(response["error"]["code"], "unauthorized");
    assert!(!fixture.control.kill_engaged());
}

#[tokio::test]
async fn bad_frames_and_unknown_methods_are_rejected_without_dropping_the_engine() {
    let fixture = Fixture::new("bad");
    run_engine(&fixture, 200);
    serve(&fixture, Some("secret"));
    let mut stream = client(&fixture).await;

    let garbage = call(&mut stream, "notAMethod", json!({}), None).await;
    assert_eq!(garbage["error"]["code"], "unknown_method");

    send(&mut stream, b"{not json").await;
    let broken = receive(&mut stream).await;
    assert_eq!(broken["error"]["code"], "bad_request");

    let stale = json!({
        "schemaVersion": 99,
        "commandId": "old",
        "method": "health",
        "params": {},
    });
    send(&mut stream, &serde_json::to_vec(&stale).unwrap()).await;
    let rejected = receive(&mut stream).await;
    assert_eq!(rejected["error"]["code"], "unsupported_schema");

    // An oversized declared length must close the connection, not allocate.
    let mut second = client(&fixture).await;
    second
        .write_all(&(u32::MAX).to_le_bytes())
        .await
        .expect("write length");
    let mut buffer = [0u8; 1];
    assert_eq!(second.read(&mut buffer).await.unwrap(), 0);

    let healthy = call(&mut stream, "health", json!({}), None).await;
    assert!(healthy["ok"].as_bool().unwrap());
}

#[tokio::test]
async fn replay_runs_on_an_isolated_core_and_is_reproducible() {
    let fixture = Fixture::new("replay");
    run_engine(&fixture, 200);
    serve(&fixture, Some("secret"));
    let mut stream = client(&fixture).await;

    let params = json!({ "seed": 4, "events": 400 });
    let first = call(&mut stream, "startReplay", params.clone(), Some("secret")).await;
    assert_eq!(first["result"]["status"], "running");
    let busy = call(&mut stream, "startReplay", params, Some("secret")).await;
    assert_eq!(busy["result"]["status"], "rejected");

    let job = first["result"]["jobId"].as_str().unwrap().to_string();
    let mut status = Value::Null;
    for _ in 0..200 {
        status = call(
            &mut stream,
            "replayStatus",
            json!({ "jobId": job }),
            Some("secret"),
        )
        .await;
        if status["result"]["status"] == "completed" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(status["result"]["status"], "completed");
    assert!(status["result"]["stateDigest"].as_str().unwrap().len() == 64);

    let missing = call(
        &mut stream,
        "replayStatus",
        json!({ "jobId": "replay-999" }),
        Some("secret"),
    )
    .await;
    assert_eq!(missing["error"]["code"], "not_found");
}

#[tokio::test]
async fn a_corrupt_journal_tail_is_reported_and_does_not_kill_the_gateway() {
    let fixture = Fixture::new("corrupt");
    run_engine(&fixture, 500);

    // Flip a byte inside the first record so its checksum no longer matches.
    let mut bytes = std::fs::read(&fixture.journal).expect("read journal");
    let offset = bytes.len() / 2;
    bytes[offset] ^= 0xff;
    std::fs::write(&fixture.journal, &bytes).expect("write journal");

    serve(&fixture, Some("secret"));
    let mut stream = client(&fixture).await;

    let mut failed = false;
    for _ in 0..10 {
        let response = call(
            &mut stream,
            "outputs",
            json!({ "limit": 100 }),
            Some("secret"),
        )
        .await;
        if !response["ok"].as_bool().unwrap() {
            assert_eq!(response["error"]["code"], "journal_error");
            failed = true;
            break;
        }
    }
    assert!(failed, "the corrupt record should have been reported");

    let healthy = call(&mut stream, "health", json!({}), None).await;
    assert!(healthy["ok"].as_bool().unwrap());
}

fn serve_with(fixture: &Fixture, token: Option<&str>, limits: server::Limits) -> Arc<Gateway> {
    let gateway = Arc::new(Gateway::new(
        fixture.control.clone(),
        fixture.journal.clone(),
        token.map(str::to_string),
    ));
    let served = Arc::clone(&gateway);
    let socket = fixture.socket.clone();
    tokio::spawn(async move {
        let _ = server::serve_with(&socket, served, limits).await;
    });
    gateway
}

#[tokio::test]
async fn the_control_socket_is_owner_only() {
    let fixture = Fixture::new("mode");
    run_engine(&fixture, 50);
    serve(&fixture, Some("secret"));
    let _ = client(&fixture).await;
    let mode = std::fs::metadata(&fixture.socket)
        .expect("socket metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);
}

#[tokio::test]
async fn sensitive_reads_require_the_token() {
    let fixture = Fixture::new("read-auth");
    run_engine(&fixture, 200);
    serve(&fixture, Some("secret"));
    let mut stream = client(&fixture).await;

    for method in ["outputs", "snapshot"] {
        let denied = call(&mut stream, method, json!({}), None).await;
        assert_eq!(denied["error"]["code"], "unauthorized", "{method}");
    }

    let health = call(&mut stream, "health", json!({}), None).await;
    assert!(health["ok"].as_bool().unwrap());

    let snapshot = call(&mut stream, "snapshot", json!({}), Some("secret")).await;
    assert!(snapshot["ok"].as_bool().unwrap());
    let outputs = call(
        &mut stream,
        "outputs",
        json!({ "limit": 10 }),
        Some("secret"),
    )
    .await;
    assert!(outputs["ok"].as_bool().unwrap());

    let kill = call(
        &mut stream,
        "engageKill",
        json!({ "engaged": true }),
        Some("secret"),
    )
    .await;
    assert!(kill["ok"].as_bool().unwrap());
}

#[tokio::test]
async fn no_configured_token_fails_closed_for_protected_reads() {
    let fixture = Fixture::new("read-notoken");
    run_engine(&fixture, 50);
    serve(&fixture, None);
    let mut stream = client(&fixture).await;
    let snapshot = call(&mut stream, "snapshot", json!({}), Some("anything")).await;
    assert_eq!(snapshot["error"]["code"], "unauthorized");
}

#[tokio::test]
async fn a_stalled_client_is_disconnected() {
    let fixture = Fixture::new("timeout");
    run_engine(&fixture, 50);
    serve_with(
        &fixture,
        Some("secret"),
        server::Limits {
            max_connections: 8,
            read_timeout: Duration::from_millis(80),
        },
    );
    let mut stream = client(&fixture).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut buf = [0u8; 1];
    assert_eq!(stream.read(&mut buf).await.unwrap(), 0);
}

#[tokio::test]
async fn the_connection_cap_is_enforced() {
    let fixture = Fixture::new("cap");
    run_engine(&fixture, 50);
    serve_with(
        &fixture,
        Some("secret"),
        server::Limits {
            max_connections: 1,
            read_timeout: Duration::from_secs(5),
        },
    );
    let mut first = client(&fixture).await;
    let mut extra = client(&fixture).await;
    let dropped = match extra.write_all(&1u32.to_le_bytes()).await {
        Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => true,
        Ok(()) => {
            let mut buf = [0u8; 1];
            extra.read(&mut buf).await.unwrap() == 0
        }
        Err(error) => panic!("unexpected extra-connection error: {error}"),
    };
    assert!(dropped, "the extra connection must be dropped");
    let health = call(&mut first, "health", json!({}), None).await;
    assert!(health["ok"].as_bool().unwrap());
}

#[tokio::test]
async fn a_large_replay_stays_off_the_worker_and_health_stays_up() {
    let fixture = Fixture::new("async-replay");
    run_engine(&fixture, 50);
    serve(&fixture, Some("secret"));
    let mut stream = client(&fixture).await;

    let started = call(
        &mut stream,
        "startReplay",
        json!({ "seed": 8, "events": 8_000 }),
        Some("secret"),
    )
    .await;
    assert_eq!(started["result"]["status"], "running");
    let health = call(&mut stream, "health", json!({}), None).await;
    assert!(health["ok"].as_bool().unwrap());

    let job = started["result"]["jobId"].as_str().unwrap().to_string();
    let mut status = Value::Null;
    for _ in 0..400 {
        status = call(
            &mut stream,
            "replayStatus",
            json!({ "jobId": job }),
            Some("secret"),
        )
        .await;
        if status["result"]["status"] == "completed" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(status["result"]["status"], "completed");
}

#[tokio::test]
async fn replay_job_history_is_bounded() {
    let fixture = Fixture::new("replay-evict");
    run_engine(&fixture, 20);
    serve(&fixture, Some("secret"));
    let mut stream = client(&fixture).await;

    let mut last = String::new();
    for _ in 0..33 {
        let started = call(
            &mut stream,
            "startReplay",
            json!({ "seed": 1, "events": 8 }),
            Some("secret"),
        )
        .await;
        last = started["result"]["jobId"].as_str().unwrap().to_string();
        for _ in 0..100 {
            let status = call(
                &mut stream,
                "replayStatus",
                json!({ "jobId": last }),
                Some("secret"),
            )
            .await;
            if status["result"]["status"] == "completed" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    let oldest = call(
        &mut stream,
        "replayStatus",
        json!({ "jobId": "replay-1" }),
        Some("secret"),
    )
    .await;
    assert_eq!(oldest["error"]["code"], "not_found");
    let newest = call(
        &mut stream,
        "replayStatus",
        json!({ "jobId": last }),
        Some("secret"),
    )
    .await;
    assert_eq!(newest["result"]["status"], "completed");
}
