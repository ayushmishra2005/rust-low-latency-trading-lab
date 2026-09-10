//! End-to-end IPC tests over a real Unix socket with a real engine run.

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
        let response = call(&mut stream, "outputs", json!({ "limit": 250 }), None).await;
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

    let snapshot = call(&mut stream, "snapshot", json!({}), None).await;
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

    let response = call(&mut stream, "outputs", json!({ "limit": 50 }), None).await;
    for event in response["result"]["events"].as_array().unwrap() {
        assert!(event["outputSeq"].is_string());
        assert!(event["engineSeq"].is_string());
        assert!(event["engineTimeNs"].is_string());
    }

    let snapshot = call(&mut stream, "snapshot", json!({}), None).await;
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
    let second = call(&mut stream, "startReplay", params, Some("secret")).await;
    assert_eq!(
        first["result"]["stateDigest"],
        second["result"]["stateDigest"]
    );

    let job = first["result"]["jobId"].as_str().unwrap().to_string();
    let status = call(&mut stream, "replayStatus", json!({ "jobId": job }), None).await;
    assert_eq!(status["result"]["status"], "completed");

    let missing = call(
        &mut stream,
        "replayStatus",
        json!({ "jobId": "replay-999" }),
        None,
    )
    .await;
    assert_eq!(missing["error"]["code"], "not_found");
}
