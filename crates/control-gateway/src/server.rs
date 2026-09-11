//! Length-prefixed JSON over a local Unix socket.
//!
//! Tokio lives here and nowhere near matching. Frames are bounded before they
//! are parsed and every mutation requires the configured token.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::time::timeout;

use crate::state::Gateway;
use crate::wire::{
    AccountEnabledParams, KillParams, OutputsParams, ReplayParams, Request, Response,
    RiskLimitsParams, MAX_FRAME_BYTES, SCHEMA_VERSION,
};

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_connections: usize,
    pub read_timeout: Duration,
}

impl Default for Limits {
    fn default() -> Limits {
        Limits {
            max_connections: 32,
            read_timeout: Duration::from_secs(15),
        }
    }
}

pub async fn serve(path: &Path, gateway: Arc<Gateway>) -> std::io::Result<()> {
    serve_with(path, gateway, Limits::default()).await
}

pub async fn serve_with(path: &Path, gateway: Arc<Gateway>, limits: Limits) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            std::fs::create_dir_all(parent)?;
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
        }
    }
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    let listener = UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    println!("control gateway listening on {}", path.display());

    let live = Arc::new(AtomicUsize::new(0));
    loop {
        let (stream, _) = listener.accept().await?;
        let current = live.fetch_add(1, Ordering::Relaxed);
        if current >= limits.max_connections {
            live.fetch_sub(1, Ordering::Relaxed);
            drop(stream);
            continue;
        }
        let gateway = Arc::clone(&gateway);
        let live = Arc::clone(&live);
        tokio::spawn(async move {
            if let Err(error) = handle(stream, gateway, limits.read_timeout).await {
                eprintln!("control connection ended: {error}");
            }
            live.fetch_sub(1, Ordering::Relaxed);
        });
    }
}

async fn handle(
    mut stream: UnixStream,
    gateway: Arc<Gateway>,
    read_timeout: Duration,
) -> std::io::Result<()> {
    let mut length = [0u8; 4];
    loop {
        match timeout(read_timeout, stream.read_exact(&mut length)).await {
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "control read timed out",
                ));
            }
            Ok(Err(_)) => return Ok(()),
            Ok(Ok(_)) => {}
        }
        let len = u32::from_le_bytes(length) as usize;
        if len == 0 || len > MAX_FRAME_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "frame length out of bounds",
            ));
        }
        let mut body = vec![0u8; len];
        timeout(read_timeout, stream.read_exact(&mut body))
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "control read timed out")
            })??;

        let mut response = match serde_json::from_slice::<Request>(&body) {
            // Journal reads and replay jobs block, so keep them off the reactor.
            Ok(request) => {
                let gateway = Arc::clone(&gateway);
                tokio::task::spawn_blocking(move || dispatch(&gateway, request))
                    .await
                    .expect("dispatch task")
            }
            Err(error) => Response::failed(
                "unknown",
                "bad_request",
                error.to_string(),
                gateway.as_of_engine_seq(),
            ),
        };

        // Report the sequence observed after the command was handled.
        response.as_of_engine_seq = gateway.as_of_engine_seq().to_string();

        let encoded = serde_json::to_vec(&response).expect("response serializes");
        stream
            .write_all(&(encoded.len() as u32).to_le_bytes())
            .await?;
        stream.write_all(&encoded).await?;
        stream.flush().await?;
    }
}

fn dispatch(gateway: &Gateway, request: Request) -> Response {
    let engine_seq = gateway.as_of_engine_seq();
    if request.schema_version != SCHEMA_VERSION {
        return Response::failed(
            &request.command_id,
            "unsupported_schema",
            format!("expected schema version {SCHEMA_VERSION}"),
            engine_seq,
        );
    }

    let protected = matches!(
        request.method.as_str(),
        "setRiskLimits"
            | "setAccountEnabled"
            | "engageKill"
            | "startReplay"
            | "snapshot"
            | "outputs"
            | "replayStatus"
            | "resetJournal"
    );
    if protected {
        if !gateway.requires_token() {
            return Response::failed(
                &request.command_id,
                "unauthorized",
                "gateway started without a control token; mutations are refused",
                engine_seq,
            );
        }
        if !gateway.authorized(request.token.as_deref()) {
            return Response::failed(
                &request.command_id,
                "unauthorized",
                "invalid control token",
                engine_seq,
            );
        }
    }

    match request.method.as_str() {
        "health" => Response::ok(&request.command_id, gateway.health(), engine_seq),
        "snapshot" => match gateway.snapshot_json() {
            Some(value) => Response::ok(&request.command_id, value, engine_seq),
            None => Response::failed(
                &request.command_id,
                "unavailable",
                "no engine snapshot yet",
                engine_seq,
            ),
        },
        "outputs" => {
            let params: OutputsParams =
                serde_json::from_value(request.params).unwrap_or(OutputsParams { limit: None });
            let limit = params.limit.unwrap_or(1_000).min(10_000) as usize;
            match gateway.next_outputs(limit) {
                Ok(events) => {
                    let payload: Vec<_> = events.iter().map(crate::json::output_event).collect();
                    Response::ok(
                        &request.command_id,
                        json!({ "events": payload }),
                        engine_seq,
                    )
                }
                Err(message) => {
                    Response::failed(&request.command_id, "journal_error", message, engine_seq)
                }
            }
        }
        "engageKill" => match serde_json::from_value::<KillParams>(request.params) {
            Ok(params) => match gateway.engage_kill(params.engaged) {
                Ok(()) => Response::ok(
                    &request.command_id,
                    json!({ "accepted": true, "engaged": params.engaged }),
                    engine_seq,
                ),
                Err(message) => {
                    Response::failed(&request.command_id, "rejected", message, engine_seq)
                }
            },
            Err(error) => Response::failed(
                &request.command_id,
                "bad_request",
                error.to_string(),
                engine_seq,
            ),
        },
        "setAccountEnabled" => match serde_json::from_value::<AccountEnabledParams>(request.params)
        {
            Ok(params) => match gateway.set_account_enabled(params.account, params.enabled) {
                Ok(()) => {
                    Response::ok(&request.command_id, json!({ "accepted": true }), engine_seq)
                }
                Err(message) => {
                    Response::failed(&request.command_id, "rejected", message, engine_seq)
                }
            },
            Err(error) => Response::failed(
                &request.command_id,
                "bad_request",
                error.to_string(),
                engine_seq,
            ),
        },
        "setRiskLimits" => match serde_json::from_value::<RiskLimitsParams>(request.params) {
            Ok(params) => {
                let parsed = (
                    params.max_order_quantity.parse::<u64>(),
                    params.max_order_notional.parse::<u128>(),
                    params.max_position_lots.parse::<u64>(),
                    params.max_gross_exposure.parse::<u128>(),
                );
                match parsed {
                    (Ok(quantity), Ok(notional), Ok(position), Ok(gross)) => {
                        match gateway.set_risk_limits(
                            params.account,
                            quantity,
                            notional,
                            position,
                            gross,
                            params.price_collar_ticks,
                        ) {
                            Ok(()) => Response::ok(
                                &request.command_id,
                                json!({ "accepted": true }),
                                engine_seq,
                            ),
                            Err(message) => Response::failed(
                                &request.command_id,
                                "rejected",
                                message,
                                engine_seq,
                            ),
                        }
                    }
                    _ => Response::failed(
                        &request.command_id,
                        "bad_request",
                        "limit values must be numeric strings",
                        engine_seq,
                    ),
                }
            }
            Err(error) => Response::failed(
                &request.command_id,
                "bad_request",
                error.to_string(),
                engine_seq,
            ),
        },
        "startReplay" => match serde_json::from_value::<ReplayParams>(request.params) {
            Ok(params) if params.events <= 1_000_000 => Response::ok(
                &request.command_id,
                gateway.start_replay(params.seed, params.events),
                engine_seq,
            ),
            Ok(_) => Response::failed(
                &request.command_id,
                "bad_request",
                "events exceeds the allowed replay bound",
                engine_seq,
            ),
            Err(error) => Response::failed(
                &request.command_id,
                "bad_request",
                error.to_string(),
                engine_seq,
            ),
        },
        "resetJournal" => {
            gateway.reset_journal_tail();
            Response::ok(&request.command_id, json!({ "accepted": true }), engine_seq)
        }
        "replayStatus" => {
            let job_id = request
                .params
                .get("jobId")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string();
            match gateway.replay_status(&job_id) {
                Some(value) => Response::ok(&request.command_id, value, engine_seq),
                None => Response::failed(
                    &request.command_id,
                    "not_found",
                    "unknown replay job",
                    engine_seq,
                ),
            }
        }
        other => Response::failed(
            &request.command_id,
            "unknown_method",
            format!("unknown method {other}"),
            engine_seq,
        ),
    }
}
