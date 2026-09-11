//! Length-prefixed JSON over a local Unix socket.
//!
//! Tokio lives here and nowhere near matching. Frames are bounded before they
//! are parsed and every mutation requires the configured token.

use std::path::Path;
use std::sync::Arc;

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

use crate::state::Gateway;
use crate::wire::{
    AccountEnabledParams, KillParams, OutputsParams, ReplayParams, Request, Response,
    RiskLimitsParams, MAX_FRAME_BYTES, SCHEMA_VERSION,
};

pub async fn serve(path: &Path, gateway: Arc<Gateway>) -> std::io::Result<()> {
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    let listener = UnixListener::bind(path)?;
    println!("control gateway listening on {}", path.display());

    loop {
        let (stream, _) = listener.accept().await?;
        let gateway = Arc::clone(&gateway);
        tokio::spawn(async move {
            if let Err(error) = handle(stream, gateway).await {
                eprintln!("control connection ended: {error}");
            }
        });
    }
}

async fn handle(mut stream: UnixStream, gateway: Arc<Gateway>) -> std::io::Result<()> {
    let mut length = [0u8; 4];
    loop {
        if stream.read_exact(&mut length).await.is_err() {
            return Ok(());
        }
        let len = u32::from_le_bytes(length) as usize;
        if len == 0 || len > MAX_FRAME_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "frame length out of bounds",
            ));
        }
        let mut body = vec![0u8; len];
        stream.read_exact(&mut body).await?;

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

    let mutating = matches!(
        request.method.as_str(),
        "setRiskLimits" | "setAccountEnabled" | "engageKill" | "startReplay"
    );
    if mutating {
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
