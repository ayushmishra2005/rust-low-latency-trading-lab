//! Versioned IPC messages between the Rust gateway and the control plane.
//!
//! Every 64-bit and 128-bit value is encoded as a JSON string because
//! JavaScript numbers cannot represent them exactly.

use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: u32 = 1;
/// Refuse oversized frames before parsing them.
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub schema_version: u32,
    pub command_id: String,
    pub method: String,
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub params: serde_json::Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    pub schema_version: u32,
    pub command_id: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorBody>,
    /// Engine sequence the gateway had observed when it answered.
    pub as_of_engine_seq: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorBody {
    pub code: &'static str,
    pub message: String,
}

impl Response {
    pub fn ok(command_id: &str, result: serde_json::Value, engine_seq: u64) -> Response {
        Response {
            schema_version: SCHEMA_VERSION,
            command_id: command_id.to_string(),
            ok: true,
            result: Some(result),
            error: None,
            as_of_engine_seq: engine_seq.to_string(),
        }
    }

    pub fn failed(
        command_id: &str,
        code: &'static str,
        message: impl Into<String>,
        engine_seq: u64,
    ) -> Response {
        Response {
            schema_version: SCHEMA_VERSION,
            command_id: command_id.to_string(),
            ok: false,
            result: None,
            error: Some(ErrorBody {
                code,
                message: message.into(),
            }),
            as_of_engine_seq: engine_seq.to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OutputsParams {
    /// Numeric strings on the wire, parsed here.
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RiskLimitsParams {
    pub account: u32,
    pub max_order_quantity: String,
    pub max_order_notional: String,
    pub max_position_lots: String,
    pub max_gross_exposure: String,
    pub price_collar_ticks: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KillParams {
    pub engaged: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountEnabledParams {
    pub account: u32,
    pub enabled: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplayParams {
    pub seed: u64,
    pub events: usize,
}
