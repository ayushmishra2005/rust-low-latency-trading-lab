//! Gateway state. The gateway never owns trading state: it reads durable
//! output, caches the latest engine snapshot, and forwards control commands.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use engine_runtime::snapshot::EngineSnapshot;
use engine_runtime::{ControlHandle, JournalTail};
use protocol::{AccountId, AccountLimits, ControlCommand, Notional, OutputEvent, QuantityLots};
use serde_json::{json, Value};
use trading_core::replay::digest_hex;
use trading_core::{EngineConfig, Generator, GeneratorConfig, Replay, TradingCore};

pub struct Gateway {
    control: ControlHandle,
    /// Opened on the first read: the journal may not exist yet at startup.
    tail: Mutex<Option<JournalTail>>,
    journal_path: PathBuf,
    latest_snapshot: Mutex<Option<EngineSnapshot>>,
    token: Option<String>,
    started: Instant,
    delivered_outputs: AtomicU64,
    last_output_seq: AtomicU64,
    replay_jobs: Mutex<HashMap<String, Value>>,
    next_job: AtomicU64,
}

impl Gateway {
    pub fn new(control: ControlHandle, journal_path: PathBuf, token: Option<String>) -> Gateway {
        Gateway {
            control,
            tail: Mutex::new(None),
            journal_path,
            latest_snapshot: Mutex::new(None),
            token,
            started: Instant::now(),
            delivered_outputs: AtomicU64::new(0),
            last_output_seq: AtomicU64::new(0),
            replay_jobs: Mutex::new(HashMap::new()),
            next_job: AtomicU64::new(0),
        }
    }

    pub fn authorized(&self, token: Option<&str>) -> bool {
        match &self.token {
            None => false,
            Some(expected) => token == Some(expected.as_str()),
        }
    }

    pub fn requires_token(&self) -> bool {
        self.token.is_some()
    }

    /// Engine sequence the gateway last observed. Responses carry it so a
    /// reader can never mistake a projection for exact current state.
    pub fn as_of_engine_seq(&self) -> u64 {
        self.latest_snapshot
            .lock()
            .expect("snapshot mutex")
            .as_ref()
            .map(|snapshot| snapshot.as_of_engine_seq.0)
            .unwrap_or(0)
    }

    pub fn refresh_snapshot(&self) {
        while let Some(snapshot) = self.control.take_snapshot() {
            *self.latest_snapshot.lock().expect("snapshot mutex") = Some(snapshot);
        }
    }

    pub fn snapshot_json(&self) -> Option<Value> {
        self.refresh_snapshot();
        self.latest_snapshot
            .lock()
            .expect("snapshot mutex")
            .as_ref()
            .map(crate::json::snapshot)
    }

    pub fn next_outputs(&self, limit: usize) -> Result<Vec<OutputEvent>, String> {
        let mut guard = self.tail.lock().expect("tail mutex");
        if guard.is_none() {
            *guard = Some(JournalTail::open(&self.journal_path).map_err(|e| e.to_string())?);
        }
        let tail = guard.as_mut().expect("tail is open");
        match tail.poll(limit) {
            Ok(events) => {
                self.delivered_outputs
                    .fetch_add(events.len() as u64, Ordering::Relaxed);
                if let Some(last) = events.last() {
                    self.last_output_seq
                        .store(last.output_seq().0, Ordering::Relaxed);
                }
                Ok(events)
            }
            Err(error) => Err(error.to_string()),
        }
    }

    pub fn health(&self) -> Value {
        self.refresh_snapshot();
        let snapshot = self.latest_snapshot.lock().expect("snapshot mutex");
        let engine_ready = snapshot.is_some();
        json!({
            "status": if engine_ready { "ready" } else { "starting" },
            "uptimeSeconds": self.started.elapsed().as_secs(),
            "journalPath": self.journal_path.display().to_string(),
            "deliveredOutputs": self.delivered_outputs.load(Ordering::Relaxed).to_string(),
            "lastOutputSeq": self.last_output_seq.load(Ordering::Relaxed).to_string(),
            "asOfEngineSeq": snapshot
                .as_ref()
                .map(|value| value.as_of_engine_seq.0.to_string())
                .unwrap_or_else(|| "0".to_string()),
            "globalKill": snapshot.as_ref().map(|value| value.global_kill).unwrap_or(false),
            "killLatched": self.control.kill_engaged(),
            "telemetryDropped": self.control.telemetry_dropped().to_string(),
            "feeds": snapshot.as_ref().map(|value| value
                .instruments
                .iter()
                .map(|instrument| json!({
                    "instrument": instrument.instrument.0,
                    "symbol": instrument.symbol,
                    "feedState": instrument.feed_state.label(),
                    "lastSourceSeq": instrument.last_source_seq.to_string(),
                }))
                .collect::<Vec<_>>())
                .unwrap_or_default(),
        })
    }

    /// The latch is the single source of truth, so the engine sees both engage
    /// and release even when the command queue is saturated.
    pub fn engage_kill(&self, engaged: bool) -> Result<(), String> {
        if engaged {
            self.control.engage_kill();
        } else {
            self.control.release_kill();
        }
        Ok(())
    }

    pub fn set_account_enabled(&self, account: u32, enabled: bool) -> Result<(), String> {
        self.submit(ControlCommand::SetAccountEnabled {
            account: AccountId(account),
            enabled,
        })
    }

    pub fn set_risk_limits(
        &self,
        account: u32,
        max_order_quantity: u64,
        max_order_notional: u128,
        max_position_lots: u64,
        max_gross_exposure: u128,
        price_collar_ticks: i64,
    ) -> Result<(), String> {
        self.submit(ControlCommand::SetAccountLimits {
            account: AccountId(account),
            limits: AccountLimits {
                max_order_quantity: QuantityLots(max_order_quantity),
                max_order_notional: Notional(max_order_notional),
                max_position_lots,
                max_gross_exposure: Notional(max_gross_exposure),
                price_collar_ticks,
            },
        })
    }

    fn submit(&self, command: ControlCommand) -> Result<(), String> {
        if self.control.submit(command) {
            Ok(())
        } else {
            Err("control queue is full".to_string())
        }
    }

    /// Replay runs on an isolated core. It never touches the live engine.
    pub fn start_replay(&self, seed: u64, events: usize) -> Value {
        let job_id = format!(
            "replay-{}",
            self.next_job.fetch_add(1, Ordering::Relaxed) + 1
        );
        let inputs = Generator::new(GeneratorConfig::new(seed, events)).generate();
        let mut replay = Replay::new(
            TradingCore::new(EngineConfig::single_instrument(u128::from(seed))),
            1_000,
        );
        replay.run(&inputs);
        let result = replay.finish();
        let value = json!({
            "jobId": job_id,
            "status": "completed",
            "seed": seed.to_string(),
            "events": events.to_string(),
            "inputs": result.inputs.to_string(),
            "outputs": result.outputs.to_string(),
            "inputDigest": digest_hex(&result.input_digest),
            "outputDigest": digest_hex(&result.output_digest),
            "stateDigest": digest_hex(&result.state_digest),
            "checkpoints": result.checkpoints.len(),
        });
        self.replay_jobs
            .lock()
            .expect("replay mutex")
            .insert(job_id, value.clone());
        value
    }

    pub fn replay_status(&self, job_id: &str) -> Option<Value> {
        self.replay_jobs
            .lock()
            .expect("replay mutex")
            .get(job_id)
            .cloned()
    }
}
