//! Deterministic replay support: canonical digests of the input stream, the
//! output stream, and engine state, plus periodic checkpoints.
//!
//! Nothing hashed here depends on memory layout, pointer values, hash-map
//! iteration order, or wall-clock time.

use protocol::canonical;
use protocol::{
    ControlCommand, EngineInput, EngineSeq, InputEvent, MarketEventKind, OutputEvent, Side,
};

use crate::core::TradingCore;

pub const STATE_SCHEMA_VERSION: u32 = 1;

pub type Digest = [u8; 32];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Checkpoint {
    pub engine_seq: EngineSeq,
    pub state: Digest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayResult {
    pub inputs: u64,
    pub outputs: u64,
    pub input_digest: Digest,
    pub output_digest: Digest,
    pub state_digest: Digest,
    pub checkpoints: Vec<Checkpoint>,
}

/// Streams canonical bytes into BLAKE3 without holding the whole run in memory.
pub struct DigestRecorder {
    input: blake3::Hasher,
    output: blake3::Hasher,
    scratch: Vec<u8>,
    pub inputs: u64,
    pub outputs: u64,
}

impl Default for DigestRecorder {
    fn default() -> DigestRecorder {
        DigestRecorder::new()
    }
}

impl DigestRecorder {
    pub fn new() -> DigestRecorder {
        DigestRecorder {
            input: blake3::Hasher::new(),
            output: blake3::Hasher::new(),
            scratch: Vec::with_capacity(256),
            inputs: 0,
            outputs: 0,
        }
    }

    pub fn record_input(&mut self, input: &EngineInput) {
        self.scratch.clear();
        encode_input(input, &mut self.scratch);
        self.input.update(&self.scratch);
        self.inputs += 1;
    }

    pub fn record_output(&mut self, event: &OutputEvent) {
        self.scratch.clear();
        canonical::encode_output(event, &mut self.scratch);
        self.output.update(&self.scratch);
        self.outputs += 1;
    }

    pub fn input_digest(&self) -> Digest {
        *self.input.finalize().as_bytes()
    }

    pub fn output_digest(&self) -> Digest {
        *self.output.finalize().as_bytes()
    }
}

/// Drives a core over recorded inputs and collects replay evidence.
pub struct Replay {
    core: TradingCore,
    recorder: DigestRecorder,
    output: Vec<OutputEvent>,
    checkpoint_interval: u64,
    checkpoints: Vec<Checkpoint>,
}

impl Replay {
    pub fn new(core: TradingCore, checkpoint_interval: u64) -> Replay {
        Replay {
            core,
            recorder: DigestRecorder::new(),
            output: Vec::with_capacity(64),
            checkpoint_interval,
            checkpoints: Vec::new(),
        }
    }

    pub fn core(&self) -> &TradingCore {
        &self.core
    }

    /// Applies one input and returns its output events.
    pub fn step(&mut self, input: &EngineInput) -> &[OutputEvent] {
        self.recorder.record_input(input);
        self.core.apply(input, &mut self.output);
        for event in &self.output {
            self.recorder.record_output(event);
        }
        let engine_seq = self.core.engine_seq();
        if self.checkpoint_interval > 0 && engine_seq.0 % self.checkpoint_interval == 0 {
            self.checkpoints.push(Checkpoint {
                engine_seq,
                state: state_digest(&self.core),
            });
        }
        &self.output
    }

    pub fn run(&mut self, inputs: &[EngineInput]) {
        for input in inputs {
            self.step(input);
        }
    }

    pub fn finish(&self) -> ReplayResult {
        ReplayResult {
            inputs: self.recorder.inputs,
            outputs: self.recorder.outputs,
            input_digest: self.recorder.input_digest(),
            output_digest: self.recorder.output_digest(),
            state_digest: state_digest(&self.core),
            checkpoints: self.checkpoints.clone(),
        }
    }
}

pub fn encode_input(input: &EngineInput, out: &mut Vec<u8>) {
    out.extend_from_slice(&input.ingress_seq.0.to_le_bytes());
    out.extend_from_slice(&input.recv_time_ns.to_le_bytes());
    match input.event {
        InputEvent::Market(event) => {
            out.push(1);
            out.extend_from_slice(&event.instrument.0.to_le_bytes());
            out.extend_from_slice(&event.source_seq.to_le_bytes());
            out.extend_from_slice(&event.source_time_ns.to_le_bytes());
            match event.kind {
                MarketEventKind::SnapshotBegin { snapshot_seq } => {
                    out.push(1);
                    out.extend_from_slice(&snapshot_seq.to_le_bytes());
                }
                MarketEventKind::SnapshotLevel {
                    side,
                    price,
                    quantity,
                } => {
                    out.push(2);
                    out.push(side.wire());
                    out.extend_from_slice(&price.0.to_le_bytes());
                    out.extend_from_slice(&quantity.0.to_le_bytes());
                }
                MarketEventKind::SnapshotEnd {
                    snapshot_seq,
                    level_count,
                } => {
                    out.push(3);
                    out.extend_from_slice(&snapshot_seq.to_le_bytes());
                    out.extend_from_slice(&level_count.to_le_bytes());
                }
                MarketEventKind::LevelSet {
                    side,
                    price,
                    quantity,
                } => {
                    out.push(4);
                    out.push(side.wire());
                    out.extend_from_slice(&price.0.to_le_bytes());
                    out.extend_from_slice(&quantity.0.to_le_bytes());
                }
                MarketEventKind::Trade {
                    aggressor,
                    price,
                    quantity,
                } => {
                    out.push(5);
                    out.push(aggressor.wire());
                    out.extend_from_slice(&price.0.to_le_bytes());
                    out.extend_from_slice(&quantity.0.to_le_bytes());
                }
                MarketEventKind::Heartbeat => out.push(6),
                MarketEventKind::FeedReset { new_epoch } => {
                    out.push(7);
                    out.extend_from_slice(&new_epoch.to_le_bytes());
                }
            }
        }
        InputEvent::Order(request) => {
            out.push(2);
            out.push(request.kind.wire());
            out.push(request.side.wire());
            out.push(request.order_type.wire());
            out.extend_from_slice(&request.account.0.to_le_bytes());
            out.extend_from_slice(&request.instrument.0.to_le_bytes());
            out.extend_from_slice(&request.request_id.0.to_le_bytes());
            out.extend_from_slice(&request.client_seq.to_le_bytes());
            out.extend_from_slice(&request.client_order_id.0.to_le_bytes());
            out.extend_from_slice(&request.target_client_order_id.0.to_le_bytes());
            out.extend_from_slice(&request.price.0.to_le_bytes());
            out.extend_from_slice(&request.quantity.0.to_le_bytes());
        }
        InputEvent::Control(command) => {
            out.push(3);
            match command {
                ControlCommand::SetGlobalKill { engaged } => {
                    out.push(1);
                    out.push(u8::from(engaged));
                }
                ControlCommand::SetAccountEnabled { account, enabled } => {
                    out.push(2);
                    out.extend_from_slice(&account.0.to_le_bytes());
                    out.push(u8::from(enabled));
                }
                ControlCommand::SetAccountLimits { account, limits } => {
                    out.push(3);
                    out.extend_from_slice(&account.0.to_le_bytes());
                    out.extend_from_slice(&limits.max_order_quantity.0.to_le_bytes());
                    out.extend_from_slice(&limits.max_order_notional.0.to_le_bytes());
                    out.extend_from_slice(&limits.max_position_lots.to_le_bytes());
                    out.extend_from_slice(&limits.max_gross_exposure.0.to_le_bytes());
                    out.extend_from_slice(&limits.price_collar_ticks.to_le_bytes());
                }
            }
        }
    }
}

/// Canonical state serialization fed to BLAKE3.
pub fn state_digest(core: &TradingCore) -> Digest {
    let mut hasher = blake3::Hasher::new();
    let mut buffer = Vec::with_capacity(1024);

    buffer.extend_from_slice(&STATE_SCHEMA_VERSION.to_le_bytes());
    buffer.extend_from_slice(&core.config().run_id.to_le_bytes());
    buffer.extend_from_slice(&core.engine_seq().0.to_le_bytes());
    buffer.extend_from_slice(&core.output_seq().0.to_le_bytes());
    buffer.extend_from_slice(&core.engine_time_ns().to_le_bytes());
    let (order_id, trade_id, priority) = core.next_ids();
    buffer.extend_from_slice(&order_id.0.to_le_bytes());
    buffer.extend_from_slice(&trade_id.0.to_le_bytes());
    buffer.extend_from_slice(&priority.0.to_le_bytes());
    buffer.push(u8::from(core.global_kill()));
    hasher.update(&buffer);

    // Instruments in numeric ID order.
    let mut instrument_order: Vec<usize> = (0..core.instruments().len()).collect();
    instrument_order.sort_by_key(|index| core.instruments()[*index].config.id);
    for index in &instrument_order {
        let instrument = &core.instruments()[*index];
        buffer.clear();
        buffer.extend_from_slice(&instrument.config.id.0.to_le_bytes());
        buffer.extend_from_slice(&instrument.config.tick_size.to_le_bytes());
        buffer.extend_from_slice(&instrument.config.lot_size.to_le_bytes());
        buffer.push(instrument.market.state().wire());
        buffer.extend_from_slice(&instrument.market.epoch().to_le_bytes());
        buffer.extend_from_slice(&instrument.market.last_source_seq().to_le_bytes());
        buffer.extend_from_slice(&instrument.market.last_update_time_ns().to_le_bytes());
        for side in [Side::Buy, Side::Sell] {
            for (price, quantity) in instrument.market.levels(side) {
                buffer.push(side.wire());
                buffer.extend_from_slice(&price.0.to_le_bytes());
                buffer.extend_from_slice(&quantity.0.to_le_bytes());
            }
        }
        for (side, price, orders) in instrument.book.snapshot() {
            buffer.push(side.wire());
            buffer.extend_from_slice(&price.0.to_le_bytes());
            for order in orders {
                buffer.extend_from_slice(&order.order_id.0.to_le_bytes());
                buffer.extend_from_slice(&order.account.0.to_le_bytes());
                buffer.extend_from_slice(&order.client_order_id.0.to_le_bytes());
                buffer.extend_from_slice(&order.total_quantity.0.to_le_bytes());
                buffer.extend_from_slice(&order.cumulative_filled.0.to_le_bytes());
                buffer.extend_from_slice(&order.priority.0.to_le_bytes());
            }
        }
        hasher.update(&buffer);
    }

    // Accounts in numeric ID order.
    let mut account_order: Vec<usize> = (0..core.accounts().len()).collect();
    account_order.sort_by_key(|index| core.accounts()[*index].id);
    for index in &account_order {
        let account = &core.accounts()[*index];
        buffer.clear();
        buffer.extend_from_slice(&account.id.0.to_le_bytes());
        buffer.push(u8::from(account.enabled));
        buffer.extend_from_slice(&account.limits.max_order_quantity.0.to_le_bytes());
        buffer.extend_from_slice(&account.limits.max_order_notional.0.to_le_bytes());
        buffer.extend_from_slice(&account.limits.max_position_lots.to_le_bytes());
        buffer.extend_from_slice(&account.limits.max_gross_exposure.0.to_le_bytes());
        buffer.extend_from_slice(&account.limits.price_collar_ticks.to_le_bytes());
        buffer.extend_from_slice(&account.last_client_seq.to_le_bytes());
        for instrument_index in &instrument_order {
            let position = &account.positions[*instrument_index];
            buffer.extend_from_slice(&position.position_lots.to_le_bytes());
            buffer.extend_from_slice(&position.open_buy_lots.to_le_bytes());
            buffer.extend_from_slice(&position.open_sell_lots.to_le_bytes());
            buffer.extend_from_slice(&position.open_buy_notional.to_le_bytes());
            buffer.extend_from_slice(&position.open_sell_notional.to_le_bytes());
        }
        for (request_id, fingerprint, outcome) in account.cache.canonical_entries() {
            buffer.extend_from_slice(&request_id.0.to_le_bytes());
            buffer.extend_from_slice(&fingerprint.to_le_bytes());
            buffer.extend_from_slice(&outcome.order_id.0.to_le_bytes());
            buffer.push(outcome.kind.wire());
            buffer.push(outcome.reject_reason.map_or(0, |reason| reason.wire()));
        }
        hasher.update(&buffer);
    }

    *hasher.finalize().as_bytes()
}

pub fn digest_hex(digest: &Digest) -> String {
    let mut out = String::with_capacity(64);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
