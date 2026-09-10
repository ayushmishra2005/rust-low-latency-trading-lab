//! Normalized events crossing the ingress boundary and leaving the engine.

use crate::enums::{FeedState, OrderState, OrderType, RejectReason, ReportKind, RequestKind, Side};
use crate::ids::{
    AccountId, ClientOrderId, EngineSeq, IngressSeq, InstrumentId, Notional, OrderId, OutputSeq,
    PriceTicks, QuantityLots, RequestId, TradeId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketEventKind {
    SnapshotBegin {
        snapshot_seq: u64,
    },
    SnapshotLevel {
        side: Side,
        price: PriceTicks,
        quantity: QuantityLots,
    },
    SnapshotEnd {
        snapshot_seq: u64,
        level_count: u32,
    },
    /// Absolute level quantity. Zero deletes the level.
    LevelSet {
        side: Side,
        price: PriceTicks,
        quantity: QuantityLots,
    },
    Trade {
        aggressor: Side,
        price: PriceTicks,
        quantity: QuantityLots,
    },
    Heartbeat,
    FeedReset {
        new_epoch: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketEvent {
    pub instrument: InstrumentId,
    pub source_seq: u64,
    pub source_time_ns: u64,
    pub kind: MarketEventKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderRequest {
    pub kind: RequestKind,
    pub account: AccountId,
    pub instrument: InstrumentId,
    pub request_id: RequestId,
    pub client_seq: u64,
    pub client_order_id: ClientOrderId,
    /// Cancel and replace target. Zero for new orders.
    pub target_client_order_id: ClientOrderId,
    pub side: Side,
    pub order_type: OrderType,
    pub price: PriceTicks,
    /// Total order quantity. For replace this is the new total, not the new remainder.
    pub quantity: QuantityLots,
}

/// Cold control input. The engine assigns its `EngineSeq` when it applies it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlCommand {
    SetGlobalKill {
        engaged: bool,
    },
    SetAccountEnabled {
        account: AccountId,
        enabled: bool,
    },
    SetAccountLimits {
        account: AccountId,
        limits: AccountLimits,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountLimits {
    pub max_order_quantity: QuantityLots,
    pub max_order_notional: Notional,
    pub max_position_lots: u64,
    pub max_gross_exposure: Notional,
    /// Allowed distance from the reference price, in ticks.
    pub price_collar_ticks: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputEvent {
    Market(MarketEvent),
    Order(OrderRequest),
    Control(ControlCommand),
}

/// One normalized input with its total order and logical time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineInput {
    pub ingress_seq: IngressSeq,
    pub recv_time_ns: u64,
    pub event: InputEvent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionReport {
    pub output_seq: OutputSeq,
    pub engine_seq: EngineSeq,
    pub engine_time_ns: u64,
    pub account: AccountId,
    pub instrument: InstrumentId,
    pub request_id: RequestId,
    pub client_order_id: ClientOrderId,
    /// Zero when the request was rejected before an order existed.
    pub order_id: OrderId,
    pub kind: ReportKind,
    pub state: OrderState,
    pub side: Side,
    pub order_type: OrderType,
    pub price: PriceTicks,
    pub total_quantity: QuantityLots,
    pub cumulative_filled: QuantityLots,
    pub remaining: QuantityLots,
    pub last_fill_quantity: QuantityLots,
    pub last_fill_price: PriceTicks,
    pub reject_reason: Option<RejectReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeEvent {
    pub output_seq: OutputSeq,
    pub engine_seq: EngineSeq,
    pub engine_time_ns: u64,
    pub trade_id: TradeId,
    pub instrument: InstrumentId,
    pub maker_order_id: OrderId,
    pub taker_order_id: OrderId,
    pub maker_account: AccountId,
    pub taker_account: AccountId,
    pub aggressor: Side,
    pub price: PriceTicks,
    pub quantity: QuantityLots,
}

/// Engine-observed state changes that replay must reproduce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineStateEvent {
    KillSwitchEngaged,
    KillSwitchReleased,
    AccountEnabledChanged {
        account: AccountId,
        enabled: bool,
    },
    AccountLimitsChanged {
        account: AccountId,
    },
    FeedStateChanged {
        instrument: InstrumentId,
        state: FeedState,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateEvent {
    pub output_seq: OutputSeq,
    pub engine_seq: EngineSeq,
    pub engine_time_ns: u64,
    pub event: EngineStateEvent,
}

/// Output order within one applied input is fixed: trade, maker report, taker report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputEvent {
    Trade(TradeEvent),
    Report(ExecutionReport),
    State(StateEvent),
}

impl OutputEvent {
    pub fn output_seq(&self) -> OutputSeq {
        match self {
            OutputEvent::Trade(trade) => trade.output_seq,
            OutputEvent::Report(report) => report.output_seq,
            OutputEvent::State(state) => state.output_seq,
        }
    }

    pub fn engine_seq(&self) -> EngineSeq {
        match self {
            OutputEvent::Trade(trade) => trade.engine_seq,
            OutputEvent::Report(report) => report.engine_seq,
            OutputEvent::State(state) => state.engine_seq,
        }
    }
}
