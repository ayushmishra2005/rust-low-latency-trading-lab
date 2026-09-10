//! Stable domain types and the versioned binary protocol shared by the engine,
//! the runtime, and cold external components.

pub mod canonical;
pub mod codec;
pub mod enums;
pub mod error;
pub mod events;
pub mod ids;
mod read;

pub use enums::{FeedState, OrderState, OrderType, RejectReason, ReportKind, RequestKind, Side};
pub use error::{DecodeError, DecodeErrorKind};
pub use events::{
    AccountLimits, ControlCommand, EngineInput, EngineStateEvent, ExecutionReport, InputEvent,
    MarketEvent, MarketEventKind, OrderRequest, OutputEvent, StateEvent, TradeEvent,
};
pub use ids::{
    AccountId, ClientOrderId, EngineSeq, IngressSeq, InstrumentId, Notional, OrderId, OutputSeq,
    PriceTicks, PrioritySeq, QuantityLots, RequestId, TradeId,
};
