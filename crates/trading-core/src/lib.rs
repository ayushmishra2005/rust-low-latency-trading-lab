//! Pure deterministic trading state machine.
//!
//! No filesystem, network, wall clock, async runtime, or external service is
//! reachable from this crate. The caller supplies logical time with every input.

pub mod account;
pub mod book;
pub mod config;
pub mod core;
pub mod generator;
pub mod market;
pub mod matching;
pub mod reference;
pub mod replay;
mod risk;

pub use crate::account::{
    apply_fill_to_position, request_fingerprint, CacheHit, InstrumentPosition, RequestCache,
};
pub use crate::core::{EngineFault, EngineMetrics, InstrumentRuntime, TradingCore};
pub use config::{AccountConfig, EngineConfig, InstrumentConfig};
pub use generator::{Generator, GeneratorConfig};
pub use market::{FeedCounters, MarketView};
pub use replay::{state_digest, Checkpoint, Digest, Replay, ReplayResult};
