//! Engine configuration. Everything that can change a decision is recorded here
//! so replay can rebuild the same engine.

use protocol::{AccountId, AccountLimits, InstrumentId, Notional, QuantityLots};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentConfig {
    pub id: InstrumentId,
    pub symbol: String,
    pub tick_size: i64,
    pub lot_size: u64,
    pub min_price_ticks: i64,
    pub max_price_ticks: i64,
}

impl InstrumentConfig {
    pub fn new(id: u32, symbol: &str) -> InstrumentConfig {
        InstrumentConfig {
            id: InstrumentId(id),
            symbol: symbol.to_string(),
            tick_size: 1,
            lot_size: 1,
            min_price_ticks: 1,
            max_price_ticks: 10_000_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountConfig {
    pub id: AccountId,
    pub enabled: bool,
    pub limits: AccountLimits,
}

impl AccountConfig {
    pub fn new(id: u32) -> AccountConfig {
        AccountConfig {
            id: AccountId(id),
            enabled: true,
            limits: AccountLimits {
                max_order_quantity: QuantityLots(10_000),
                max_order_notional: Notional(1_000_000_000),
                max_position_lots: 100_000,
                max_gross_exposure: Notional(10_000_000_000),
                price_collar_ticks: 5_000,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineConfig {
    pub run_id: u128,
    pub instruments: Vec<InstrumentConfig>,
    pub accounts: Vec<AccountConfig>,
    /// Live resting orders allowed per instrument book.
    pub max_live_orders: usize,
    /// Reject risk-increasing orders when the market view is older than this.
    pub max_market_age_ns: u64,
    /// Distance from the reference price used as a market-order protection bound.
    pub market_protection_ticks: i64,
    /// Retained request fingerprints per account.
    pub dedup_window: usize,
}

impl EngineConfig {
    /// One instrument and two accounts. Used by tests, fixtures, and the simulator.
    pub fn single_instrument(run_id: u128) -> EngineConfig {
        EngineConfig {
            run_id,
            instruments: vec![InstrumentConfig::new(1, "LAB-USD")],
            accounts: vec![AccountConfig::new(1), AccountConfig::new(2)],
            max_live_orders: 4_096,
            max_market_age_ns: 1_000_000_000,
            market_protection_ticks: 50,
            dedup_window: 256,
        }
    }
}
