//! Read-model snapshots published by the engine for cold consumers.
//!
//! Every snapshot carries the engine sequence it was taken at, so a reader can
//! never present it as exact current state.

use protocol::{
    AccountId, ClientOrderId, EngineSeq, FeedState, InstrumentId, OrderId, PriceTicks,
    QuantityLots, Side,
};
use trading_core::{EngineMetrics, TradingCore};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LevelSnapshot {
    pub side: Side,
    pub price: PriceTicks,
    pub quantity: QuantityLots,
    pub order_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderSnapshot {
    pub order_id: OrderId,
    pub account: AccountId,
    pub client_order_id: ClientOrderId,
    pub side: Side,
    pub price: PriceTicks,
    pub total_quantity: QuantityLots,
    pub cumulative_filled: QuantityLots,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentSnapshot {
    pub instrument: InstrumentId,
    pub symbol: String,
    pub feed_state: FeedState,
    pub last_source_seq: u64,
    pub best_bid: Option<PriceTicks>,
    pub best_ask: Option<PriceTicks>,
    pub levels: Vec<LevelSnapshot>,
    pub orders: Vec<OrderSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionSnapshot {
    pub account: AccountId,
    pub instrument: InstrumentId,
    pub position_lots: i64,
    pub open_buy_lots: u64,
    pub open_sell_lots: u64,
    pub open_buy_notional: u128,
    pub open_sell_notional: u128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineSnapshot {
    pub as_of_engine_seq: EngineSeq,
    pub engine_time_ns: u64,
    pub global_kill: bool,
    pub metrics: EngineMetrics,
    pub instruments: Vec<InstrumentSnapshot>,
    pub positions: Vec<PositionSnapshot>,
}

/// Orders a snapshot may carry per level of requested depth.
const ORDERS_PER_LEVEL: usize = 8;

/// Copies a bounded read model out of the engine. Called on the engine thread,
/// so it reads at most `depth` levels per side and `depth * 8` orders per
/// instrument, whatever the live order count is.
pub fn capture(core: &TradingCore, depth: usize) -> EngineSnapshot {
    let mut instruments = Vec::with_capacity(core.instruments().len());
    let mut positions = Vec::new();

    let max_orders = depth.saturating_mul(ORDERS_PER_LEVEL);
    for instrument in core.instruments() {
        let mut levels = Vec::with_capacity(depth * 2);
        let mut orders = Vec::with_capacity(max_orders);
        // Only the top levels are read, so cost follows the depth, not the book.
        for side in [Side::Buy, Side::Sell] {
            for (price, level) in instrument.book.top_levels(side, depth) {
                levels.push(LevelSnapshot {
                    side,
                    price,
                    quantity: QuantityLots(level.total_remaining),
                    order_count: level.order_count,
                });
                let room = max_orders.saturating_sub(orders.len());
                if room == 0 {
                    continue;
                }
                for order in instrument.book.level_orders_upto(side, price, room) {
                    orders.push(OrderSnapshot {
                        order_id: order.order_id,
                        account: order.account,
                        client_order_id: order.client_order_id,
                        side: order.side,
                        price: order.price,
                        total_quantity: order.total_quantity,
                        cumulative_filled: order.cumulative_filled,
                    });
                }
            }
        }
        instruments.push(InstrumentSnapshot {
            instrument: instrument.config.id,
            symbol: instrument.config.symbol.clone(),
            feed_state: instrument.market.state(),
            last_source_seq: instrument.market.last_source_seq(),
            best_bid: instrument.market.best_bid(),
            best_ask: instrument.market.best_ask(),
            levels,
            orders,
        });
    }

    for account in core.accounts() {
        for (index, position) in account.positions.iter().enumerate() {
            positions.push(PositionSnapshot {
                account: account.id,
                instrument: core.instruments()[index].config.id,
                position_lots: position.position_lots,
                open_buy_lots: position.open_buy_lots,
                open_sell_lots: position.open_sell_lots,
                open_buy_notional: position.open_buy_notional,
                open_sell_notional: position.open_sell_notional,
            });
        }
    }

    EngineSnapshot {
        as_of_engine_seq: core.engine_seq(),
        engine_time_ns: core.engine_time_ns(),
        global_kill: core.global_kill(),
        metrics: core.metrics(),
        instruments,
        positions,
    }
}
