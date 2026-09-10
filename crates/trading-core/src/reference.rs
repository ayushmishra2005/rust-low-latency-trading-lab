//! Deliberately simple book used as the correctness oracle for the indexed
//! book. Clarity matters here, not performance.

use std::collections::{BTreeMap, VecDeque};

use protocol::{OrderId, PriceTicks, PrioritySeq, QuantityLots, Side};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReferenceOrder {
    pub order_id: OrderId,
    pub priority: PrioritySeq,
    pub total_quantity: QuantityLots,
    pub cumulative_filled: QuantityLots,
}

impl ReferenceOrder {
    pub fn remaining(&self) -> QuantityLots {
        QuantityLots(self.total_quantity.0 - self.cumulative_filled.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReferenceFill {
    pub maker_order_id: OrderId,
    pub price: PriceTicks,
    pub quantity: QuantityLots,
    pub maker_remaining: QuantityLots,
}

/// One canonical row: side, price, and the orders resting at that price in FIFO
/// order. Both book implementations must produce identical rows.
pub type BookRow = (Side, PriceTicks, Vec<(OrderId, QuantityLots, PrioritySeq)>);

#[derive(Debug, Default)]
pub struct ReferenceBook {
    bids: BTreeMap<PriceTicks, VecDeque<ReferenceOrder>>,
    asks: BTreeMap<PriceTicks, VecDeque<ReferenceOrder>>,
}

impl ReferenceBook {
    pub fn new() -> ReferenceBook {
        ReferenceBook::default()
    }

    fn side_mut(&mut self, side: Side) -> &mut BTreeMap<PriceTicks, VecDeque<ReferenceOrder>> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    pub fn insert(&mut self, side: Side, price: PriceTicks, order: ReferenceOrder) {
        self.side_mut(side)
            .entry(price)
            .or_default()
            .push_back(order);
    }

    pub fn remove(&mut self, order_id: OrderId) -> Option<(Side, PriceTicks, ReferenceOrder)> {
        for side in [Side::Buy, Side::Sell] {
            let levels = self.side_mut(side);
            let mut found = None;
            for (price, orders) in levels.iter_mut() {
                if let Some(position) = orders.iter().position(|o| o.order_id == order_id) {
                    let order = orders.remove(position).expect("index came from position");
                    found = Some((*price, order, orders.is_empty()));
                    break;
                }
            }
            if let Some((price, order, empty)) = found {
                if empty {
                    levels.remove(&price);
                }
                return Some((side, price, order));
            }
        }
        None
    }

    pub fn reduce_total(&mut self, order_id: OrderId, new_total: QuantityLots) {
        for side in [Side::Buy, Side::Sell] {
            for orders in self.side_mut(side).values_mut() {
                if let Some(order) = orders.iter_mut().find(|o| o.order_id == order_id) {
                    order.total_quantity = new_total;
                    return;
                }
            }
        }
    }

    pub fn get(&self, order_id: OrderId) -> Option<(Side, PriceTicks, ReferenceOrder)> {
        for (side, levels) in [(Side::Buy, &self.bids), (Side::Sell, &self.asks)] {
            for (price, orders) in levels {
                if let Some(order) = orders.iter().find(|o| o.order_id == order_id) {
                    return Some((side, *price, *order));
                }
            }
        }
        None
    }

    pub fn match_order(
        &mut self,
        taker_side: Side,
        limit: PriceTicks,
        quantity: QuantityLots,
        fills: &mut Vec<ReferenceFill>,
    ) -> QuantityLots {
        let maker_side = taker_side.opposite();
        let mut remaining = quantity.0;

        while remaining > 0 {
            let price = match maker_side {
                Side::Buy => self.bids.keys().next_back().copied(),
                Side::Sell => self.asks.keys().next().copied(),
            };
            let Some(price) = price else { break };
            let eligible = match maker_side {
                Side::Buy => price >= limit,
                Side::Sell => price <= limit,
            };
            if !eligible {
                break;
            }

            let levels = self.side_mut(maker_side);
            let orders = levels.get_mut(&price).expect("price came from this map");
            while remaining > 0 {
                let Some(maker) = orders.front_mut() else {
                    break;
                };
                let traded = remaining.min(maker.remaining().0);
                maker.cumulative_filled = QuantityLots(maker.cumulative_filled.0 + traded);
                remaining -= traded;
                let maker_remaining = maker.remaining();
                let maker_order_id = maker.order_id;
                if maker_remaining.is_zero() {
                    orders.pop_front();
                }
                fills.push(ReferenceFill {
                    maker_order_id,
                    price,
                    quantity: QuantityLots(traded),
                    maker_remaining,
                });
            }
            if orders.is_empty() {
                levels.remove(&price);
            }
        }

        QuantityLots(quantity.0 - remaining)
    }

    pub fn snapshot(&self) -> Vec<BookRow> {
        let mut rows = Vec::new();
        for (price, orders) in self.bids.iter().rev() {
            rows.push((Side::Buy, *price, canonical_orders(orders)));
        }
        for (price, orders) in &self.asks {
            rows.push((Side::Sell, *price, canonical_orders(orders)));
        }
        rows
    }
}

fn canonical_orders(
    orders: &VecDeque<ReferenceOrder>,
) -> Vec<(OrderId, QuantityLots, PrioritySeq)> {
    orders
        .iter()
        .map(|order| (order.order_id, order.remaining(), order.priority))
        .collect()
}

/// Same canonical rows for the indexed book, so the two can be compared directly.
pub fn book_rows(book: &crate::book::OrderBook) -> Vec<BookRow> {
    book.snapshot()
        .into_iter()
        .map(|(side, price, orders)| {
            (
                side,
                price,
                orders
                    .into_iter()
                    .map(|order| (order.order_id, order.remaining(), order.priority))
                    .collect(),
            )
        })
        .collect()
}
