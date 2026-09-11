//! Matching book for orders accepted by this engine.
//!
//! Price levels live in a `BTreeMap`; the orders inside a level form an indexed
//! doubly linked FIFO over a preallocated slab. Cancellation unlinks in O(1) and
//! never uses raw pointers.

use std::collections::{BTreeMap, HashMap};

use protocol::{AccountId, ClientOrderId, OrderId, PriceTicks, PrioritySeq, QuantityLots, Side};
use slab::Slab;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderNode {
    pub order_id: OrderId,
    pub account: AccountId,
    pub client_order_id: ClientOrderId,
    pub side: Side,
    pub price: PriceTicks,
    pub total_quantity: QuantityLots,
    pub cumulative_filled: QuantityLots,
    pub priority: PrioritySeq,
    prev: Option<usize>,
    next: Option<usize>,
}

impl OrderNode {
    pub fn remaining(&self) -> QuantityLots {
        QuantityLots(
            self.total_quantity
                .0
                .checked_sub(self.cumulative_filled.0)
                .expect("accounting: cumulative fill exceeded total"),
        )
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PriceLevel {
    head: Option<usize>,
    tail: Option<usize>,
    pub order_count: u32,
    pub total_remaining: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NewOrder {
    pub order_id: OrderId,
    pub account: AccountId,
    pub client_order_id: ClientOrderId,
    pub side: Side,
    pub price: PriceTicks,
    pub total_quantity: QuantityLots,
    pub cumulative_filled: QuantityLots,
    pub priority: PrioritySeq,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapacityExhausted;

pub struct OrderBook {
    bids: BTreeMap<PriceTicks, PriceLevel>,
    asks: BTreeMap<PriceTicks, PriceLevel>,
    orders: Slab<OrderNode>,
    index: HashMap<OrderId, usize>,
    capacity: usize,
}

impl OrderBook {
    pub fn with_capacity(capacity: usize) -> OrderBook {
        OrderBook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            orders: Slab::with_capacity(capacity),
            index: HashMap::with_capacity(capacity),
            capacity,
        }
    }

    pub fn live_order_count(&self) -> usize {
        self.orders.len()
    }

    pub fn is_full(&self) -> bool {
        self.orders.len() >= self.capacity
    }

    pub fn get(&self, order_id: OrderId) -> Option<&OrderNode> {
        self.index.get(&order_id).map(|slot| &self.orders[*slot])
    }

    pub fn best_bid(&self) -> Option<(PriceTicks, &PriceLevel)> {
        self.bids.iter().next_back().map(|(p, l)| (*p, l))
    }

    pub fn best_ask(&self) -> Option<(PriceTicks, &PriceLevel)> {
        self.asks.iter().next().map(|(p, l)| (*p, l))
    }

    pub fn level_count(&self) -> usize {
        self.bids.len() + self.asks.len()
    }

    /// Appends an order at the tail of its price level.
    pub fn insert(&mut self, order: NewOrder) -> Result<(), CapacityExhausted> {
        if self.is_full() {
            return Err(CapacityExhausted);
        }
        let remaining = order
            .total_quantity
            .0
            .checked_sub(order.cumulative_filled.0)
            .ok_or(CapacityExhausted)?;
        {
            let levels = match order.side {
                Side::Buy => &self.bids,
                Side::Sell => &self.asks,
            };
            if let Some(level) = levels.get(&order.price) {
                if level.total_remaining.checked_add(remaining).is_none()
                    || level.order_count.checked_add(1).is_none()
                {
                    return Err(CapacityExhausted);
                }
            }
        }
        let slot = self.orders.insert(OrderNode {
            order_id: order.order_id,
            account: order.account,
            client_order_id: order.client_order_id,
            side: order.side,
            price: order.price,
            total_quantity: order.total_quantity,
            cumulative_filled: order.cumulative_filled,
            priority: order.priority,
            prev: None,
            next: None,
        });
        self.index.insert(order.order_id, slot);

        let levels = match order.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let level = levels.entry(order.price).or_default();
        match level.tail {
            Some(tail) => {
                self.orders[tail].next = Some(slot);
                self.orders[slot].prev = Some(tail);
            }
            None => level.head = Some(slot),
        }
        level.tail = Some(slot);
        level.order_count += 1;
        level.total_remaining += remaining;
        Ok(())
    }

    /// Unlinks and removes an order. Returns its final node state.
    pub fn remove(&mut self, order_id: OrderId) -> Option<OrderNode> {
        let slot = self.index.remove(&order_id)?;
        let node = self.orders.remove(slot);
        self.unlink(&node);
        Some(node)
    }

    fn unlink(&mut self, node: &OrderNode) {
        let levels = match node.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let Some(level) = levels.get_mut(&node.price) else {
            return;
        };
        match node.prev {
            Some(prev) => self.orders[prev].next = node.next,
            None => level.head = node.next,
        }
        match node.next {
            Some(next) => self.orders[next].prev = node.prev,
            None => level.tail = node.prev,
        }
        level.order_count = level.order_count.saturating_sub(1);
        level.total_remaining = level
            .total_remaining
            .checked_sub(node.remaining().0)
            .expect("accounting: level remaining underflow");
        if level.order_count == 0 {
            levels.remove(&node.price);
        }
    }

    /// Reduces the total quantity of a resting order while keeping its priority.
    pub fn reduce_total(&mut self, order_id: OrderId, new_total: QuantityLots) {
        let Some(&slot) = self.index.get(&order_id) else {
            return;
        };
        let node = &mut self.orders[slot];
        let old_remaining = node.remaining().0;
        node.total_quantity = new_total;
        let new_remaining = node.remaining().0;
        let (side, price) = (node.side, node.price);
        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        if let Some(level) = levels.get_mut(&price) {
            level.total_remaining = level
                .total_remaining
                .checked_sub(old_remaining)
                .and_then(|total| total.checked_add(new_remaining))
                .expect("accounting: level remaining overflow");
        }
    }

    /// Applies a fill to a resting order and removes it when fully filled.
    pub(crate) fn fill_maker(&mut self, slot: usize, quantity: QuantityLots) -> OrderNode {
        let node = &mut self.orders[slot];
        node.cumulative_filled = QuantityLots(
            node.cumulative_filled
                .0
                .checked_add(quantity.0)
                .expect("accounting: cumulative fill overflow"),
        );
        let filled_out = node.remaining().is_zero();
        let node_copy = *node;
        let (side, price) = (node_copy.side, node_copy.price);
        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        if let Some(level) = levels.get_mut(&price) {
            level.total_remaining = level
                .total_remaining
                .checked_sub(quantity.0)
                .expect("accounting: level remaining underflow");
        }
        if filled_out {
            self.index.remove(&node_copy.order_id);
            let removed = self.orders.remove(slot);
            self.unlink(&removed);
        }
        node_copy
    }

    pub(crate) fn head_slot(&self, side: Side, price: PriceTicks) -> Option<usize> {
        let levels = match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        };
        levels.get(&price).and_then(|level| level.head)
    }

    pub(crate) fn node(&self, slot: usize) -> &OrderNode {
        &self.orders[slot]
    }

    /// Best resting price on `side` that is eligible against `limit`.
    pub(crate) fn next_eligible_price(
        &self,
        side: Side,
        limit: Option<PriceTicks>,
    ) -> Option<PriceTicks> {
        match side {
            Side::Buy => {
                let (price, _) = self.bids.iter().next_back()?;
                match limit {
                    Some(limit) if *price < limit => None,
                    _ => Some(*price),
                }
            }
            Side::Sell => {
                let (price, _) = self.asks.iter().next()?;
                match limit {
                    Some(limit) if *price > limit => None,
                    _ => Some(*price),
                }
            }
        }
    }

    /// Level view in canonical price order: bids descending, asks ascending.
    pub fn levels(&self, side: Side) -> Vec<(PriceTicks, PriceLevel)> {
        match side {
            Side::Buy => self.bids.iter().rev().map(|(p, l)| (*p, *l)).collect(),
            Side::Sell => self.asks.iter().map(|(p, l)| (*p, *l)).collect(),
        }
    }

    /// Best `limit` levels on one side. Work is bounded by `limit`, not by the
    /// number of levels in the book.
    pub fn top_levels(&self, side: Side, limit: usize) -> Vec<(PriceTicks, PriceLevel)> {
        match side {
            Side::Buy => self
                .bids
                .iter()
                .rev()
                .take(limit)
                .map(|(p, l)| (*p, *l))
                .collect(),
            Side::Sell => self
                .asks
                .iter()
                .take(limit)
                .map(|(p, l)| (*p, *l))
                .collect(),
        }
    }

    /// First `limit` orders at one price, in FIFO priority order.
    pub fn level_orders_upto(&self, side: Side, price: PriceTicks, limit: usize) -> Vec<OrderNode> {
        let levels = match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        };
        let mut out = Vec::new();
        let mut cursor = levels.get(&price).and_then(|level| level.head);
        while let Some(slot) = cursor {
            if out.len() >= limit {
                break;
            }
            let node = self.orders[slot];
            out.push(node);
            cursor = node.next;
        }
        out
    }

    /// Orders at one price in FIFO priority order.
    pub fn level_orders(&self, side: Side, price: PriceTicks) -> Vec<OrderNode> {
        let levels = match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        };
        let mut out = Vec::new();
        let mut cursor = levels.get(&price).and_then(|level| level.head);
        while let Some(slot) = cursor {
            let node = self.orders[slot];
            out.push(node);
            cursor = node.next;
        }
        out
    }

    /// Full canonical view used by state hashing, tests, and read models.
    pub fn snapshot(&self) -> Vec<(Side, PriceTicks, Vec<OrderNode>)> {
        let mut out = Vec::new();
        for side in [Side::Buy, Side::Sell] {
            for (price, _) in self.levels(side) {
                out.push((side, price, self.level_orders(side, price)));
            }
        }
        out
    }

    /// Checks the structural invariants. Used by tests and debug tooling.
    pub fn check_invariants(&self) -> Result<(), String> {
        let mut seen = 0usize;
        for side in [Side::Buy, Side::Sell] {
            for (price, level) in self.levels(side) {
                if level.order_count == 0 {
                    return Err(format!("empty level retained at {price}"));
                }
                let orders = self.level_orders(side, price);
                if orders.len() != level.order_count as usize {
                    return Err(format!("level {price} count mismatch"));
                }
                let total: u64 = orders.iter().map(|order| order.remaining().0).sum();
                if total != level.total_remaining {
                    return Err(format!("level {price} aggregate mismatch"));
                }
                let mut previous_priority = None;
                for order in &orders {
                    if order.side != side || order.price != price {
                        return Err(format!("order {} in wrong level", order.order_id));
                    }
                    if order.remaining().is_zero() {
                        return Err(format!("order {} rests with no quantity", order.order_id));
                    }
                    if let Some(previous) = previous_priority {
                        if order.priority <= previous {
                            return Err(format!("priority not increasing at {price}"));
                        }
                    }
                    previous_priority = Some(order.priority);
                    match self.index.get(&order.order_id) {
                        Some(slot) if self.orders[*slot].order_id == order.order_id => {}
                        _ => return Err(format!("order {} missing from index", order.order_id)),
                    }
                }
                seen += orders.len();
            }
        }
        if seen != self.orders.len() || seen != self.index.len() {
            return Err("live order count mismatch".to_string());
        }
        if let (Some((bid, _)), Some((ask, _))) = (self.best_bid(), self.best_ask()) {
            if bid >= ask {
                return Err(format!("crossed book: bid {bid} ask {ask}"));
            }
        }
        Ok(())
    }
}
