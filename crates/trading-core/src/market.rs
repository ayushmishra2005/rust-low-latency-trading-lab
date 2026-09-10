//! External market view and the feed synchronization state machine.
//!
//! This is reference data only. It never holds orders accepted by the engine.

use std::collections::BTreeMap;

use protocol::{FeedState, MarketEventKind, PriceTicks, QuantityLots, Side};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FeedCounters {
    pub applied: u64,
    pub duplicates: u64,
    pub gaps: u64,
    pub resyncs: u64,
    pub out_of_order: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketUpdate {
    pub previous_state: FeedState,
    pub state: FeedState,
    pub duplicate: bool,
}

impl MarketUpdate {
    pub fn state_changed(&self) -> bool {
        self.previous_state != self.state
    }
}

pub struct MarketView {
    state: FeedState,
    epoch: u64,
    last_source_seq: u64,
    bids: BTreeMap<PriceTicks, u64>,
    asks: BTreeMap<PriceTicks, u64>,
    scratch_bids: BTreeMap<PriceTicks, u64>,
    scratch_asks: BTreeMap<PriceTicks, u64>,
    scratch_levels: u32,
    snapshot_seq: u64,
    last_trade_price: Option<PriceTicks>,
    last_update_time_ns: u64,
    counters: FeedCounters,
}

impl Default for MarketView {
    fn default() -> MarketView {
        MarketView::new()
    }
}

impl MarketView {
    pub fn new() -> MarketView {
        MarketView {
            state: FeedState::Unsynchronized,
            epoch: 0,
            last_source_seq: 0,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            scratch_bids: BTreeMap::new(),
            scratch_asks: BTreeMap::new(),
            scratch_levels: 0,
            snapshot_seq: 0,
            last_trade_price: None,
            last_update_time_ns: 0,
            counters: FeedCounters::default(),
        }
    }

    pub fn state(&self) -> FeedState {
        self.state
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn last_source_seq(&self) -> u64 {
        self.last_source_seq
    }

    pub fn counters(&self) -> FeedCounters {
        self.counters
    }

    pub fn last_update_time_ns(&self) -> u64 {
        self.last_update_time_ns
    }

    pub fn last_trade_price(&self) -> Option<PriceTicks> {
        self.last_trade_price
    }

    pub fn best_bid(&self) -> Option<PriceTicks> {
        self.bids.keys().next_back().copied()
    }

    pub fn best_ask(&self) -> Option<PriceTicks> {
        self.asks.keys().next().copied()
    }

    /// Midpoint when both sides are valid, otherwise the last trade price.
    pub fn reference_price(&self) -> Option<PriceTicks> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) if bid < ask => PriceTicks::midpoint(bid, ask),
            _ => self.last_trade_price,
        }
    }

    pub fn best(&self, side: Side) -> Option<PriceTicks> {
        match side {
            Side::Buy => self.best_bid(),
            Side::Sell => self.best_ask(),
        }
    }

    pub fn levels(&self, side: Side) -> Vec<(PriceTicks, QuantityLots)> {
        match side {
            Side::Buy => self
                .bids
                .iter()
                .rev()
                .map(|(p, q)| (*p, QuantityLots(*q)))
                .collect(),
            Side::Sell => self
                .asks
                .iter()
                .map(|(p, q)| (*p, QuantityLots(*q)))
                .collect(),
        }
    }

    pub fn apply(
        &mut self,
        source_seq: u64,
        kind: MarketEventKind,
        recv_time_ns: u64,
    ) -> MarketUpdate {
        let previous_state = self.state;

        // Non-sequenced messages carry source sequence zero.
        if source_seq != 0 {
            match self.check_sequence(source_seq) {
                SequenceCheck::Duplicate => {
                    self.counters.duplicates += 1;
                    return MarketUpdate {
                        previous_state,
                        state: self.state,
                        duplicate: true,
                    };
                }
                SequenceCheck::Gap => {
                    self.counters.gaps += 1;
                    self.enter_gap();
                    return MarketUpdate {
                        previous_state,
                        state: self.state,
                        duplicate: false,
                    };
                }
                SequenceCheck::OutOfOrder => {
                    self.counters.out_of_order += 1;
                    self.enter_gap();
                    return MarketUpdate {
                        previous_state,
                        state: self.state,
                        duplicate: false,
                    };
                }
                SequenceCheck::Ok => self.last_source_seq = source_seq,
            }
        }

        self.counters.applied += 1;
        self.last_update_time_ns = recv_time_ns;

        match kind {
            MarketEventKind::SnapshotBegin { snapshot_seq } => {
                self.scratch_bids.clear();
                self.scratch_asks.clear();
                self.scratch_levels = 0;
                self.snapshot_seq = snapshot_seq;
                self.state = FeedState::ApplyingSnapshot;
            }
            MarketEventKind::SnapshotLevel {
                side,
                price,
                quantity,
            } => {
                if self.state == FeedState::ApplyingSnapshot && !quantity.is_zero() {
                    let scratch = match side {
                        Side::Buy => &mut self.scratch_bids,
                        Side::Sell => &mut self.scratch_asks,
                    };
                    scratch.insert(price, quantity.0);
                    self.scratch_levels += 1;
                }
            }
            MarketEventKind::SnapshotEnd {
                snapshot_seq,
                level_count,
            } => {
                let complete = self.state == FeedState::ApplyingSnapshot
                    && snapshot_seq == self.snapshot_seq
                    && level_count == self.scratch_levels;
                if complete {
                    // The snapshot becomes visible only when it is fully validated.
                    std::mem::swap(&mut self.bids, &mut self.scratch_bids);
                    std::mem::swap(&mut self.asks, &mut self.scratch_asks);
                    self.state = FeedState::Live;
                    self.counters.resyncs += 1;
                } else {
                    self.enter_gap();
                }
                self.scratch_bids.clear();
                self.scratch_asks.clear();
                self.scratch_levels = 0;
            }
            MarketEventKind::LevelSet {
                side,
                price,
                quantity,
            } => {
                if self.state == FeedState::Live {
                    let levels = match side {
                        Side::Buy => &mut self.bids,
                        Side::Sell => &mut self.asks,
                    };
                    if quantity.is_zero() {
                        levels.remove(&price);
                    } else {
                        levels.insert(price, quantity.0);
                    }
                }
            }
            MarketEventKind::Trade { price, .. } => {
                if self.state == FeedState::Live {
                    self.last_trade_price = Some(price);
                }
            }
            MarketEventKind::Heartbeat => {}
            MarketEventKind::FeedReset { new_epoch } => {
                self.epoch = new_epoch;
                self.last_source_seq = 0;
                self.bids.clear();
                self.asks.clear();
                self.scratch_bids.clear();
                self.scratch_asks.clear();
                self.scratch_levels = 0;
                self.last_trade_price = None;
                self.state = FeedState::Unsynchronized;
            }
        }

        MarketUpdate {
            previous_state,
            state: self.state,
            duplicate: false,
        }
    }

    fn check_sequence(&self, source_seq: u64) -> SequenceCheck {
        match self.state {
            // A new snapshot may start from any sequence.
            FeedState::Unsynchronized | FeedState::Gap => SequenceCheck::Ok,
            FeedState::ApplyingSnapshot | FeedState::Live => {
                if self.last_source_seq == 0 {
                    SequenceCheck::Ok
                } else if source_seq == self.last_source_seq {
                    SequenceCheck::Duplicate
                } else if source_seq < self.last_source_seq {
                    SequenceCheck::OutOfOrder
                } else if source_seq == self.last_source_seq + 1 {
                    SequenceCheck::Ok
                } else {
                    SequenceCheck::Gap
                }
            }
        }
    }

    fn enter_gap(&mut self) {
        self.state = FeedState::Gap;
        self.scratch_bids.clear();
        self.scratch_asks.clear();
        self.scratch_levels = 0;
    }
}

enum SequenceCheck {
    Ok,
    Duplicate,
    Gap,
    OutOfOrder,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(view: &mut MarketView, start_seq: u64) {
        view.apply(
            start_seq,
            MarketEventKind::SnapshotBegin { snapshot_seq: 1 },
            10,
        );
        view.apply(
            start_seq + 1,
            MarketEventKind::SnapshotLevel {
                side: Side::Buy,
                price: PriceTicks(99),
                quantity: QuantityLots(4),
            },
            11,
        );
        view.apply(
            start_seq + 2,
            MarketEventKind::SnapshotLevel {
                side: Side::Sell,
                price: PriceTicks(101),
                quantity: QuantityLots(4),
            },
            12,
        );
        view.apply(
            start_seq + 3,
            MarketEventKind::SnapshotEnd {
                snapshot_seq: 1,
                level_count: 2,
            },
            13,
        );
    }

    #[test]
    fn snapshot_becomes_visible_only_at_the_end() {
        let mut view = MarketView::new();
        view.apply(1, MarketEventKind::SnapshotBegin { snapshot_seq: 1 }, 1);
        view.apply(
            2,
            MarketEventKind::SnapshotLevel {
                side: Side::Buy,
                price: PriceTicks(99),
                quantity: QuantityLots(4),
            },
            2,
        );
        assert_eq!(view.state(), FeedState::ApplyingSnapshot);
        assert_eq!(view.best_bid(), None);

        view.apply(
            3,
            MarketEventKind::SnapshotEnd {
                snapshot_seq: 1,
                level_count: 1,
            },
            3,
        );
        assert_eq!(view.state(), FeedState::Live);
        assert_eq!(view.best_bid(), Some(PriceTicks(99)));
    }

    #[test]
    fn interrupted_snapshot_enters_gap() {
        let mut view = MarketView::new();
        view.apply(1, MarketEventKind::SnapshotBegin { snapshot_seq: 1 }, 1);
        view.apply(
            2,
            MarketEventKind::SnapshotEnd {
                snapshot_seq: 1,
                level_count: 3,
            },
            2,
        );
        assert_eq!(view.state(), FeedState::Gap);
    }

    #[test]
    fn duplicate_is_counted_and_ignored() {
        let mut view = MarketView::new();
        snapshot(&mut view, 1);
        let update = view.apply(
            4,
            MarketEventKind::LevelSet {
                side: Side::Buy,
                price: PriceTicks(98),
                quantity: QuantityLots(1),
            },
            20,
        );
        assert!(update.duplicate);
        assert_eq!(view.counters().duplicates, 1);
        assert_eq!(view.best_bid(), Some(PriceTicks(99)));
    }

    #[test]
    fn forward_jump_enters_gap_and_stops_applying() {
        let mut view = MarketView::new();
        snapshot(&mut view, 1);
        view.apply(
            9,
            MarketEventKind::LevelSet {
                side: Side::Buy,
                price: PriceTicks(98),
                quantity: QuantityLots(1),
            },
            20,
        );
        assert_eq!(view.state(), FeedState::Gap);
        assert_eq!(view.counters().gaps, 1);

        // A fresh snapshot recovers.
        snapshot(&mut view, 20);
        assert_eq!(view.state(), FeedState::Live);
    }

    #[test]
    fn reference_price_is_the_midpoint_when_both_sides_are_valid() {
        let mut view = MarketView::new();
        snapshot(&mut view, 1);
        assert_eq!(view.reference_price(), Some(PriceTicks(100)));
    }
}
