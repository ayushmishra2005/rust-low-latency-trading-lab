//! Seeded deterministic generator for market data and order requests.
//!
//! The same seed and configuration always produce the same normalized event
//! sequence. This is a workload generator, not a trading strategy.

use std::collections::BTreeMap;

use protocol::codec::{Frame, FrameBody};
use protocol::{
    AccountId, ClientOrderId, EngineInput, IngressSeq, InputEvent, InstrumentId, MarketEvent,
    MarketEventKind, OrderRequest, OrderType, PriceTicks, QuantityLots, RequestId, RequestKind,
    Side,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratorConfig {
    pub seed: u64,
    pub instrument: InstrumentId,
    pub accounts: Vec<AccountId>,
    /// Normalized events to produce after the opening snapshot.
    pub events: usize,
    pub start_price_ticks: i64,
    pub snapshot_levels: u32,
    /// Percentage of events that are order requests rather than market data.
    pub order_percent: u32,
    pub market_order_percent: u32,
    pub cancel_percent: u32,
    pub replace_percent: u32,
    pub inject_gaps: bool,
    pub inject_duplicates: bool,
    pub inter_arrival_ns: u64,
}

impl GeneratorConfig {
    pub fn new(seed: u64, events: usize) -> GeneratorConfig {
        GeneratorConfig {
            seed,
            instrument: InstrumentId(1),
            accounts: vec![AccountId(1), AccountId(2)],
            events,
            start_price_ticks: 10_000,
            snapshot_levels: 10,
            order_percent: 60,
            market_order_percent: 10,
            cancel_percent: 20,
            replace_percent: 15,
            inject_gaps: false,
            inject_duplicates: false,
            inter_arrival_ns: 1_000,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct LiveClientOrder {
    account_slot: usize,
    client_order_id: ClientOrderId,
    side: Side,
    price: PriceTicks,
    quantity: u64,
}

pub struct Generator {
    config: GeneratorConfig,
    rng: SplitMix64,
    ingress: u64,
    source_seq: u64,
    time_ns: u64,
    request_id: u64,
    client_order_id: u64,
    client_seq: Vec<u64>,
    live: Vec<LiveClientOrder>,
    mid: i64,
    // Published depth, so the generated market never crosses itself.
    bids: BTreeMap<i64, u64>,
    asks: BTreeMap<i64, u64>,
}

impl Generator {
    pub fn new(config: GeneratorConfig) -> Generator {
        let seed = config.seed;
        let accounts = config.accounts.len();
        let mid = config.start_price_ticks;
        Generator {
            config,
            rng: SplitMix64::new(seed),
            ingress: 0,
            source_seq: 0,
            time_ns: 1_000_000,
            request_id: 0,
            client_order_id: 0,
            client_seq: vec![0; accounts],
            live: Vec::new(),
            mid,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    pub fn generate(&mut self) -> Vec<EngineInput> {
        let mut inputs = Vec::with_capacity(self.config.events + 32);
        self.emit_snapshot(&mut inputs);
        for _ in 0..self.config.events {
            let roll = self.rng.below(100);
            if roll < u64::from(self.config.order_percent) {
                self.emit_order(&mut inputs);
            } else {
                self.emit_market(&mut inputs);
            }
        }
        inputs
    }

    fn push(&mut self, event: InputEvent, inputs: &mut Vec<EngineInput>) {
        self.ingress += 1;
        self.time_ns += self.config.inter_arrival_ns;
        inputs.push(EngineInput {
            ingress_seq: IngressSeq(self.ingress),
            recv_time_ns: self.time_ns,
            event,
        });
    }

    fn push_market(&mut self, kind: MarketEventKind, inputs: &mut Vec<EngineInput>) {
        match kind {
            MarketEventKind::SnapshotLevel {
                side,
                price,
                quantity,
            }
            | MarketEventKind::LevelSet {
                side,
                price,
                quantity,
            } => {
                let levels = match side {
                    Side::Buy => &mut self.bids,
                    Side::Sell => &mut self.asks,
                };
                if quantity.0 == 0 {
                    levels.remove(&price.0);
                } else {
                    levels.insert(price.0, quantity.0);
                }
            }
            _ => {}
        }
        self.source_seq += 1;
        let event = InputEvent::Market(MarketEvent {
            instrument: self.config.instrument,
            source_seq: self.source_seq,
            source_time_ns: self.time_ns,
            kind,
        });
        self.push(event, inputs);
    }

    fn emit_snapshot(&mut self, inputs: &mut Vec<EngineInput>) {
        let snapshot_seq = self.source_seq + 1;
        self.push_market(MarketEventKind::SnapshotBegin { snapshot_seq }, inputs);
        let levels = self.config.snapshot_levels;
        for depth in 0..levels {
            let offset = i64::from(depth) + 1;
            let quantity = QuantityLots(10 + u64::from(depth));
            self.push_market(
                MarketEventKind::SnapshotLevel {
                    side: Side::Buy,
                    price: PriceTicks(self.mid - offset),
                    quantity,
                },
                inputs,
            );
            self.push_market(
                MarketEventKind::SnapshotLevel {
                    side: Side::Sell,
                    price: PriceTicks(self.mid + offset),
                    quantity,
                },
                inputs,
            );
        }
        self.push_market(
            MarketEventKind::SnapshotEnd {
                snapshot_seq,
                level_count: levels * 2,
            },
            inputs,
        );
    }

    fn emit_market(&mut self, inputs: &mut Vec<EngineInput>) {
        // Random walk of one tick keeps the reference price moving deterministically.
        if self.rng.below(2) == 0 {
            self.mid += 1;
        } else {
            self.mid -= 1;
        }

        if self.config.inject_duplicates && self.rng.below(50) == 0 {
            // Repeat the previous sequence; the market view must ignore it.
            self.source_seq -= 1;
        }
        if self.config.inject_gaps && self.rng.below(200) == 0 {
            self.source_seq += 3;
        }

        let roll = self.rng.below(10);
        if roll < 6 {
            let side = if self.rng.below(2) == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let depth = i64::try_from(self.rng.below(5)).unwrap_or(0) + 1;
            let price = match side {
                Side::Buy => self.mid - depth,
                Side::Sell => self.mid + depth,
            };
            let quantity = QuantityLots(self.rng.below(50));
            if quantity.0 > 0 {
                self.clear_crossing(side, price, inputs);
            }
            self.push_market(
                MarketEventKind::LevelSet {
                    side,
                    price: PriceTicks(price),
                    quantity,
                },
                inputs,
            );
            return;
        }

        let kind = if roll < 9 {
            let side = if self.rng.below(2) == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            MarketEventKind::Trade {
                aggressor: side,
                price: PriceTicks(self.mid),
                quantity: QuantityLots(1 + self.rng.below(5)),
            }
        } else {
            MarketEventKind::Heartbeat
        };
        self.push_market(kind, inputs);
    }

    /// Removes opposite-side levels that a new level would trade through.
    fn clear_crossing(&mut self, side: Side, price: i64, inputs: &mut Vec<EngineInput>) {
        let crossed: Vec<i64> = match side {
            Side::Buy => self.asks.range(..=price).map(|(key, _)| *key).collect(),
            Side::Sell => self.bids.range(price..).map(|(key, _)| *key).collect(),
        };
        let other = match side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        for level in crossed {
            self.push_market(
                MarketEventKind::LevelSet {
                    side: other,
                    price: PriceTicks(level),
                    quantity: QuantityLots(0),
                },
                inputs,
            );
        }
    }

    fn emit_order(&mut self, inputs: &mut Vec<EngineInput>) {
        let account_slot = (self.rng.below(self.config.accounts.len() as u64)) as usize;
        let account = self.config.accounts[account_slot];
        self.request_id += 1;
        self.client_seq[account_slot] += 1;

        let mut request = OrderRequest {
            kind: RequestKind::New,
            account,
            instrument: self.config.instrument,
            request_id: RequestId(self.request_id),
            client_seq: self.client_seq[account_slot],
            client_order_id: ClientOrderId(0),
            target_client_order_id: ClientOrderId(0),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: PriceTicks(0),
            quantity: QuantityLots(0),
        };

        let roll = self.rng.below(100);
        let cancel_bound = u64::from(self.config.cancel_percent);
        let replace_bound = cancel_bound + u64::from(self.config.replace_percent);

        if roll < cancel_bound && !self.live.is_empty() {
            let index = (self.rng.below(self.live.len() as u64)) as usize;
            let target = self.live.swap_remove(index);
            request.kind = RequestKind::Cancel;
            request.account = self.config.accounts[target.account_slot];
            request.client_seq = {
                self.client_seq[target.account_slot] += 1;
                self.client_seq[target.account_slot]
            };
            request.target_client_order_id = target.client_order_id;
        } else if roll < replace_bound && !self.live.is_empty() {
            let index = (self.rng.below(self.live.len() as u64)) as usize;
            let target = self.live[index];
            request.kind = RequestKind::Replace;
            request.account = self.config.accounts[target.account_slot];
            request.client_seq = {
                self.client_seq[target.account_slot] += 1;
                self.client_seq[target.account_slot]
            };
            request.target_client_order_id = target.client_order_id;
            // A replace keeps the side and type of the resting order.
            request.side = target.side;
            request.price = target.price;
            let new_total = 1 + self.rng.below(target.quantity.max(1) + 5);
            request.quantity = QuantityLots(new_total);
            self.live[index].quantity = new_total;
        } else {
            self.client_order_id += 1;
            let client_order_id = ClientOrderId(self.client_order_id);
            let side = if self.rng.below(2) == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let quantity = 1 + self.rng.below(10);
            request.client_order_id = client_order_id;
            request.side = side;
            request.quantity = QuantityLots(quantity);

            if self.rng.below(100) < u64::from(self.config.market_order_percent) {
                request.order_type = OrderType::Market;
            } else {
                let offset = i64::try_from(self.rng.below(6)).unwrap_or(0) - 2;
                let price = match side {
                    Side::Buy => self.mid - 1 + offset,
                    Side::Sell => self.mid + 1 + offset,
                };
                request.price = PriceTicks(price.max(1));
                if self.live.len() < 512 {
                    self.live.push(LiveClientOrder {
                        account_slot,
                        client_order_id,
                        side,
                        price: request.price,
                        quantity,
                    });
                }
            }
        }

        self.push(InputEvent::Order(request), inputs);
    }
}

/// Converts a normalized input into a wire frame. Control inputs have no v1 frame.
pub fn to_frame(input: &EngineInput) -> Option<Frame> {
    let (source_seq, source_time_ns, instrument, body) = match input.event {
        InputEvent::Market(event) => {
            let body = match event.kind {
                MarketEventKind::SnapshotBegin { snapshot_seq } => {
                    FrameBody::SnapshotBegin { snapshot_seq }
                }
                MarketEventKind::SnapshotLevel {
                    side,
                    price,
                    quantity,
                } => FrameBody::SnapshotLevel {
                    side,
                    price,
                    quantity,
                },
                MarketEventKind::SnapshotEnd {
                    snapshot_seq,
                    level_count,
                } => FrameBody::SnapshotEnd {
                    snapshot_seq,
                    level_count,
                    state_crc: 0,
                },
                MarketEventKind::LevelSet {
                    side,
                    price,
                    quantity,
                } => FrameBody::LevelSet {
                    side,
                    price,
                    quantity,
                },
                MarketEventKind::Trade {
                    aggressor,
                    price,
                    quantity,
                } => FrameBody::MarketTrade {
                    aggressor,
                    price,
                    quantity,
                },
                MarketEventKind::Heartbeat => FrameBody::Heartbeat,
                MarketEventKind::FeedReset { new_epoch } => FrameBody::FeedReset { new_epoch },
            };
            (
                event.source_seq,
                event.source_time_ns,
                event.instrument,
                body,
            )
        }
        InputEvent::Order(request) => (
            0,
            input.recv_time_ns,
            request.instrument,
            FrameBody::OrderRequest(request),
        ),
        InputEvent::Control(_) => return None,
    };

    Some(Frame {
        source_seq,
        source_time_ns,
        recv_time_ns: input.recv_time_ns,
        instrument,
        body,
    })
}

/// Converts a decoded frame back into a normalized input.
pub fn from_frame(frame: &Frame, ingress_seq: IngressSeq) -> EngineInput {
    let event = match frame.body {
        FrameBody::OrderRequest(request) => InputEvent::Order(request),
        _ => {
            let kind = match frame.body {
                FrameBody::SnapshotBegin { snapshot_seq } => {
                    MarketEventKind::SnapshotBegin { snapshot_seq }
                }
                FrameBody::SnapshotLevel {
                    side,
                    price,
                    quantity,
                } => MarketEventKind::SnapshotLevel {
                    side,
                    price,
                    quantity,
                },
                FrameBody::SnapshotEnd {
                    snapshot_seq,
                    level_count,
                    ..
                } => MarketEventKind::SnapshotEnd {
                    snapshot_seq,
                    level_count,
                },
                FrameBody::LevelSet {
                    side,
                    price,
                    quantity,
                } => MarketEventKind::LevelSet {
                    side,
                    price,
                    quantity,
                },
                FrameBody::MarketTrade {
                    aggressor,
                    price,
                    quantity,
                } => MarketEventKind::Trade {
                    aggressor,
                    price,
                    quantity,
                },
                FrameBody::Heartbeat => MarketEventKind::Heartbeat,
                FrameBody::FeedReset { new_epoch } => MarketEventKind::FeedReset { new_epoch },
                FrameBody::OrderRequest(_) => unreachable!("handled above"),
            };
            InputEvent::Market(MarketEvent {
                instrument: frame.instrument,
                source_seq: frame.source_seq,
                source_time_ns: frame.source_time_ns,
                kind,
            })
        }
    };
    EngineInput {
        ingress_seq,
        recv_time_ns: frame.recv_time_ns,
        event,
    }
}

/// SplitMix64. Small, deterministic, and adequate for workload generation.
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> SplitMix64 {
        SplitMix64 { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            return 0;
        }
        self.next() % bound
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_produces_the_same_sequence() {
        let config = GeneratorConfig::new(42, 500);
        let first = Generator::new(config.clone()).generate();
        let second = Generator::new(config).generate();
        assert_eq!(first, second);
    }

    #[test]
    fn different_seeds_produce_different_sequences() {
        let first = Generator::new(GeneratorConfig::new(1, 200)).generate();
        let second = Generator::new(GeneratorConfig::new(2, 200)).generate();
        assert_ne!(first, second);
    }

    #[test]
    fn frames_round_trip_through_the_wire_format() {
        let inputs = Generator::new(GeneratorConfig::new(9, 300)).generate();
        for input in &inputs {
            let frame = to_frame(input).expect("generated inputs are representable");
            let mut bytes = Vec::new();
            frame.encode(&mut bytes);
            let (decoded, _) = Frame::decode(&bytes).unwrap();
            let restored = from_frame(&decoded, input.ingress_seq);
            match (input.event, restored.event) {
                // Order requests are not sequenced on the wire in v1.
                (InputEvent::Order(a), InputEvent::Order(b)) => assert_eq!(a, b),
                (a, b) => assert_eq!(a, b),
            }
            assert_eq!(restored.recv_time_ns, input.recv_time_ns);
        }
    }
}
