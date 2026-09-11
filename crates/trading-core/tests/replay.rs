//! Replay determinism and post-run accounting invariants over a seeded workload.

use std::collections::HashMap;

use protocol::{OutputEvent, QuantityLots, Side};
use trading_core::{EngineConfig, Generator, GeneratorConfig, Replay, TradingCore};

fn run(seed: u64, events: usize) -> (Replay, Vec<OutputEvent>) {
    let inputs = Generator::new(GeneratorConfig::new(seed, events)).generate();
    let mut replay = Replay::new(TradingCore::new(EngineConfig::single_instrument(11)), 64);
    let mut collected = Vec::new();
    for input in &inputs {
        collected.extend_from_slice(replay.step(input));
    }
    (replay, collected)
}

/// Applies market events to a fresh core and returns its state digest.
fn market_state_digest(kinds: &[protocol::MarketEventKind]) -> [u8; 32] {
    use protocol::{EngineInput, IngressSeq, InputEvent, InstrumentId, MarketEvent};

    let mut core = TradingCore::new(EngineConfig::single_instrument(9));
    let mut out = Vec::new();
    for (index, kind) in kinds.iter().enumerate() {
        let seq = index as u64 + 1;
        let input = EngineInput {
            ingress_seq: IngressSeq(seq),
            recv_time_ns: seq * 1_000,
            event: InputEvent::Market(MarketEvent {
                instrument: InstrumentId(1),
                source_seq: seq,
                source_time_ns: seq * 1_000,
                kind: *kind,
            }),
        };
        core.apply(&input, &mut out);
    }
    trading_core::state_digest(&core)
}

#[test]
fn the_state_digest_covers_the_last_trade_price() {
    use protocol::{MarketEventKind, PriceTicks};

    let with = |price: i64| {
        market_state_digest(&[
            MarketEventKind::SnapshotBegin { snapshot_seq: 1 },
            MarketEventKind::SnapshotEnd {
                snapshot_seq: 1,
                level_count: 0,
            },
            MarketEventKind::Trade {
                aggressor: Side::Buy,
                price: PriceTicks(price),
                quantity: QuantityLots(1),
            },
        ])
    };
    assert_ne!(with(100), with(101));
    assert_eq!(with(100), with(100));
}

#[test]
fn the_state_digest_covers_an_unfinished_snapshot() {
    use protocol::{MarketEventKind, PriceTicks};

    let with = |price: i64| {
        market_state_digest(&[
            MarketEventKind::SnapshotBegin { snapshot_seq: 1 },
            MarketEventKind::SnapshotLevel {
                side: Side::Buy,
                price: PriceTicks(price),
                quantity: QuantityLots(10),
            },
        ])
    };
    // Neither level is visible yet, but they decide the book at snapshot end.
    assert_ne!(with(99), with(98));
}

#[test]
fn the_state_digest_separates_pending_levels_from_visible_ones() {
    use protocol::{MarketEventKind, PriceTicks};

    let level = MarketEventKind::SnapshotLevel {
        side: Side::Buy,
        price: PriceTicks(99),
        quantity: QuantityLots(10),
    };
    let pending = market_state_digest(&[MarketEventKind::SnapshotBegin { snapshot_seq: 1 }, level]);
    let visible = market_state_digest(&[
        MarketEventKind::SnapshotBegin { snapshot_seq: 1 },
        level,
        MarketEventKind::SnapshotEnd {
            snapshot_seq: 1,
            level_count: 1,
        },
    ]);
    assert_ne!(pending, visible);
}

#[test]
fn identical_input_produces_identical_output_and_state() {
    let (first, first_events) = run(2024, 5_000);
    let (second, second_events) = run(2024, 5_000);

    let a = first.finish();
    let b = second.finish();
    assert_eq!(a.input_digest, b.input_digest);
    assert_eq!(a.output_digest, b.output_digest);
    assert_eq!(a.state_digest, b.state_digest);
    assert_eq!(a.checkpoints, b.checkpoints);
    assert_eq!(first_events, second_events);
    assert!(a.outputs > 0, "workload produced no output");
}

#[test]
fn a_different_seed_changes_the_digests() {
    let (first, _) = run(1, 2_000);
    let (second, _) = run(2, 2_000);
    assert_ne!(first.finish().output_digest, second.finish().output_digest);
}

#[test]
fn checkpoints_locate_the_first_divergence() {
    let (first, _) = run(7, 1_000);
    let (second, _) = run(7, 1_000);
    let a = first.finish();
    let b = second.finish();
    assert!(!a.checkpoints.is_empty());
    for (left, right) in a.checkpoints.iter().zip(&b.checkpoints) {
        assert_eq!(left.engine_seq, right.engine_seq);
        assert_eq!(left.state, right.state);
    }
}

fn assert_reservations_match_live_orders(core: &TradingCore) {
    let mut expected: HashMap<(u32, usize, Side), (u64, u128)> = HashMap::new();
    for (instrument_index, instrument) in core.instruments().iter().enumerate() {
        for (side, price, orders) in instrument.book.snapshot() {
            for order in orders {
                let remaining = order.remaining().0;
                let notional = u128::from(remaining) * u128::from(price.0.unsigned_abs());
                let entry = expected
                    .entry((order.account.0, instrument_index, side))
                    .or_default();
                entry.0 += remaining;
                entry.1 += notional;
            }
        }
    }
    for account in core.accounts() {
        for (instrument_index, position) in account.positions.iter().enumerate() {
            let buys = expected
                .get(&(account.id.0, instrument_index, Side::Buy))
                .copied()
                .unwrap_or((0, 0));
            let sells = expected
                .get(&(account.id.0, instrument_index, Side::Sell))
                .copied()
                .unwrap_or((0, 0));
            assert_eq!(position.open_buy_lots, buys.0, "buy reservation mismatch");
            assert_eq!(
                position.open_sell_lots, sells.0,
                "sell reservation mismatch"
            );
            assert_eq!(position.open_buy_notional, buys.1, "buy notional mismatch");
            assert_eq!(
                position.open_sell_notional, sells.1,
                "sell notional mismatch"
            );
        }
    }
}

fn assert_positions_match_fills(core: &TradingCore, fills: &HashMap<(u32, u32), i64>) {
    for account in core.accounts() {
        for (instrument_index, position) in account.positions.iter().enumerate() {
            let instrument = core.instruments()[instrument_index].config.id.0;
            let expected = fills.get(&(account.id.0, instrument)).copied().unwrap_or(0);
            assert_eq!(
                position.position_lots, expected,
                "position {} on {} != signed fills",
                account.id.0, instrument
            );
        }
    }
}

#[test]
fn accounting_holds_after_every_command() {
    let inputs = Generator::new(GeneratorConfig::new(31, 3_000)).generate();
    let mut core = TradingCore::new(EngineConfig::single_instrument(11));
    let mut out = Vec::new();
    let mut fills: HashMap<(u32, u32), i64> = HashMap::new();
    for input in &inputs {
        core.apply(input, &mut out);
        assert!(core.fault().is_none(), "engine faulted mid-run");
        for event in &out {
            if let OutputEvent::Trade(trade) = event {
                let qty = i64::try_from(trade.quantity.0).expect("fill fits i64");
                let signed = match trade.aggressor {
                    Side::Buy => qty,
                    Side::Sell => -qty,
                };
                *fills
                    .entry((trade.taker_account.0, trade.instrument.0))
                    .or_default() += signed;
                *fills
                    .entry((trade.maker_account.0, trade.instrument.0))
                    .or_default() -= signed;
            }
        }
        assert_reservations_match_live_orders(&core);
        assert_positions_match_fills(&core, &fills);
    }
}

#[test]
fn reservations_match_live_orders_and_positions_net_to_zero() {
    let (replay, events) = run(31, 8_000);
    let core = replay.core();

    for instrument in core.instruments() {
        instrument.book.check_invariants().expect("book invariants");
    }

    // Working reservations must equal the live remaining exposure they represent.
    let mut expected: HashMap<(u32, usize, Side), u64> = HashMap::new();
    for (instrument_index, instrument) in core.instruments().iter().enumerate() {
        for (side, price, orders) in instrument.book.snapshot() {
            let _ = price;
            for order in orders {
                *expected
                    .entry((order.account.0, instrument_index, side))
                    .or_default() += order.remaining().0;
            }
        }
    }
    for account in core.accounts() {
        for (instrument_index, position) in account.positions.iter().enumerate() {
            let buys = expected
                .get(&(account.id.0, instrument_index, Side::Buy))
                .copied()
                .unwrap_or(0);
            let sells = expected
                .get(&(account.id.0, instrument_index, Side::Sell))
                .copied()
                .unwrap_or(0);
            assert_eq!(position.open_buy_lots, buys, "buy reservation mismatch");
            assert_eq!(position.open_sell_lots, sells, "sell reservation mismatch");
        }
    }

    // The simulator is closed, so every fill has an equal and opposite side.
    let net: i64 = core
        .accounts()
        .iter()
        .flat_map(|account| account.positions.iter())
        .map(|position| position.position_lots)
        .sum();
    assert_eq!(net, 0);

    let traded: u64 = events
        .iter()
        .filter_map(|event| match event {
            OutputEvent::Trade(trade) => Some(trade.quantity.0),
            _ => None,
        })
        .sum();
    assert!(traded > 0, "workload produced no trades");
}

#[test]
fn cumulative_fill_never_exceeds_the_accepted_total() {
    let (_, events) = run(55, 5_000);
    for event in &events {
        if let OutputEvent::Report(report) = event {
            assert!(report.cumulative_filled <= report.total_quantity);
            if report.reject_reason.is_none() && !report.state.is_terminal() {
                assert_eq!(
                    report.remaining,
                    QuantityLots(report.total_quantity.0 - report.cumulative_filled.0)
                );
            }
        }
        if let OutputEvent::Trade(trade) = event {
            assert!(!trade.quantity.is_zero());
        }
    }
}

#[test]
fn gaps_and_duplicates_do_not_break_determinism() {
    let mut config = GeneratorConfig::new(99, 4_000);
    config.inject_gaps = true;
    config.inject_duplicates = true;
    let inputs = Generator::new(config).generate();

    let digests: Vec<_> = (0..2)
        .map(|_| {
            let mut replay =
                Replay::new(TradingCore::new(EngineConfig::single_instrument(11)), 128);
            replay.run(&inputs);
            replay.finish()
        })
        .collect();

    assert_eq!(digests[0], digests[1]);
}
