mod support;

use protocol::{
    ControlCommand, FeedState, MarketEventKind, OrderState, PriceTicks, QuantityLots, RejectReason,
    ReportKind, Side,
};
use support::{last_report, reports, trades, Harness};
use trading_core::EngineConfig;

#[test]
fn limit_order_rests_and_reserves_exposure() {
    let mut harness = Harness::new();
    harness.sync_feed();

    let report = last_report(harness.limit(1, 100, Side::Buy, 99, 10));
    assert_eq!(report.kind, ReportKind::Accepted);
    assert_eq!(report.state, OrderState::Working);
    assert_eq!(report.remaining, QuantityLots(10));

    let position = &harness.core.accounts()[0].positions[0];
    assert_eq!(position.open_buy_lots, 10);
    assert_eq!(position.open_buy_notional, 990);
    harness.check_invariants();
}

#[test]
fn crossing_limit_emits_trade_then_maker_then_taker() {
    let mut harness = Harness::new();
    harness.sync_feed();
    harness.limit(1, 100, Side::Sell, 101, 10);

    let events = harness.limit(2, 200, Side::Buy, 101, 4).to_vec();
    assert!(matches!(events[0], protocol::OutputEvent::Trade(_)));
    let reports = reports(&events);
    assert_eq!(reports[0].account, protocol::AccountId(1));
    assert_eq!(reports[0].kind, ReportKind::PartiallyFilled);
    assert_eq!(reports[1].account, protocol::AccountId(2));
    assert_eq!(reports[1].kind, ReportKind::Filled);

    let trade = trades(&events)[0];
    assert_eq!(trade.price, PriceTicks(101));
    assert_eq!(trade.quantity, QuantityLots(4));
    assert_eq!(trade.aggressor, Side::Buy);
    harness.check_invariants();
}

#[test]
fn positions_are_conserved_across_both_accounts() {
    let mut harness = Harness::new();
    harness.sync_feed();
    harness.limit(1, 100, Side::Sell, 101, 10);
    harness.limit(2, 200, Side::Buy, 101, 10);

    assert_eq!(harness.core.accounts()[0].positions[0].position_lots, -10);
    assert_eq!(harness.core.accounts()[1].positions[0].position_lots, 10);
    assert_eq!(harness.core.accounts()[0].positions[0].open_sell_lots, 0);
}

#[test]
fn market_order_never_rests() {
    let mut harness = Harness::new();
    harness.sync_feed();
    harness.limit(1, 100, Side::Sell, 101, 3);

    let events = harness.market_order(2, 200, Side::Buy, 10).to_vec();
    let final_report = last_report(&events);
    assert_eq!(final_report.kind, ReportKind::Cancelled);
    assert_eq!(final_report.cumulative_filled, QuantityLots(3));
    assert_eq!(final_report.remaining, QuantityLots(0));
    assert_eq!(harness.core.live_order_count(), 0);
}

#[test]
fn market_order_stops_at_the_protection_price() {
    let mut harness = Harness::new();
    harness.sync_feed();
    // Reference midpoint is 100 and protection is 50 ticks, so 200 is out of reach.
    harness.limit(1, 100, Side::Sell, 120, 5);
    harness.limit(1, 101, Side::Sell, 200, 5);

    let events = harness.market_order(2, 200, Side::Buy, 8).to_vec();
    assert_eq!(trades(&events).len(), 1);
    assert_eq!(last_report(&events).cumulative_filled, QuantityLots(5));
}

#[test]
fn market_order_is_rejected_without_synchronized_market_data() {
    let mut harness = Harness::new();
    let report = last_report(harness.market_order(1, 100, Side::Buy, 1));
    assert_eq!(
        report.reject_reason,
        Some(RejectReason::MarketDataUnsynchronized)
    );
}

#[test]
fn stale_market_data_rejects_new_orders() {
    let mut config = EngineConfig::single_instrument(1);
    config.max_market_age_ns = 1;
    let mut harness = Harness::with_config(config);
    harness.sync_feed();
    for _ in 0..5 {
        harness.market(MarketEventKind::Heartbeat);
    }
    let report = last_report(harness.limit(1, 100, Side::Buy, 99, 1));
    assert_eq!(report.reject_reason, Some(RejectReason::MarketDataStale));
}

#[test]
fn cancel_releases_the_residual_reservation() {
    let mut harness = Harness::new();
    harness.sync_feed();
    harness.limit(1, 100, Side::Sell, 101, 10);
    harness.limit(2, 200, Side::Buy, 101, 4);

    let report = last_report(harness.cancel(1, 100));
    assert_eq!(report.kind, ReportKind::Cancelled);
    assert_eq!(report.cumulative_filled, QuantityLots(4));

    let position = &harness.core.accounts()[0].positions[0];
    assert_eq!(position.open_sell_lots, 0);
    assert_eq!(position.open_sell_notional, 0);
    harness.check_invariants();
}

#[test]
fn cancelling_an_unknown_order_is_rejected() {
    let mut harness = Harness::new();
    harness.sync_feed();
    let report = last_report(harness.cancel(1, 999));
    assert_eq!(report.reject_reason, Some(RejectReason::UnknownOrder));
}

#[test]
fn replace_decrease_at_the_same_price_keeps_priority() {
    let mut harness = Harness::new();
    harness.sync_feed();
    harness.limit(1, 100, Side::Buy, 99, 10);
    harness.limit(2, 200, Side::Buy, 99, 10);

    let before = harness.core.instrument(support::INSTRUMENT).unwrap();
    let priority = before.book.level_orders(Side::Buy, PriceTicks(99))[0].priority;

    let report = last_report(harness.replace(1, 100, 99, 5));
    assert_eq!(report.kind, ReportKind::Replaced);
    assert_eq!(report.remaining, QuantityLots(5));

    let after = harness.core.instrument(support::INSTRUMENT).unwrap();
    let orders = after.book.level_orders(Side::Buy, PriceTicks(99));
    assert_eq!(orders[0].priority, priority);
    assert_eq!(orders[0].client_order_id, protocol::ClientOrderId(100));
    harness.check_invariants();
}

#[test]
fn replace_increase_loses_priority() {
    let mut harness = Harness::new();
    harness.sync_feed();
    harness.limit(1, 100, Side::Buy, 99, 10);
    harness.limit(2, 200, Side::Buy, 99, 10);

    harness.replace(1, 100, 99, 20);

    let book = &harness.core.instrument(support::INSTRUMENT).unwrap().book;
    let orders = book.level_orders(Side::Buy, PriceTicks(99));
    assert_eq!(orders[0].client_order_id, protocol::ClientOrderId(200));
    assert_eq!(orders[1].client_order_id, protocol::ClientOrderId(100));
    harness.check_invariants();
}

#[test]
fn replace_below_cumulative_fill_is_rejected_and_changes_nothing() {
    let mut harness = Harness::new();
    harness.sync_feed();
    harness.limit(1, 100, Side::Sell, 101, 10);
    harness.limit(2, 200, Side::Buy, 101, 6);

    let digest_before = trading_core::state_digest(&harness.core);
    let report = last_report(harness.replace(1, 100, 101, 3));
    assert_eq!(
        report.reject_reason,
        Some(RejectReason::InvalidReplaceQuantity)
    );

    let book = &harness.core.instrument(support::INSTRUMENT).unwrap().book;
    let order = book.level_orders(Side::Sell, PriceTicks(101))[0];
    assert_eq!(order.total_quantity, QuantityLots(10));
    assert_eq!(order.cumulative_filled, QuantityLots(6));
    assert_ne!(digest_before, trading_core::state_digest(&harness.core));
}

#[test]
fn replace_to_the_cumulative_fill_cancels_the_remainder() {
    let mut harness = Harness::new();
    harness.sync_feed();
    harness.limit(1, 100, Side::Sell, 101, 10);
    harness.limit(2, 200, Side::Buy, 101, 6);

    let report = last_report(harness.replace(1, 100, 101, 6));
    assert_eq!(report.kind, ReportKind::Cancelled);
    assert_eq!(harness.core.live_order_count(), 0);
    harness.check_invariants();
}

#[test]
fn replace_across_the_spread_matches_immediately() {
    let mut harness = Harness::new();
    harness.sync_feed();
    harness.limit(1, 100, Side::Sell, 105, 5);
    harness.limit(2, 200, Side::Buy, 100, 5);

    let events = harness.replace(2, 200, 105, 5).to_vec();
    assert_eq!(trades(&events).len(), 1);
    assert_eq!(last_report(&events).kind, ReportKind::Filled);
    assert_eq!(harness.core.live_order_count(), 0);
    harness.check_invariants();
}

#[test]
fn exact_retry_returns_the_recorded_result() {
    let mut harness = Harness::new();
    harness.sync_feed();
    let first = last_report(harness.limit(1, 100, Side::Buy, 99, 10));

    let mut retry = protocol::OrderRequest {
        kind: protocol::RequestKind::New,
        account: protocol::AccountId(1),
        instrument: support::INSTRUMENT,
        request_id: first.request_id,
        client_seq: 1,
        client_order_id: protocol::ClientOrderId(100),
        target_client_order_id: protocol::ClientOrderId(0),
        side: Side::Buy,
        order_type: protocol::OrderType::Limit,
        price: PriceTicks(99),
        quantity: QuantityLots(10),
    };
    let report = last_report(harness.apply(protocol::InputEvent::Order(retry)));
    assert_eq!(report.kind, ReportKind::Accepted);
    assert_eq!(report.order_id, first.order_id);
    assert_eq!(harness.core.live_order_count(), 1);

    // Same request ID with different content is a conflict.
    retry.quantity = QuantityLots(11);
    let report = last_report(harness.apply(protocol::InputEvent::Order(retry)));
    assert_eq!(
        report.reject_reason,
        Some(RejectReason::DuplicateRequestConflict)
    );
    assert_eq!(harness.core.live_order_count(), 1);
}

#[test]
fn duplicate_client_order_id_is_rejected() {
    let mut harness = Harness::new();
    harness.sync_feed();
    harness.limit(1, 100, Side::Buy, 99, 10);
    let report = last_report(harness.limit(1, 100, Side::Buy, 98, 10));
    assert_eq!(
        report.reject_reason,
        Some(RejectReason::DuplicateClientOrderId)
    );
}

#[test]
fn global_kill_blocks_new_orders_but_allows_cancel() {
    let mut harness = Harness::new();
    harness.sync_feed();
    harness.limit(1, 100, Side::Buy, 99, 10);
    harness.control(ControlCommand::SetGlobalKill { engaged: true });

    let report = last_report(harness.limit(1, 101, Side::Buy, 98, 1));
    assert_eq!(report.reject_reason, Some(RejectReason::GlobalKillActive));

    let report = last_report(harness.cancel(1, 100));
    assert_eq!(report.kind, ReportKind::Cancelled);
}

#[test]
fn disabled_account_cannot_send_new_orders() {
    let mut harness = Harness::new();
    harness.sync_feed();
    harness.control(ControlCommand::SetAccountEnabled {
        account: protocol::AccountId(1),
        enabled: false,
    });
    let report = last_report(harness.limit(1, 100, Side::Buy, 99, 1));
    assert_eq!(report.reject_reason, Some(RejectReason::AccountDisabled));
}

#[test]
fn risk_limits_reject_in_a_fixed_order() {
    let mut config = EngineConfig::single_instrument(3);
    config.accounts[0].limits.max_order_quantity = QuantityLots(5);
    config.accounts[0].limits.price_collar_ticks = 10;
    config.accounts[0].limits.max_position_lots = 4;
    let mut harness = Harness::with_config(config);
    harness.sync_feed();

    assert_eq!(
        last_report(harness.limit(1, 1, Side::Buy, 99, 6)).reject_reason,
        Some(RejectReason::MaxOrderQuantity)
    );
    assert_eq!(
        last_report(harness.limit(1, 2, Side::Buy, 80, 1)).reject_reason,
        Some(RejectReason::PriceCollar)
    );
    assert_eq!(
        last_report(harness.limit(1, 3, Side::Buy, 99, 5)).reject_reason,
        Some(RejectReason::MaxPosition)
    );
    assert_eq!(
        last_report(harness.limit(1, 4, Side::Buy, 99, 4)).kind,
        ReportKind::Accepted
    );
}

#[test]
fn capacity_exhaustion_rejects_without_mutating_state() {
    let mut config = EngineConfig::single_instrument(4);
    config.max_live_orders = 2;
    let mut harness = Harness::with_config(config);
    harness.sync_feed();

    harness.limit(1, 1, Side::Buy, 99, 1);
    harness.limit(1, 2, Side::Buy, 98, 1);
    let digest_before = trading_core::state_digest(&harness.core);
    let report = last_report(harness.limit(1, 3, Side::Buy, 97, 1));
    assert_eq!(report.reject_reason, Some(RejectReason::CapacityExhausted));
    assert_eq!(harness.core.live_order_count(), 2);
    assert_ne!(digest_before, trading_core::state_digest(&harness.core));
    harness.check_invariants();
}

#[test]
fn feed_gap_emits_a_state_event_and_blocks_new_orders() {
    let mut harness = Harness::new();
    harness.sync_feed();

    // Skip a source sequence.
    let events = harness
        .apply(protocol::InputEvent::Market(protocol::MarketEvent {
            instrument: support::INSTRUMENT,
            source_seq: 99,
            source_time_ns: 1,
            kind: MarketEventKind::LevelSet {
                side: Side::Buy,
                price: PriceTicks(98),
                quantity: QuantityLots(1),
            },
        }))
        .to_vec();

    assert!(matches!(
        events[0],
        protocol::OutputEvent::State(protocol::StateEvent {
            event: protocol::EngineStateEvent::FeedStateChanged {
                state: FeedState::Gap,
                ..
            },
            ..
        })
    ));
    let report = last_report(harness.limit(1, 1, Side::Buy, 99, 1));
    assert_eq!(
        report.reject_reason,
        Some(RejectReason::MarketDataUnsynchronized)
    );
}

#[test]
fn cancelled_orders_never_trade_again() {
    let mut harness = Harness::new();
    harness.sync_feed();
    harness.limit(1, 100, Side::Sell, 101, 10);
    harness.cancel(1, 100);

    let events = harness.limit(2, 200, Side::Buy, 101, 10).to_vec();
    assert!(trades(&events).is_empty());
    assert_eq!(last_report(&events).kind, ReportKind::Accepted);
}

#[test]
fn output_sequences_are_dense_and_increasing() {
    let mut harness = Harness::new();
    harness.sync_feed();
    harness.limit(1, 100, Side::Sell, 101, 10);
    harness.limit(2, 200, Side::Buy, 101, 4);
    harness.cancel(1, 100);

    let mut previous = 0;
    for event in &harness.events {
        let seq = event.output_seq().0;
        // A gap would mean an emitted event never reached a consumer.
        assert_eq!(seq, previous + 1, "output sequence {seq} skipped a value");
        previous = seq;
    }
}
