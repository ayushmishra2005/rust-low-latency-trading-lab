//! JSON projection of engine output. Wide integers become strings.

use engine_runtime::snapshot::EngineSnapshot;
use protocol::{OutputEvent, Side};
use serde_json::{json, Value};

fn side(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

pub fn output_event(event: &OutputEvent) -> Value {
    match event {
        OutputEvent::Trade(trade) => json!({
            "kind": "trade",
            "outputSeq": trade.output_seq.0.to_string(),
            "engineSeq": trade.engine_seq.0.to_string(),
            "engineTimeNs": trade.engine_time_ns.to_string(),
            "tradeId": trade.trade_id.0.to_string(),
            "instrument": trade.instrument.0,
            "makerOrderId": trade.maker_order_id.0.to_string(),
            "takerOrderId": trade.taker_order_id.0.to_string(),
            "makerAccount": trade.maker_account.0,
            "takerAccount": trade.taker_account.0,
            "aggressor": side(trade.aggressor),
            "priceTicks": trade.price.0.to_string(),
            "quantityLots": trade.quantity.0.to_string(),
        }),
        OutputEvent::Report(report) => json!({
            "kind": "report",
            "outputSeq": report.output_seq.0.to_string(),
            "engineSeq": report.engine_seq.0.to_string(),
            "engineTimeNs": report.engine_time_ns.to_string(),
            "account": report.account.0,
            "instrument": report.instrument.0,
            "requestId": report.request_id.0.to_string(),
            "clientOrderId": report.client_order_id.0.to_string(),
            "orderId": report.order_id.0.to_string(),
            "reportKind": format!("{:?}", report.kind).to_lowercase(),
            "orderState": format!("{:?}", report.state).to_lowercase(),
            "side": side(report.side),
            "orderType": format!("{:?}", report.order_type).to_lowercase(),
            "priceTicks": report.price.0.to_string(),
            "totalQuantityLots": report.total_quantity.0.to_string(),
            "cumulativeFilledLots": report.cumulative_filled.0.to_string(),
            "remainingLots": report.remaining.0.to_string(),
            "lastFillQuantityLots": report.last_fill_quantity.0.to_string(),
            "lastFillPriceTicks": report.last_fill_price.0.to_string(),
            "rejectReason": report.reject_reason.map(|reason| reason.label()),
        }),
        OutputEvent::State(state) => json!({
            "kind": "state",
            "outputSeq": state.output_seq.0.to_string(),
            "engineSeq": state.engine_seq.0.to_string(),
            "engineTimeNs": state.engine_time_ns.to_string(),
            "event": state_event(state.event),
        }),
    }
}

fn state_event(event: protocol::EngineStateEvent) -> Value {
    use protocol::EngineStateEvent as Event;
    match event {
        Event::KillSwitchEngaged => json!({ "type": "killSwitchEngaged" }),
        Event::KillSwitchReleased => json!({ "type": "killSwitchReleased" }),
        Event::AccountEnabledChanged { account, enabled } => json!({
            "type": "accountEnabledChanged",
            "account": account.0,
            "enabled": enabled,
        }),
        Event::AccountLimitsChanged { account } => json!({
            "type": "accountLimitsChanged",
            "account": account.0,
        }),
        Event::FeedStateChanged { instrument, state } => json!({
            "type": "feedStateChanged",
            "instrument": instrument.0,
            "feedState": state.label(),
        }),
    }
}

pub fn snapshot(snapshot: &EngineSnapshot) -> Value {
    let instruments: Vec<Value> = snapshot
        .instruments
        .iter()
        .map(|instrument| {
            json!({
                "instrument": instrument.instrument.0,
                "symbol": instrument.symbol,
                "feedState": instrument.feed_state.label(),
                "lastSourceSeq": instrument.last_source_seq.to_string(),
                "marketBestBidTicks": instrument.best_bid.map(|price| price.0.to_string()),
                "marketBestAskTicks": instrument.best_ask.map(|price| price.0.to_string()),
                "bookLevels": instrument.levels.iter().map(|level| json!({
                    "side": side(level.side),
                    "priceTicks": level.price.0.to_string(),
                    "quantityLots": level.quantity.0.to_string(),
                    "orderCount": level.order_count,
                })).collect::<Vec<_>>(),
                "bookOrders": instrument.orders.iter().map(|order| json!({
                    "orderId": order.order_id.0.to_string(),
                    "account": order.account.0,
                    "clientOrderId": order.client_order_id.0.to_string(),
                    "side": side(order.side),
                    "priceTicks": order.price.0.to_string(),
                    "totalQuantityLots": order.total_quantity.0.to_string(),
                    "cumulativeFilledLots": order.cumulative_filled.0.to_string(),
                })).collect::<Vec<_>>(),
            })
        })
        .collect();

    let positions: Vec<Value> = snapshot
        .positions
        .iter()
        .map(|position| {
            json!({
                "account": position.account.0,
                "instrument": position.instrument.0,
                "positionLots": position.position_lots.to_string(),
                "openBuyLots": position.open_buy_lots.to_string(),
                "openSellLots": position.open_sell_lots.to_string(),
                "openBuyNotional": position.open_buy_notional.to_string(),
                "openSellNotional": position.open_sell_notional.to_string(),
            })
        })
        .collect();

    json!({
        "runId": snapshot.run_id.to_string(),
        "asOfEngineSeq": snapshot.as_of_engine_seq.0.to_string(),
        "engineTimeNs": snapshot.engine_time_ns.to_string(),
        "globalKill": snapshot.global_kill,
        "metrics": {
            "inputsApplied": snapshot.metrics.inputs_applied.to_string(),
            "requestsAccepted": snapshot.metrics.requests_accepted.to_string(),
            "requestsRejected": snapshot.metrics.requests_rejected.to_string(),
            "reportsEmitted": snapshot.metrics.reports_emitted.to_string(),
            "trades": snapshot.metrics.trades.to_string(),
            "marketEvents": snapshot.metrics.market_events.to_string(),
            "controlEvents": snapshot.metrics.control_events.to_string(),
        },
        "instruments": instruments,
        "positions": positions,
    })
}
