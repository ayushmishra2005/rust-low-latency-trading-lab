//! `TradingCore` is the pure deterministic state machine.
//!
//! It owns the market view, the matching book, risk state, positions, IDs, and
//! sequences. It never touches the clock, the filesystem, the network, or any
//! async runtime: the caller supplies logical time with every input.

use std::collections::HashMap;

use protocol::{
    AccountId, ClientOrderId, ControlCommand, EngineInput, EngineSeq, EngineStateEvent,
    ExecutionReport, InputEvent, InstrumentId, MarketEvent, OrderId, OrderRequest, OrderState,
    OrderType, OutputEvent, OutputSeq, PriceTicks, PrioritySeq, QuantityLots, RejectReason,
    ReportKind, RequestKind, Side, StateEvent, TradeEvent, TradeId,
};

use crate::account::{
    apply_fill_to_position, request_fingerprint, AccountState, CacheHit, InstrumentPosition,
};
use crate::book::{NewOrder, OrderBook};
use crate::config::EngineConfig;
use crate::market::MarketView;
use crate::matching::{self, Fill};

pub struct InstrumentRuntime {
    pub config: crate::config::InstrumentConfig,
    pub market: MarketView,
    pub book: OrderBook,
}

#[derive(Debug, Clone, Copy)]
struct LiveOrder {
    account_index: usize,
    instrument_index: usize,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EngineMetrics {
    pub inputs_applied: u64,
    pub requests_accepted: u64,
    pub requests_rejected: u64,
    pub reports_emitted: u64,
    pub trades: u64,
    pub market_events: u64,
    pub control_events: u64,
}

/// Fatal conditions. These stop the run instead of silently wrapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineFault {
    SequenceExhausted,
}

pub struct TradingCore {
    config: EngineConfig,
    instruments: Vec<InstrumentRuntime>,
    instrument_index: HashMap<InstrumentId, usize>,
    accounts: Vec<AccountState>,
    account_index: HashMap<AccountId, usize>,
    live_orders: HashMap<OrderId, LiveOrder>,
    client_orders: HashMap<(AccountId, ClientOrderId), OrderId>,
    global_kill: bool,
    engine_seq: EngineSeq,
    output_seq: OutputSeq,
    next_order_id: OrderId,
    next_trade_id: TradeId,
    next_priority: PrioritySeq,
    engine_time_ns: u64,
    fills: Vec<Fill>,
    metrics: EngineMetrics,
    fault: Option<EngineFault>,
}

impl TradingCore {
    pub fn new(config: EngineConfig) -> TradingCore {
        let instruments: Vec<InstrumentRuntime> = config
            .instruments
            .iter()
            .map(|instrument| InstrumentRuntime {
                config: instrument.clone(),
                market: MarketView::new(),
                book: OrderBook::with_capacity(config.max_live_orders),
            })
            .collect();
        let instrument_index = instruments
            .iter()
            .enumerate()
            .map(|(index, runtime)| (runtime.config.id, index))
            .collect();
        let accounts: Vec<AccountState> = config
            .accounts
            .iter()
            .map(|account| {
                AccountState::new(
                    account.id,
                    account.enabled,
                    account.limits,
                    instruments.len(),
                    config.dedup_window,
                )
            })
            .collect();
        let account_index = accounts
            .iter()
            .enumerate()
            .map(|(index, account)| (account.id, index))
            .collect();

        TradingCore {
            instruments,
            instrument_index,
            accounts,
            account_index,
            live_orders: HashMap::with_capacity(config.max_live_orders),
            client_orders: HashMap::with_capacity(config.max_live_orders),
            global_kill: false,
            engine_seq: EngineSeq(0),
            output_seq: OutputSeq(0),
            next_order_id: OrderId(0),
            next_trade_id: TradeId(0),
            next_priority: PrioritySeq(0),
            engine_time_ns: 0,
            fills: Vec::with_capacity(64),
            metrics: EngineMetrics::default(),
            fault: None,
            config,
        }
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    pub fn engine_seq(&self) -> EngineSeq {
        self.engine_seq
    }

    pub fn output_seq(&self) -> OutputSeq {
        self.output_seq
    }

    pub fn engine_time_ns(&self) -> u64 {
        self.engine_time_ns
    }

    pub fn global_kill(&self) -> bool {
        self.global_kill
    }

    pub fn metrics(&self) -> EngineMetrics {
        self.metrics
    }

    pub fn fault(&self) -> Option<EngineFault> {
        self.fault
    }

    pub fn instruments(&self) -> &[InstrumentRuntime] {
        &self.instruments
    }

    pub fn accounts(&self) -> &[AccountState] {
        &self.accounts
    }

    pub fn live_order_count(&self) -> usize {
        self.live_orders.len()
    }

    pub fn instrument(&self, id: InstrumentId) -> Option<&InstrumentRuntime> {
        self.instrument_index
            .get(&id)
            .map(|index| &self.instruments[*index])
    }

    pub fn next_ids(&self) -> (OrderId, TradeId, PrioritySeq) {
        (self.next_order_id, self.next_trade_id, self.next_priority)
    }

    /// Applies one input and appends its output events to `out`.
    ///
    /// The caller reuses `out` across calls; it is cleared here.
    pub fn apply(&mut self, input: &EngineInput, out: &mut Vec<OutputEvent>) {
        out.clear();
        if self.fault.is_some() {
            return;
        }
        match self.engine_seq.next() {
            Some(next) => self.engine_seq = next,
            None => {
                self.fault = Some(EngineFault::SequenceExhausted);
                return;
            }
        }
        self.engine_time_ns = input.recv_time_ns;
        self.metrics.inputs_applied += 1;

        match input.event {
            InputEvent::Market(event) => self.apply_market(event, out),
            InputEvent::Order(request) => self.apply_request(&request, out),
            InputEvent::Control(command) => self.apply_control(command, out),
        }
        self.metrics.reports_emitted += out
            .iter()
            .filter(|event| matches!(event, OutputEvent::Report(_)))
            .count() as u64;
    }

    fn apply_market(&mut self, event: MarketEvent, out: &mut Vec<OutputEvent>) {
        self.metrics.market_events += 1;
        let Some(&index) = self.instrument_index.get(&event.instrument) else {
            return;
        };
        let update =
            self.instruments[index]
                .market
                .apply(event.source_seq, event.kind, self.engine_time_ns);
        if update.state_changed() {
            let state_event = EngineStateEvent::FeedStateChanged {
                instrument: event.instrument,
                state: update.state,
            };
            self.push_state(state_event, out);
        }
    }

    fn apply_control(&mut self, command: ControlCommand, out: &mut Vec<OutputEvent>) {
        self.metrics.control_events += 1;
        match command {
            ControlCommand::SetGlobalKill { engaged } => {
                if self.global_kill == engaged {
                    return;
                }
                self.global_kill = engaged;
                let event = if engaged {
                    EngineStateEvent::KillSwitchEngaged
                } else {
                    EngineStateEvent::KillSwitchReleased
                };
                self.push_state(event, out);
            }
            ControlCommand::SetAccountEnabled { account, enabled } => {
                let Some(&index) = self.account_index.get(&account) else {
                    return;
                };
                if self.accounts[index].enabled == enabled {
                    return;
                }
                self.accounts[index].enabled = enabled;
                self.push_state(
                    EngineStateEvent::AccountEnabledChanged { account, enabled },
                    out,
                );
            }
            ControlCommand::SetAccountLimits { account, limits } => {
                let Some(&index) = self.account_index.get(&account) else {
                    return;
                };
                self.accounts[index].limits = limits;
                self.push_state(EngineStateEvent::AccountLimitsChanged { account }, out);
            }
        }
    }

    fn apply_request(&mut self, request: &OrderRequest, out: &mut Vec<OutputEvent>) {
        let fingerprint = request_fingerprint(request);
        if let Some(&account_index) = self.account_index.get(&request.account) {
            match self.accounts[account_index]
                .cache
                .lookup(request.request_id, fingerprint)
            {
                // An exact retry returns the recorded result and changes nothing.
                Some(CacheHit::Retry(previous)) => {
                    let mut report = previous;
                    report.output_seq = self.take_output_seq();
                    report.engine_seq = self.engine_seq;
                    report.engine_time_ns = self.engine_time_ns;
                    out.push(OutputEvent::Report(report));
                    return;
                }
                Some(CacheHit::Conflict) => {
                    let report =
                        self.reject_report(request, RejectReason::DuplicateRequestConflict);
                    self.record_outcome(account_index, request, fingerprint, report);
                    out.push(OutputEvent::Report(report));
                    return;
                }
                None => {}
            }
        }

        let outcome = match request.kind {
            RequestKind::New => self.handle_new(request, out),
            RequestKind::Cancel => self.handle_cancel(request, out),
            RequestKind::Replace => self.handle_replace(request, out),
        };

        if outcome.reject_reason.is_some() {
            self.metrics.requests_rejected += 1;
        } else {
            self.metrics.requests_accepted += 1;
        }
        if let Some(&account_index) = self.account_index.get(&request.account) {
            self.record_outcome(account_index, request, fingerprint, outcome);
        }
        out.push(OutputEvent::Report(outcome));
    }

    fn handle_new(
        &mut self,
        request: &OrderRequest,
        out: &mut Vec<OutputEvent>,
    ) -> ExecutionReport {
        let checked = match self.check_new_order(request) {
            Ok(checked) => checked,
            Err(reason) => return self.reject_report(request, reason),
        };

        let Some(order_id) = self.next_order_id.next() else {
            self.fault = Some(EngineFault::SequenceExhausted);
            return self.reject_report(request, RejectReason::CapacityExhausted);
        };
        self.next_order_id = order_id;

        let filled = self.execute(
            checked.instrument_index,
            checked.account_index,
            request,
            order_id,
            checked.execution_limit,
            request.quantity,
            out,
        );

        let remaining = QuantityLots(request.quantity.0 - filled.0);
        if remaining.is_zero() {
            return self.terminal_report(request, order_id, ReportKind::Filled, filled);
        }

        if request.order_type == OrderType::Market {
            // Market orders never rest. The remainder is cancelled.
            return self.cancelled_report(request, order_id, request.quantity, filled);
        }

        self.rest_order(
            checked.instrument_index,
            checked.account_index,
            request,
            order_id,
            request.quantity,
            filled,
        );

        let mut report = self.base_report(request, order_id);
        report.kind = ReportKind::Accepted;
        report.state = if filled.is_zero() {
            OrderState::Working
        } else {
            OrderState::PartiallyFilled
        };
        report.cumulative_filled = filled;
        report.remaining = remaining;
        report
    }

    fn handle_cancel(
        &mut self,
        request: &OrderRequest,
        _out: &mut [OutputEvent],
    ) -> ExecutionReport {
        if !self.account_index.contains_key(&request.account) {
            return self.reject_report(request, RejectReason::UnknownAccount);
        }
        let key = (request.account, request.target_client_order_id);
        let Some(&order_id) = self.client_orders.get(&key) else {
            return self.reject_report(request, RejectReason::UnknownOrder);
        };
        let Some(live) = self.live_orders.get(&order_id).copied() else {
            return self.reject_report(request, RejectReason::OrderAlreadyTerminal);
        };

        let node = self.instruments[live.instrument_index]
            .book
            .remove(order_id)
            .expect("live order is present in its book");
        self.release_reservation(
            live.account_index,
            live.instrument_index,
            node.side,
            node.remaining(),
            node.price,
        );
        self.live_orders.remove(&order_id);
        self.client_orders.remove(&key);

        let mut report = self.base_report(request, order_id);
        report.kind = ReportKind::Cancelled;
        report.state = OrderState::Cancelled;
        report.client_order_id = node.client_order_id;
        report.side = node.side;
        report.price = node.price;
        report.total_quantity = node.total_quantity;
        report.cumulative_filled = node.cumulative_filled;
        report.remaining = QuantityLots::ZERO;
        report
    }

    fn handle_replace(
        &mut self,
        request: &OrderRequest,
        out: &mut Vec<OutputEvent>,
    ) -> ExecutionReport {
        let checked = match self.check_replace(request) {
            Ok(checked) => checked,
            Err(reason) => return self.reject_report(request, reason),
        };

        let account_index = checked.account_index;
        let instrument_index = checked.instrument_index;
        let order_id = checked.order_id;
        let existing = checked.existing;
        let key = (request.account, existing.client_order_id);

        // Same price with a lower total keeps queue priority.
        if request.price == existing.price && request.quantity < existing.total_quantity {
            let released = QuantityLots(existing.total_quantity.0 - request.quantity.0);
            self.release_reservation(
                account_index,
                instrument_index,
                existing.side,
                released,
                existing.price,
            );
            self.instruments[instrument_index]
                .book
                .reduce_total(order_id, request.quantity);

            if request.quantity == existing.cumulative_filled {
                self.instruments[instrument_index].book.remove(order_id);
                self.live_orders.remove(&order_id);
                self.client_orders.remove(&key);
                return self.replace_cancelled_report(request, order_id, &existing);
            }

            let mut report = self.base_report(request, order_id);
            report.kind = ReportKind::Replaced;
            report.client_order_id = existing.client_order_id;
            report.side = existing.side;
            report.state = if existing.cumulative_filled.is_zero() {
                OrderState::Working
            } else {
                OrderState::PartiallyFilled
            };
            report.cumulative_filled = existing.cumulative_filled;
            report.remaining = QuantityLots(request.quantity.0 - existing.cumulative_filled.0);
            return report;
        }

        // A price change or quantity increase loses priority.
        self.instruments[instrument_index].book.remove(order_id);
        self.release_reservation(
            account_index,
            instrument_index,
            existing.side,
            existing.remaining(),
            existing.price,
        );
        self.live_orders.remove(&order_id);
        self.client_orders.remove(&key);

        if request.quantity == existing.cumulative_filled {
            return self.replace_cancelled_report(request, order_id, &existing);
        }

        let target_remaining = QuantityLots(request.quantity.0 - existing.cumulative_filled.0);
        let filled = self.execute(
            instrument_index,
            account_index,
            request,
            order_id,
            request.price,
            target_remaining,
            out,
        );

        let cumulative = QuantityLots(existing.cumulative_filled.0 + filled.0);
        if cumulative == request.quantity {
            let mut report = self.base_report(request, order_id);
            report.kind = ReportKind::Filled;
            report.state = OrderState::Filled;
            report.client_order_id = existing.client_order_id;
            report.side = existing.side;
            report.cumulative_filled = cumulative;
            report.remaining = QuantityLots::ZERO;
            return report;
        }

        self.rest_order_with(
            instrument_index,
            account_index,
            existing.client_order_id,
            existing.side,
            request.price,
            request.quantity,
            cumulative,
            order_id,
        );

        let mut report = self.base_report(request, order_id);
        report.kind = ReportKind::Replaced;
        report.client_order_id = existing.client_order_id;
        report.side = existing.side;
        report.state = if cumulative.is_zero() {
            OrderState::Working
        } else {
            OrderState::PartiallyFilled
        };
        report.cumulative_filled = cumulative;
        report.remaining = QuantityLots(request.quantity.0 - cumulative.0);
        report
    }

    /// Matches one aggressing order and emits trade, maker report, taker report
    /// for every fill in that fixed order.
    #[allow(clippy::too_many_arguments)]
    fn execute(
        &mut self,
        instrument_index: usize,
        account_index: usize,
        request: &OrderRequest,
        order_id: OrderId,
        limit: PriceTicks,
        quantity: QuantityLots,
        out: &mut Vec<OutputEvent>,
    ) -> QuantityLots {
        self.fills.clear();
        let filled = matching::match_order(
            &mut self.instruments[instrument_index].book,
            request.side,
            limit,
            quantity,
            &mut self.fills,
        );
        if filled.is_zero() {
            return filled;
        }

        let instrument = self.instruments[instrument_index].config.id;
        let mut taker_cumulative = QuantityLots::ZERO;
        for index in 0..self.fills.len() {
            let fill = self.fills[index];
            taker_cumulative = QuantityLots(taker_cumulative.0 + fill.quantity.0);

            let Some(trade_id) = self.next_trade_id.next() else {
                self.fault = Some(EngineFault::SequenceExhausted);
                return filled;
            };
            self.next_trade_id = trade_id;
            self.metrics.trades += 1;

            let maker_account_index = self.account_index[&fill.maker_account];
            self.settle_fill(maker_account_index, instrument_index, &fill, request.side);
            self.settle_taker(account_index, instrument_index, request.side, fill.quantity);

            let output_seq = self.take_output_seq();
            out.push(OutputEvent::Trade(TradeEvent {
                output_seq,
                engine_seq: self.engine_seq,
                engine_time_ns: self.engine_time_ns,
                trade_id,
                instrument,
                maker_order_id: fill.maker_order_id,
                taker_order_id: order_id,
                maker_account: fill.maker_account,
                taker_account: request.account,
                aggressor: request.side,
                price: fill.price,
                quantity: fill.quantity,
            }));

            let maker_report = ExecutionReport {
                output_seq: self.take_output_seq(),
                engine_seq: self.engine_seq,
                engine_time_ns: self.engine_time_ns,
                account: fill.maker_account,
                instrument,
                request_id: protocol::RequestId(0),
                client_order_id: fill.maker_client_order_id,
                order_id: fill.maker_order_id,
                kind: if fill.maker_is_filled() {
                    ReportKind::Filled
                } else {
                    ReportKind::PartiallyFilled
                },
                state: if fill.maker_is_filled() {
                    OrderState::Filled
                } else {
                    OrderState::PartiallyFilled
                },
                side: request.side.opposite(),
                order_type: OrderType::Limit,
                price: fill.price,
                total_quantity: fill.maker_total_quantity,
                cumulative_filled: fill.maker_cumulative_filled,
                remaining: fill.maker_remaining,
                last_fill_quantity: fill.quantity,
                last_fill_price: fill.price,
                reject_reason: None,
            };
            out.push(OutputEvent::Report(maker_report));

            if fill.maker_is_filled() {
                self.live_orders.remove(&fill.maker_order_id);
                self.client_orders
                    .remove(&(fill.maker_account, fill.maker_client_order_id));
            }

            let mut taker_report = self.base_report(request, order_id);
            taker_report.kind = if taker_cumulative == quantity {
                ReportKind::Filled
            } else {
                ReportKind::PartiallyFilled
            };
            taker_report.state = if taker_cumulative == quantity {
                OrderState::Filled
            } else {
                OrderState::PartiallyFilled
            };
            taker_report.cumulative_filled = taker_cumulative;
            taker_report.remaining = QuantityLots(quantity.0 - taker_cumulative.0);
            taker_report.last_fill_quantity = fill.quantity;
            taker_report.last_fill_price = fill.price;
            out.push(OutputEvent::Report(taker_report));
        }

        filled
    }

    fn settle_fill(
        &mut self,
        maker_account_index: usize,
        instrument_index: usize,
        fill: &Fill,
        aggressor: Side,
    ) {
        let maker_side = aggressor.opposite();
        self.release_reservation(
            maker_account_index,
            instrument_index,
            maker_side,
            fill.quantity,
            fill.price,
        );
        let position = &mut self.accounts[maker_account_index].positions[instrument_index];
        apply_fill_to_position(position, maker_side, fill.quantity);
    }

    fn settle_taker(
        &mut self,
        account_index: usize,
        instrument_index: usize,
        side: Side,
        quantity: QuantityLots,
    ) {
        let position = &mut self.accounts[account_index].positions[instrument_index];
        apply_fill_to_position(position, side, quantity);
    }

    fn rest_order(
        &mut self,
        instrument_index: usize,
        account_index: usize,
        request: &OrderRequest,
        order_id: OrderId,
        total: QuantityLots,
        filled: QuantityLots,
    ) {
        self.rest_order_with(
            instrument_index,
            account_index,
            request.client_order_id,
            request.side,
            request.price,
            total,
            filled,
            order_id,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn rest_order_with(
        &mut self,
        instrument_index: usize,
        account_index: usize,
        client_order_id: ClientOrderId,
        side: Side,
        price: PriceTicks,
        total: QuantityLots,
        filled: QuantityLots,
        order_id: OrderId,
    ) {
        let Some(priority) = self.next_priority.next() else {
            self.fault = Some(EngineFault::SequenceExhausted);
            return;
        };
        self.next_priority = priority;
        let account = self.accounts[account_index].id;
        let inserted = self.instruments[instrument_index].book.insert(NewOrder {
            order_id,
            account,
            client_order_id,
            side,
            price,
            total_quantity: total,
            cumulative_filled: filled,
            priority,
        });
        if inserted.is_err() {
            return;
        }
        let remaining = QuantityLots(total.0 - filled.0);
        self.reserve(account_index, instrument_index, side, remaining, price);
        self.live_orders.insert(
            order_id,
            LiveOrder {
                account_index,
                instrument_index,
            },
        );
        self.client_orders
            .insert((account, client_order_id), order_id);
    }

    fn reserve(
        &mut self,
        account_index: usize,
        instrument_index: usize,
        side: Side,
        lots: QuantityLots,
        price: PriceTicks,
    ) {
        let notional = lots
            .checked_notional(price)
            .unwrap_or(protocol::Notional::ZERO);
        self.accounts[account_index].positions[instrument_index].reserve(side, lots, notional);
    }

    fn release_reservation(
        &mut self,
        account_index: usize,
        instrument_index: usize,
        side: Side,
        lots: QuantityLots,
        price: PriceTicks,
    ) {
        if lots.is_zero() {
            return;
        }
        let notional = lots
            .checked_notional(price)
            .unwrap_or(protocol::Notional::ZERO);
        self.accounts[account_index].positions[instrument_index].release(side, lots, notional);
    }

    fn record_outcome(
        &mut self,
        account_index: usize,
        request: &OrderRequest,
        fingerprint: u64,
        report: ExecutionReport,
    ) {
        self.accounts[account_index]
            .cache
            .record(request.request_id, fingerprint, report);
        if report.reject_reason.is_none() && request.client_seq > 0 {
            self.accounts[account_index].last_client_seq = request.client_seq;
        }
    }

    fn take_output_seq(&mut self) -> OutputSeq {
        match self.output_seq.next() {
            Some(next) => {
                self.output_seq = next;
                next
            }
            None => {
                self.fault = Some(EngineFault::SequenceExhausted);
                self.output_seq
            }
        }
    }

    fn push_state(&mut self, event: EngineStateEvent, out: &mut Vec<OutputEvent>) {
        let output_seq = self.take_output_seq();
        out.push(OutputEvent::State(StateEvent {
            output_seq,
            engine_seq: self.engine_seq,
            engine_time_ns: self.engine_time_ns,
            event,
        }));
    }

    fn base_report(&mut self, request: &OrderRequest, order_id: OrderId) -> ExecutionReport {
        ExecutionReport {
            output_seq: self.take_output_seq(),
            engine_seq: self.engine_seq,
            engine_time_ns: self.engine_time_ns,
            account: request.account,
            instrument: request.instrument,
            request_id: request.request_id,
            client_order_id: request.client_order_id,
            order_id,
            kind: ReportKind::Accepted,
            state: OrderState::Working,
            side: request.side,
            order_type: request.order_type,
            price: request.price,
            total_quantity: request.quantity,
            cumulative_filled: QuantityLots::ZERO,
            remaining: request.quantity,
            last_fill_quantity: QuantityLots::ZERO,
            last_fill_price: PriceTicks(0),
            reject_reason: None,
        }
    }

    fn reject_report(&mut self, request: &OrderRequest, reason: RejectReason) -> ExecutionReport {
        let mut report = self.base_report(request, OrderId(0));
        report.kind = ReportKind::Rejected;
        report.state = OrderState::Cancelled;
        report.remaining = QuantityLots::ZERO;
        report.reject_reason = Some(reason);
        report
    }

    fn terminal_report(
        &mut self,
        request: &OrderRequest,
        order_id: OrderId,
        kind: ReportKind,
        filled: QuantityLots,
    ) -> ExecutionReport {
        let mut report = self.base_report(request, order_id);
        report.kind = kind;
        report.state = OrderState::Filled;
        report.cumulative_filled = filled;
        report.remaining = QuantityLots::ZERO;
        report
    }

    fn cancelled_report(
        &mut self,
        request: &OrderRequest,
        order_id: OrderId,
        total: QuantityLots,
        filled: QuantityLots,
    ) -> ExecutionReport {
        let mut report = self.base_report(request, order_id);
        report.kind = ReportKind::Cancelled;
        report.state = OrderState::Cancelled;
        report.total_quantity = total;
        report.cumulative_filled = filled;
        report.remaining = QuantityLots::ZERO;
        report
    }

    fn replace_cancelled_report(
        &mut self,
        request: &OrderRequest,
        order_id: OrderId,
        existing: &crate::book::OrderNode,
    ) -> ExecutionReport {
        let mut report = self.base_report(request, order_id);
        report.kind = ReportKind::Cancelled;
        report.state = OrderState::Cancelled;
        report.client_order_id = existing.client_order_id;
        report.side = existing.side;
        report.cumulative_filled = existing.cumulative_filled;
        report.remaining = QuantityLots::ZERO;
        report
    }

    pub(crate) fn account_index_of(&self, account: AccountId) -> Option<usize> {
        self.account_index.get(&account).copied()
    }

    pub(crate) fn instrument_index_of(&self, instrument: InstrumentId) -> Option<usize> {
        self.instrument_index.get(&instrument).copied()
    }

    pub(crate) fn position(
        &self,
        account_index: usize,
        instrument_index: usize,
    ) -> &InstrumentPosition {
        &self.accounts[account_index].positions[instrument_index]
    }

    pub(crate) fn client_order_lookup(
        &self,
        account: AccountId,
        client_order_id: ClientOrderId,
    ) -> Option<OrderId> {
        self.client_orders.get(&(account, client_order_id)).copied()
    }
}

/// Fields resolved by the pre-trade checks so the mutating path does not repeat work.
pub(crate) struct CheckedNewOrder {
    pub(crate) account_index: usize,
    pub(crate) instrument_index: usize,
    pub(crate) execution_limit: PriceTicks,
}

pub(crate) struct CheckedReplace {
    pub(crate) account_index: usize,
    pub(crate) instrument_index: usize,
    pub(crate) order_id: OrderId,
    pub(crate) existing: crate::book::OrderNode,
}
