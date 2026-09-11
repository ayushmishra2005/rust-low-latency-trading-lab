//! Pre-trade risk. Checks run in a fixed order so one invalid request always
//! produces the same primary reject reason. Nothing here mutates state.

use protocol::{
    FeedState, Notional, OrderRequest, OrderType, PriceTicks, QuantityLots, RejectReason,
    RequestKind, Side,
};

use crate::account::{instrument_exposure, InstrumentPosition};
use crate::core::{CheckedNewOrder, CheckedReplace, TradingCore};

impl TradingCore {
    pub(crate) fn check_new_order(
        &self,
        request: &OrderRequest,
    ) -> Result<CheckedNewOrder, RejectReason> {
        if request.kind != RequestKind::New {
            return Err(RejectReason::MalformedRequest);
        }
        let account_index = self
            .account_index_of(request.account)
            .ok_or(RejectReason::UnknownAccount)?;
        let instrument_index = self
            .instrument_index_of(request.instrument)
            .ok_or(RejectReason::UnknownInstrument)?;

        self.check_quantity(instrument_index, request.quantity)?;
        if request.order_type == OrderType::Limit {
            self.check_price(instrument_index, request.price)?;
        }
        if self
            .client_order_lookup(request.account, request.client_order_id)
            .is_some()
        {
            return Err(RejectReason::DuplicateClientOrderId);
        }

        self.check_trading_permitted(account_index)?;
        self.check_client_sequence(account_index, request.client_seq)?;

        let reference = self.reference_price(instrument_index)?;
        let account = &self.accounts()[account_index];

        if request.quantity > account.limits.max_order_quantity {
            return Err(RejectReason::MaxOrderQuantity);
        }

        let execution_limit = match request.order_type {
            OrderType::Limit => request.price,
            OrderType::Market => protection_price(
                reference,
                request.side,
                self.config().market_protection_ticks,
            )?,
        };

        if tick_distance(execution_limit, reference) > collar_ticks(account.limits) {
            return Err(RejectReason::PriceCollar);
        }

        let notional = request
            .quantity
            .checked_notional(execution_limit)
            .ok_or(RejectReason::ArithmeticOverflow)?;
        if notional > account.limits.max_order_notional {
            return Err(RejectReason::MaxOrderNotional);
        }

        self.check_exposure(
            account_index,
            instrument_index,
            request.side,
            request.quantity,
            execution_limit,
            QuantityLots::ZERO,
            execution_limit,
        )?;

        if self.instruments()[instrument_index].book.is_full() {
            return Err(RejectReason::CapacityExhausted);
        }

        Ok(CheckedNewOrder {
            account_index,
            instrument_index,
            execution_limit,
        })
    }

    pub(crate) fn check_replace(
        &self,
        request: &OrderRequest,
    ) -> Result<CheckedReplace, RejectReason> {
        let account_index = self
            .account_index_of(request.account)
            .ok_or(RejectReason::UnknownAccount)?;
        let instrument_index = self
            .instrument_index_of(request.instrument)
            .ok_or(RejectReason::UnknownInstrument)?;

        let order_id = self
            .client_order_lookup(request.account, request.target_client_order_id)
            .ok_or(RejectReason::UnknownOrder)?;
        let existing = *self.instruments()[instrument_index]
            .book
            .get(order_id)
            .ok_or(RejectReason::OrderAlreadyTerminal)?;

        // A replace may change price and quantity, never side or order type.
        if request.side != existing.side || request.order_type != OrderType::Limit {
            return Err(RejectReason::MalformedRequest);
        }

        self.check_price(instrument_index, request.price)?;
        if request.quantity.is_zero() {
            return Err(RejectReason::InvalidQuantity);
        }
        let lot_size = self.instruments()[instrument_index].config.lot_size;
        if request.quantity.0 % lot_size != 0 {
            return Err(RejectReason::QuantityNotAligned);
        }
        // A replacement total below the cumulative fill is not representable.
        if request.quantity < existing.cumulative_filled {
            return Err(RejectReason::InvalidReplaceQuantity);
        }

        self.check_trading_permitted(account_index)?;
        self.check_client_sequence(account_index, request.client_seq)?;

        let reference = self.reference_price(instrument_index)?;
        let account = &self.accounts()[account_index];
        let new_remaining = QuantityLots(request.quantity.0 - existing.cumulative_filled.0);

        if new_remaining > account.limits.max_order_quantity {
            return Err(RejectReason::MaxOrderQuantity);
        }
        if tick_distance(request.price, reference) > collar_ticks(account.limits) {
            return Err(RejectReason::PriceCollar);
        }
        let notional = new_remaining
            .checked_notional(request.price)
            .ok_or(RejectReason::ArithmeticOverflow)?;
        if notional > account.limits.max_order_notional {
            return Err(RejectReason::MaxOrderNotional);
        }

        self.check_exposure(
            account_index,
            instrument_index,
            existing.side,
            new_remaining,
            request.price,
            existing.remaining(),
            existing.price,
        )?;

        Ok(CheckedReplace {
            account_index,
            instrument_index,
            order_id,
            existing,
        })
    }

    fn check_quantity(
        &self,
        instrument_index: usize,
        quantity: QuantityLots,
    ) -> Result<(), RejectReason> {
        if quantity.is_zero() {
            return Err(RejectReason::InvalidQuantity);
        }
        let lot_size = self.instruments()[instrument_index].config.lot_size;
        if quantity.0 % lot_size != 0 {
            return Err(RejectReason::QuantityNotAligned);
        }
        Ok(())
    }

    fn check_price(&self, instrument_index: usize, price: PriceTicks) -> Result<(), RejectReason> {
        let instrument = &self.instruments()[instrument_index].config;
        if price.0 < instrument.min_price_ticks || price.0 > instrument.max_price_ticks {
            return Err(RejectReason::InvalidPrice);
        }
        if price.0 % instrument.tick_size != 0 {
            return Err(RejectReason::PriceNotAligned);
        }
        Ok(())
    }

    fn check_trading_permitted(&self, account_index: usize) -> Result<(), RejectReason> {
        if self.global_kill() {
            return Err(RejectReason::GlobalKillActive);
        }
        if !self.accounts()[account_index].enabled {
            return Err(RejectReason::AccountDisabled);
        }
        Ok(())
    }

    fn check_client_sequence(
        &self,
        account_index: usize,
        client_seq: u64,
    ) -> Result<(), RejectReason> {
        if client_seq == 0 {
            return Err(RejectReason::MalformedRequest);
        }
        if client_seq <= self.accounts()[account_index].last_client_seq {
            return Err(RejectReason::SequenceTooOld);
        }
        Ok(())
    }

    /// Requires a synchronized and sufficiently fresh market view.
    fn reference_price(&self, instrument_index: usize) -> Result<PriceTicks, RejectReason> {
        let market = &self.instruments()[instrument_index].market;
        if market.state() != FeedState::Live {
            return Err(RejectReason::MarketDataUnsynchronized);
        }
        let age = self
            .engine_time_ns()
            .saturating_sub(market.last_update_time_ns());
        if age > self.config().max_market_age_ns {
            return Err(RejectReason::MarketDataStale);
        }
        market
            .reference_price()
            .ok_or(RejectReason::NoReferencePrice)
    }

    /// Worst-case position and gross exposure including working orders.
    #[allow(clippy::too_many_arguments)]
    fn check_exposure(
        &self,
        account_index: usize,
        instrument_index: usize,
        side: Side,
        added_lots: QuantityLots,
        added_price: PriceTicks,
        replaced_lots: QuantityLots,
        replaced_price: PriceTicks,
    ) -> Result<(), RejectReason> {
        let account = &self.accounts()[account_index];
        let current = self.position(account_index, instrument_index);

        let mut projected = *current;
        let released = replaced_lots
            .checked_notional(replaced_price)
            .ok_or(RejectReason::ArithmeticOverflow)?;
        let added = added_lots
            .checked_notional(added_price)
            .ok_or(RejectReason::ArithmeticOverflow)?;
        match side {
            Side::Buy => {
                projected.open_buy_lots = projected
                    .open_buy_lots
                    .checked_sub(replaced_lots.0)
                    .and_then(|lots| lots.checked_add(added_lots.0))
                    .ok_or(RejectReason::ArithmeticOverflow)?;
                projected.open_buy_notional = projected
                    .open_buy_notional
                    .checked_sub(released.0)
                    .and_then(|value| value.checked_add(added.0))
                    .ok_or(RejectReason::ArithmeticOverflow)?;
            }
            Side::Sell => {
                projected.open_sell_lots = projected
                    .open_sell_lots
                    .checked_sub(replaced_lots.0)
                    .and_then(|lots| lots.checked_add(added_lots.0))
                    .ok_or(RejectReason::ArithmeticOverflow)?;
                projected.open_sell_notional = projected
                    .open_sell_notional
                    .checked_sub(released.0)
                    .and_then(|value| value.checked_add(added.0))
                    .ok_or(RejectReason::ArithmeticOverflow)?;
            }
        }

        let worst =
            worst_case_position(&projected, side).ok_or(RejectReason::ArithmeticOverflow)?;
        if worst > account.limits.max_position_lots {
            return Err(RejectReason::MaxPosition);
        }

        let mut gross = Notional::ZERO;
        for (index, instrument) in self.instruments().iter().enumerate() {
            let position = if index == instrument_index {
                projected
            } else {
                *self.position(account_index, index)
            };
            if position.position_lots == 0
                && position.open_buy_notional == 0
                && position.open_sell_notional == 0
            {
                continue;
            }
            let mark = match instrument.market.reference_price() {
                Some(mark) => mark,
                None if position.position_lots == 0 => PriceTicks(0),
                None => return Err(RejectReason::NoReferencePrice),
            };
            let exposure =
                instrument_exposure(&position, mark).ok_or(RejectReason::ArithmeticOverflow)?;
            gross = gross
                .checked_add(exposure)
                .ok_or(RejectReason::ArithmeticOverflow)?;
        }
        if gross > account.limits.max_gross_exposure {
            return Err(RejectReason::MaxGrossExposure);
        }

        Ok(())
    }
}

/// Tick distance that is correct for every pair of prices.
fn tick_distance(left: PriceTicks, right: PriceTicks) -> u64 {
    left.0.abs_diff(right.0)
}

/// A negative collar admits no distance at all.
fn collar_ticks(limits: protocol::AccountLimits) -> u64 {
    u64::try_from(limits.price_collar_ticks).unwrap_or(0)
}

fn worst_case_position(position: &InstrumentPosition, side: Side) -> Option<u64> {
    let signed = match side {
        Side::Buy => position
            .position_lots
            .checked_add(i64::try_from(position.open_buy_lots).ok()?)?,
        Side::Sell => position
            .position_lots
            .checked_sub(i64::try_from(position.open_sell_lots).ok()?)?,
    };
    Some(signed.unsigned_abs())
}

/// Market orders execute up to a bounded distance from the reference price.
fn protection_price(
    reference: PriceTicks,
    side: Side,
    ticks: i64,
) -> Result<PriceTicks, RejectReason> {
    let price = match side {
        Side::Buy => reference.checked_add_ticks(ticks),
        Side::Sell => reference.checked_sub_ticks(ticks),
    };
    price.ok_or(RejectReason::ArithmeticOverflow)
}
