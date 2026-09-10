//! Deterministic matching against the book.
//!
//! Resting orders set the execution price. Better prices execute first and, at
//! one price, earlier priority executes first. Self-trading is allowed in this
//! closed simulator.

use protocol::{AccountId, ClientOrderId, OrderId, PriceTicks, QuantityLots, Side};

use crate::book::OrderBook;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fill {
    pub maker_order_id: OrderId,
    pub maker_account: AccountId,
    pub maker_client_order_id: ClientOrderId,
    pub price: PriceTicks,
    pub quantity: QuantityLots,
    pub maker_total_quantity: QuantityLots,
    pub maker_cumulative_filled: QuantityLots,
    pub maker_remaining: QuantityLots,
}

impl Fill {
    pub fn maker_is_filled(&self) -> bool {
        self.maker_remaining.is_zero()
    }
}

/// Matches `quantity` from `taker_side` against resting liquidity.
///
/// `limit` bounds the worst acceptable price; market orders pass their
/// protection price. Returns the filled quantity and appends fills in
/// execution order.
pub fn match_order(
    book: &mut OrderBook,
    taker_side: Side,
    limit: PriceTicks,
    quantity: QuantityLots,
    fills: &mut Vec<Fill>,
) -> QuantityLots {
    let maker_side = taker_side.opposite();
    let mut remaining = quantity.0;

    while remaining > 0 {
        let Some(price) = book.next_eligible_price(maker_side, Some(limit)) else {
            break;
        };
        while remaining > 0 {
            let Some(slot) = book.head_slot(maker_side, price) else {
                break;
            };
            let maker = *book.node(slot);
            let traded = remaining.min(maker.remaining().0);
            let maker_after = book.fill_maker(slot, QuantityLots(traded));
            remaining -= traded;
            fills.push(Fill {
                maker_order_id: maker.order_id,
                maker_account: maker.account,
                maker_client_order_id: maker.client_order_id,
                price,
                quantity: QuantityLots(traded),
                maker_total_quantity: maker_after.total_quantity,
                maker_cumulative_filled: maker_after.cumulative_filled,
                maker_remaining: maker_after.remaining(),
            });
        }
    }

    QuantityLots(quantity.0 - remaining)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::NewOrder;
    use protocol::PrioritySeq;

    fn rest(book: &mut OrderBook, id: u64, side: Side, price: i64, quantity: u64, priority: u64) {
        book.insert(NewOrder {
            order_id: OrderId(id),
            account: AccountId(1),
            client_order_id: ClientOrderId(id),
            side,
            price: PriceTicks(price),
            total_quantity: QuantityLots(quantity),
            cumulative_filled: QuantityLots(0),
            priority: PrioritySeq(priority),
        })
        .unwrap();
    }

    #[test]
    fn better_prices_execute_first() {
        let mut book = OrderBook::with_capacity(16);
        rest(&mut book, 1, Side::Sell, 101, 5, 1);
        rest(&mut book, 2, Side::Sell, 100, 5, 2);

        let mut fills = Vec::new();
        let filled = match_order(
            &mut book,
            Side::Buy,
            PriceTicks(101),
            QuantityLots(8),
            &mut fills,
        );

        assert_eq!(filled, QuantityLots(8));
        assert_eq!(fills[0].price, PriceTicks(100));
        assert_eq!(fills[0].quantity, QuantityLots(5));
        assert_eq!(fills[1].price, PriceTicks(101));
        assert_eq!(fills[1].quantity, QuantityLots(3));
        assert!(book.check_invariants().is_ok());
    }

    #[test]
    fn fifo_priority_holds_at_one_price() {
        let mut book = OrderBook::with_capacity(16);
        rest(&mut book, 1, Side::Sell, 100, 2, 1);
        rest(&mut book, 2, Side::Sell, 100, 2, 2);

        let mut fills = Vec::new();
        match_order(
            &mut book,
            Side::Buy,
            PriceTicks(100),
            QuantityLots(3),
            &mut fills,
        );

        assert_eq!(fills[0].maker_order_id, OrderId(1));
        assert_eq!(fills[1].maker_order_id, OrderId(2));
        assert_eq!(fills[1].maker_remaining, QuantityLots(1));
    }

    #[test]
    fn limit_stops_at_the_protection_price() {
        let mut book = OrderBook::with_capacity(16);
        rest(&mut book, 1, Side::Sell, 100, 2, 1);
        rest(&mut book, 2, Side::Sell, 105, 2, 2);

        let mut fills = Vec::new();
        let filled = match_order(
            &mut book,
            Side::Buy,
            PriceTicks(102),
            QuantityLots(4),
            &mut fills,
        );

        assert_eq!(filled, QuantityLots(2));
        assert_eq!(fills.len(), 1);
        assert_eq!(book.best_ask().unwrap().0, PriceTicks(105));
    }
}
