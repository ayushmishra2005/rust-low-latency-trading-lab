//! The indexed book must behave exactly like the simple reference model after
//! every single command, not only at the end.

use proptest::prelude::*;
use protocol::{AccountId, ClientOrderId, OrderId, PriceTicks, PrioritySeq, QuantityLots, Side};
use trading_core::book::{NewOrder, OrderBook};
use trading_core::matching::match_order;
use trading_core::reference::{book_rows, ReferenceBook, ReferenceOrder};

#[derive(Debug, Clone, Copy)]
enum Command {
    Insert {
        side: bool,
        price: i64,
        quantity: u64,
    },
    Cancel {
        which: usize,
    },
    Reduce {
        which: usize,
        quantity: u64,
    },
    Match {
        side: bool,
        limit: i64,
        quantity: u64,
    },
}

fn command() -> impl Strategy<Value = Command> {
    prop_oneof![
        (any::<bool>(), 95i64..106, 1u64..20).prop_map(|(side, price, quantity)| Command::Insert {
            side,
            price,
            quantity
        }),
        (0usize..16).prop_map(|which| Command::Cancel { which }),
        (0usize..16, 1u64..20).prop_map(|(which, quantity)| Command::Reduce { which, quantity }),
        (any::<bool>(), 95i64..106, 1u64..40).prop_map(|(side, limit, quantity)| Command::Match {
            side,
            limit,
            quantity
        }),
    ]
}

fn side_of(flag: bool) -> Side {
    if flag {
        Side::Buy
    } else {
        Side::Sell
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn indexed_book_matches_the_reference_model(commands in prop::collection::vec(command(), 1..60)) {
        let mut book = OrderBook::with_capacity(256);
        let mut reference = ReferenceBook::new();
        let mut live: Vec<OrderId> = Vec::new();
        let mut next_id = 0u64;
        let mut next_priority = 0u64;
        let mut fills = Vec::new();
        let mut reference_fills = Vec::new();

        for command in commands {
            match command {
                Command::Insert { side, price, quantity } => {
                    let side = side_of(side);
                    let price = PriceTicks(price);
                    // Only rest orders that do not cross, so the models stay comparable.
                    let crosses = match side {
                        Side::Buy => book.best_ask().is_some_and(|(ask, _)| price >= ask),
                        Side::Sell => book.best_bid().is_some_and(|(bid, _)| price <= bid),
                    };
                    if crosses {
                        continue;
                    }
                    next_id += 1;
                    next_priority += 1;
                    let order_id = OrderId(next_id);
                    book.insert(NewOrder {
                        order_id,
                        account: AccountId(1),
                        client_order_id: ClientOrderId(next_id),
                        side,
                        price,
                        total_quantity: QuantityLots(quantity),
                        cumulative_filled: QuantityLots::ZERO,
                        priority: PrioritySeq(next_priority),
                    }).unwrap();
                    reference.insert(side, price, ReferenceOrder {
                        order_id,
                        priority: PrioritySeq(next_priority),
                        total_quantity: QuantityLots(quantity),
                        cumulative_filled: QuantityLots::ZERO,
                    });
                    live.push(order_id);
                }
                Command::Cancel { which } => {
                    if live.is_empty() {
                        continue;
                    }
                    let order_id = live.remove(which % live.len());
                    let removed = book.remove(order_id);
                    let reference_removed = reference.remove(order_id);
                    prop_assert_eq!(removed.is_some(), reference_removed.is_some());
                }
                Command::Reduce { which, quantity } => {
                    if live.is_empty() {
                        continue;
                    }
                    let order_id = live[which % live.len()];
                    let Some(node) = book.get(order_id) else { continue };
                    let new_total = QuantityLots(quantity);
                    if new_total <= node.cumulative_filled || new_total >= node.total_quantity {
                        continue;
                    }
                    book.reduce_total(order_id, new_total);
                    reference.reduce_total(order_id, new_total);
                }
                Command::Match { side, limit, quantity } => {
                    let side = side_of(side);
                    fills.clear();
                    reference_fills.clear();
                    let filled = match_order(&mut book, side, PriceTicks(limit), QuantityLots(quantity), &mut fills);
                    let reference_filled = reference.match_order(side, PriceTicks(limit), QuantityLots(quantity), &mut reference_fills);
                    prop_assert_eq!(filled, reference_filled);
                    prop_assert_eq!(fills.len(), reference_fills.len());
                    for (fill, expected) in fills.iter().zip(&reference_fills) {
                        prop_assert_eq!(fill.maker_order_id, expected.maker_order_id);
                        prop_assert_eq!(fill.price, expected.price);
                        prop_assert_eq!(fill.quantity, expected.quantity);
                        prop_assert_eq!(fill.maker_remaining, expected.maker_remaining);
                    }
                    live.retain(|order_id| book.get(*order_id).is_some());
                }
            }

            prop_assert_eq!(book_rows(&book), reference.snapshot());
            prop_assert!(book.check_invariants().is_ok(), "{}", book.check_invariants().unwrap_err());
        }
    }
}
