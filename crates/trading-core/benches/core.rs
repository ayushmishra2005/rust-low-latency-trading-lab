//! Criterion microbenchmarks for the codec, book, matching, and risk paths.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use protocol::codec::Frame;
use protocol::{
    AccountId, ClientOrderId, EngineInput, IngressSeq, InputEvent, InstrumentId, OrderId,
    OrderRequest, OrderType, OutputEvent, PriceTicks, PrioritySeq, QuantityLots, RequestId,
    RequestKind, Side,
};
use trading_core::book::{NewOrder, OrderBook};
use trading_core::generator::to_frame;
use trading_core::matching::match_order;
use trading_core::{EngineConfig, Generator, GeneratorConfig, TradingCore};

fn encoded_frames() -> Vec<Vec<u8>> {
    Generator::new(GeneratorConfig::new(1, 2_000))
        .generate()
        .iter()
        .filter_map(to_frame)
        .map(|frame| {
            let mut bytes = Vec::new();
            frame.encode(&mut bytes);
            bytes
        })
        .collect()
}

fn codec(c: &mut Criterion) {
    let frames = encoded_frames();
    let mut group = c.benchmark_group("codec");
    group.bench_function("decode_one_frame", |b| {
        let bytes = &frames[0];
        b.iter(|| black_box(Frame::decode(black_box(bytes)).unwrap()));
    });
    group.bench_function("decode_mixed_batch", |b| {
        b.iter(|| {
            for bytes in &frames {
                black_box(Frame::decode(bytes).unwrap());
            }
        });
    });
    group.finish();
}

fn filled_book(orders: u64) -> OrderBook {
    let mut book = OrderBook::with_capacity(orders as usize * 2 + 16);
    for index in 0..orders {
        let price = 10_000 - i64::try_from(index % 50).unwrap() - 1;
        book.insert(NewOrder {
            order_id: OrderId(index + 1),
            account: AccountId(1),
            client_order_id: ClientOrderId(index + 1),
            side: Side::Buy,
            price: PriceTicks(price),
            total_quantity: QuantityLots(10),
            cumulative_filled: QuantityLots::ZERO,
            priority: PrioritySeq(index + 1),
        })
        .unwrap();
    }
    book
}

fn book(c: &mut Criterion) {
    let mut group = c.benchmark_group("book");
    group.bench_function("insert_1000", |b| {
        b.iter(|| black_box(filled_book(1_000)));
    });
    group.bench_function("cancel_head", |b| {
        b.iter_batched(
            || filled_book(1_000),
            |mut book| {
                for index in 0..1_000u64 {
                    black_box(book.remove(OrderId(index + 1)));
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.bench_function("match_multi_level_sweep", |b| {
        let mut fills = Vec::with_capacity(256);
        b.iter_batched(
            || filled_book(1_000),
            |mut book| {
                fills.clear();
                black_box(match_order(
                    &mut book,
                    Side::Sell,
                    PriceTicks(1),
                    QuantityLots(5_000),
                    &mut fills,
                ));
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn warm_core() -> (TradingCore, Vec<OutputEvent>) {
    let mut core = TradingCore::new(EngineConfig::single_instrument(1));
    let mut out = Vec::with_capacity(64);
    let inputs = Generator::new(GeneratorConfig::new(5, 200)).generate();
    for input in &inputs {
        core.apply(input, &mut out);
    }
    (core, out)
}

fn order_input(ingress: u64, request_id: u64, client_seq: u64, price: i64) -> EngineInput {
    EngineInput {
        ingress_seq: IngressSeq(ingress),
        recv_time_ns: 1_000_000_000 + ingress,
        event: InputEvent::Order(OrderRequest {
            kind: RequestKind::New,
            account: AccountId(1),
            instrument: InstrumentId(1),
            request_id: RequestId(request_id),
            client_seq,
            client_order_id: ClientOrderId(request_id + 1_000_000),
            target_client_order_id: ClientOrderId(0),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: PriceTicks(price),
            quantity: QuantityLots(1),
        }),
    }
}

/// Identifiers start above anything the warm-up consumed.
const ID_BASE: u64 = 1_000_000;
/// Orders per measured batch. The book holds 4096, so a fresh core per batch
/// keeps every one of these acceptable.
const BATCH_ORDERS: u64 = 1_000;

fn batch_input(index: u64, price: i64) -> EngineInput {
    let id = ID_BASE + index;
    order_input(id, id, id, price)
}

/// Fails the run if the measured path is not actually accepting orders.
fn check_accepted_batch() {
    let mut accepted = 0u64;
    let mut rejected = 0u64;
    for _ in 0..5 {
        let (mut core, mut out) = warm_core();
        for index in 0..BATCH_ORDERS {
            core.apply(&batch_input(index, 9_000), &mut out);
            match out.iter().find_map(|event| match event {
                OutputEvent::Report(report) => Some(report.reject_reason),
                _ => None,
            }) {
                Some(None) => accepted += 1,
                Some(Some(reason)) => {
                    rejected += 1;
                    if rejected == 1 {
                        println!("first rejection at order {index}: {reason:?}");
                    }
                }
                None => panic!("no report for order {index}"),
            }
        }
    }
    println!("accepted_new_limit validation: accepted = {accepted} rejected = {rejected}");
    assert_eq!(rejected, 0, "the accepted benchmark must not reject");
}

fn engine(c: &mut Criterion) {
    check_accepted_batch();
    let mut group = c.benchmark_group("engine");
    group.throughput(criterion::Throughput::Elements(BATCH_ORDERS));

    group.bench_function("accepted_new_limit", |b| {
        b.iter_batched_ref(
            warm_core,
            |(core, out)| {
                let mut rejects = 0u32;
                for index in 0..BATCH_ORDERS {
                    core.apply(black_box(&batch_input(index, 9_000)), out);
                    if let Some(OutputEvent::Report(report)) = out.last() {
                        rejects += u32::from(report.reject_reason.is_some());
                    }
                }
                assert_eq!(rejects, 0, "the accepted benchmark must not reject");
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("rejected_risk_check", |b| {
        b.iter_batched_ref(
            warm_core,
            |(core, out)| {
                let mut accepts = 0u32;
                for index in 0..BATCH_ORDERS {
                    // Far outside the collar, so this exercises the reject path.
                    core.apply(black_box(&batch_input(index, 1)), out);
                    if let Some(OutputEvent::Report(report)) = out.last() {
                        accepts += u32::from(report.reject_reason.is_none());
                    }
                }
                assert_eq!(accepts, 0, "the rejected benchmark must not accept");
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.throughput(criterion::Throughput::Elements(10_000));
    group.bench_function("apply_generated_workload", |b| {
        let inputs = Generator::new(GeneratorConfig::new(77, 10_000)).generate();
        b.iter_batched(
            || TradingCore::new(EngineConfig::single_instrument(1)),
            |mut core| {
                let mut out = Vec::with_capacity(64);
                for input in &inputs {
                    core.apply(input, &mut out);
                }
                black_box(core.metrics())
            },
            criterion::BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion_group!(benches, codec, book, engine);
criterion_main!(benches);
