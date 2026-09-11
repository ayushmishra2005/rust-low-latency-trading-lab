//! Threaded pipeline behaviour: determinism across real threads, backpressure,
//! journal durability, and clean shutdown.

use engine_runtime::{
    read_journal, ControlHandle, FeedSource, JournalError, JournalTail, PipelineConfig,
    WaitStrategy,
};
use protocol::codec::{FileHeader, InstrumentSpec};
use protocol::{
    AccountId, ClientOrderId, ControlCommand, EngineInput, IngressSeq, InputEvent, InstrumentId,
    MarketEvent, MarketEventKind, OrderRequest, OrderType, OutputEvent, PriceTicks, QuantityLots,
    RequestId, RequestKind, Side,
};
use trading_core::generator::to_frame;
use trading_core::{EngineConfig, Generator, GeneratorConfig, Replay, TradingCore};

fn workload(seed: u64, events: usize) -> Vec<protocol::EngineInput> {
    Generator::new(GeneratorConfig::new(seed, events)).generate()
}

fn temp_path(name: &str) -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "rltl-pipeline-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

fn attach_tail(path: &std::path::Path) -> JournalTail {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match JournalTail::open(path) {
            Ok(tail) => return tail,
            Err(JournalError::IncompleteHeader) => {}
            Err(JournalError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("unexpected attach error: {error}"),
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "timed out waiting for a complete journal header at {}",
                path.display()
            );
        }
        std::thread::yield_now();
    }
}

#[test]
fn threaded_run_matches_single_threaded_replay() {
    let inputs = workload(4242, 20_000);

    let mut replay = Replay::new(TradingCore::new(EngineConfig::single_instrument(11)), 0);
    replay.run(&inputs);
    let expected = replay.finish();

    let summary = engine_runtime::run(
        EngineConfig::single_instrument(11),
        PipelineConfig {
            input_capacity: 1_024,
            output_capacity: 1_024,
            ..PipelineConfig::default()
        },
        FeedSource::Memory(inputs.clone()),
        ControlHandle::default(),
    )
    .unwrap();

    assert_eq!(summary.inputs, inputs.len() as u64);
    assert_eq!(summary.input_digest, expected.input_digest);
    assert_eq!(summary.output_digest, expected.output_digest);
    assert_eq!(summary.state_digest, expected.state_digest);
    assert_eq!(summary.outputs, expected.outputs);
}

#[test]
fn small_queues_apply_backpressure_without_losing_output() {
    let inputs = workload(9, 10_000);

    let mut replay = Replay::new(TradingCore::new(EngineConfig::single_instrument(3)), 0);
    replay.run(&inputs);
    let expected = replay.finish();

    let summary = engine_runtime::run(
        EngineConfig::single_instrument(3),
        PipelineConfig {
            input_capacity: 8,
            output_capacity: 8,
            wait: WaitStrategy::Adaptive {
                spins: 8,
                yields: 2,
            },
            ..PipelineConfig::default()
        },
        FeedSource::Memory(inputs),
        ControlHandle::default(),
    )
    .unwrap();

    assert_eq!(summary.output_digest, expected.output_digest);
    assert!(
        summary.input_queue.full_events > 0 || summary.output_queue.full_events > 0,
        "expected the run to hit a full queue"
    );
    assert!(summary.input_queue.high_water <= 8);
    assert!(summary.output_queue.high_water <= 8);
}

#[test]
fn journal_contains_every_output_event_in_order() {
    let inputs = workload(17, 5_000);
    let path = temp_path("run.journal");

    let summary = engine_runtime::run(
        EngineConfig::single_instrument(21),
        PipelineConfig {
            journal_path: Some(path.clone()),
            ..PipelineConfig::default()
        },
        FeedSource::Memory(inputs),
        ControlHandle::default(),
    )
    .unwrap();

    let recovered = read_journal(&path).unwrap();
    assert!(recovered.truncated_at.is_none());
    assert_eq!(recovered.run_id, 21);
    assert_eq!(recovered.events.len() as u64, summary.outputs);
    assert_eq!(summary.journal_records, summary.outputs);

    let mut previous = 0;
    for event in &recovered.events {
        let seq = event.output_seq().0;
        assert!(seq > previous);
        previous = seq;
    }
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn feed_bytes_are_decoded_and_produce_the_same_state() {
    let inputs = workload(88, 4_000);

    let mut bytes = Vec::new();
    FileHeader {
        run_id: 5,
        seed: 88,
        start_wall_time_ns: 0,
        instruments: vec![InstrumentSpec::new(1, "LAB-USD", 1, 1)],
    }
    .encode(&mut bytes);
    for input in &inputs {
        to_frame(input).unwrap().encode(&mut bytes);
    }

    let from_memory = engine_runtime::run(
        EngineConfig::single_instrument(5),
        PipelineConfig::default(),
        FeedSource::Memory(inputs.clone()),
        ControlHandle::default(),
    )
    .unwrap();

    let from_bytes = engine_runtime::run(
        EngineConfig::single_instrument(5),
        PipelineConfig::default(),
        FeedSource::Bytes(bytes),
        ControlHandle::default(),
    )
    .unwrap();

    assert_eq!(from_bytes.decode_errors, 0);
    assert_eq!(from_bytes.inputs, from_memory.inputs);
    assert_eq!(from_bytes.state_digest, from_memory.state_digest);
    assert_eq!(from_bytes.output_digest, from_memory.output_digest);
}

#[test]
fn a_malformed_feed_stops_at_the_bad_frame() {
    let inputs = workload(3, 500);
    let mut bytes = Vec::new();
    FileHeader {
        run_id: 5,
        seed: 3,
        start_wall_time_ns: 0,
        instruments: vec![InstrumentSpec::new(1, "LAB-USD", 1, 1)],
    }
    .encode(&mut bytes);
    for input in &inputs {
        to_frame(input).unwrap().encode(&mut bytes);
    }
    let corrupt_at = bytes.len() / 2;
    bytes[corrupt_at] ^= 0xff;

    let summary = engine_runtime::run(
        EngineConfig::single_instrument(5),
        PipelineConfig::default(),
        FeedSource::Bytes(bytes),
        ControlHandle::default(),
    )
    .unwrap();

    assert_eq!(summary.decode_errors, 1);
    assert!(summary.inputs < inputs.len() as u64);
}

#[test]
fn kill_latch_reaches_the_engine_and_is_recorded() {
    let control = ControlHandle::default();
    control.engage_kill();

    let summary = engine_runtime::run(
        EngineConfig::single_instrument(1),
        PipelineConfig::default(),
        FeedSource::Memory(workload(6, 2_000)),
        control,
    )
    .unwrap();

    assert!(summary.outputs > 0);

    // The recorded run must contain the kill event exactly once.
    let path = temp_path("kill.journal");
    let control = ControlHandle::default();
    control.engage_kill();
    engine_runtime::run(
        EngineConfig::single_instrument(1),
        PipelineConfig {
            journal_path: Some(path.clone()),
            ..PipelineConfig::default()
        },
        FeedSource::Memory(workload(6, 500)),
        control,
    )
    .unwrap();

    let recovered = read_journal(&path).unwrap();
    let kills = recovered
        .events
        .iter()
        .filter(|event| {
            matches!(
                event,
                OutputEvent::State(state)
                    if state.event == protocol::EngineStateEvent::KillSwitchEngaged
            )
        })
        .count();
    assert_eq!(kills, 1);
    std::fs::remove_file(&path).unwrap();
}

/// Phases are separated by long paced gaps so the control thread can act
/// between them without racing the feed.
fn kill_switch_workload() -> Vec<protocol::EngineInput> {
    let mut build = Workload::default();

    build.market(1_000, MarketEventKind::SnapshotBegin { snapshot_seq: 1 });
    build.market(
        1_001,
        MarketEventKind::SnapshotLevel {
            side: Side::Buy,
            price: PriceTicks(99),
            quantity: QuantityLots(100),
        },
    );
    build.market(
        1_002,
        MarketEventKind::SnapshotLevel {
            side: Side::Sell,
            price: PriceTicks(101),
            quantity: QuantityLots(100),
        },
    );
    build.market(
        1_003,
        MarketEventKind::SnapshotEnd {
            snapshot_seq: 1,
            level_count: 2,
        },
    );

    // Phase 1: before any kill. Phase 2: killed. Phase 3: released. Phase 4: killed again.
    // Each phase opens with a heartbeat so the market never goes stale.
    build.market(PHASE_NS - 1, MarketEventKind::Heartbeat);
    build.order(PHASE_NS, RequestKind::New, 1, 100, 0);
    build.market(2 * PHASE_NS - 1, MarketEventKind::Heartbeat);
    build.order(2 * PHASE_NS, RequestKind::New, 2, 200, 0);
    build.order(2 * PHASE_NS + 1_000, RequestKind::Cancel, 3, 0, 100);
    build.market(3 * PHASE_NS - 1, MarketEventKind::Heartbeat);
    build.order(3 * PHASE_NS, RequestKind::New, 4, 300, 0);
    build.market(4 * PHASE_NS - 1, MarketEventKind::Heartbeat);
    build.order(4 * PHASE_NS, RequestKind::New, 5, 400, 0);
    build.inputs
}

const PHASE_NS: u64 = 500_000_000;

#[derive(Default)]
struct Workload {
    inputs: Vec<EngineInput>,
    ingress: u64,
    source_seq: u64,
}

impl Workload {
    fn push(&mut self, time_ns: u64, event: InputEvent) {
        self.ingress += 1;
        self.inputs.push(EngineInput {
            ingress_seq: IngressSeq(self.ingress),
            recv_time_ns: time_ns,
            event,
        });
    }

    fn market(&mut self, time_ns: u64, kind: MarketEventKind) {
        self.source_seq += 1;
        let event = InputEvent::Market(MarketEvent {
            instrument: InstrumentId(1),
            source_seq: self.source_seq,
            source_time_ns: time_ns,
            kind,
        });
        self.push(time_ns, event);
    }

    fn order(
        &mut self,
        time_ns: u64,
        kind: RequestKind,
        id: u64,
        client_order_id: u64,
        target: u64,
    ) {
        let request = OrderRequest {
            kind,
            account: AccountId(1),
            instrument: InstrumentId(1),
            request_id: RequestId(id),
            client_seq: id,
            client_order_id: ClientOrderId(client_order_id),
            target_client_order_id: ClientOrderId(target),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: PriceTicks(99),
            quantity: QuantityLots(1),
        };
        self.push(time_ns, InputEvent::Order(request));
    }
}

#[test]
fn the_kill_switch_can_be_released_and_engaged_again() {
    use protocol::{ClientOrderId, EngineStateEvent, RejectReason, ReportKind};

    let path = temp_path("kill-cycle.journal");
    let control = ControlHandle::default();
    let controller = control.clone();
    let toggles = std::thread::spawn(move || {
        let phase = std::time::Duration::from_millis(500);
        std::thread::sleep(phase + phase / 2);
        controller.engage_kill();
        controller.engage_kill();
        std::thread::sleep(phase);
        controller.release_kill();
        controller.release_kill();
        std::thread::sleep(phase);
        controller.engage_kill();
    });

    engine_runtime::run(
        EngineConfig::single_instrument(1),
        PipelineConfig {
            journal_path: Some(path.clone()),
            paced: true,
            ..PipelineConfig::default()
        },
        FeedSource::Memory(kill_switch_workload()),
        control,
    )
    .unwrap();
    toggles.join().unwrap();

    let events = read_journal(&path).unwrap().events;
    std::fs::remove_file(&path).unwrap();

    let state_events: Vec<EngineStateEvent> = events
        .iter()
        .filter_map(|event| match event {
            OutputEvent::State(state) => Some(state.event),
            _ => None,
        })
        .filter(|event| {
            matches!(
                event,
                EngineStateEvent::KillSwitchEngaged | EngineStateEvent::KillSwitchReleased
            )
        })
        .collect();
    assert_eq!(
        state_events,
        vec![
            EngineStateEvent::KillSwitchEngaged,
            EngineStateEvent::KillSwitchReleased,
            EngineStateEvent::KillSwitchEngaged,
        ]
    );

    let report_for = |client_order_id: u64| {
        events
            .iter()
            .filter_map(|event| match event {
                OutputEvent::Report(report)
                    if report.client_order_id == ClientOrderId(client_order_id) =>
                {
                    Some(*report)
                }
                _ => None,
            })
            .next_back()
            .unwrap_or_else(|| panic!("no report for {client_order_id}"))
    };

    assert_eq!(report_for(100).kind, ReportKind::Cancelled);
    assert_eq!(
        report_for(200).reject_reason,
        Some(RejectReason::GlobalKillActive)
    );
    assert_eq!(report_for(300).kind, ReportKind::Accepted);
    assert_eq!(
        report_for(400).reject_reason,
        Some(RejectReason::GlobalKillActive)
    );
}

/// Replays the recorded engine input stream and asserts it reproduces the run.
fn assert_replays_exactly(run_id: u128, summary: &engine_runtime::RunSummary) {
    assert_eq!(summary.captured_inputs.len() as u64, summary.inputs);
    let mut replay = Replay::new(TradingCore::new(EngineConfig::single_instrument(run_id)), 0);
    replay.run(&summary.captured_inputs);
    let expected = replay.finish();

    assert_eq!(summary.input_digest, expected.input_digest);
    assert_eq!(summary.output_digest, expected.output_digest);
    assert_eq!(summary.state_digest, expected.state_digest);
    assert_eq!(summary.outputs, expected.outputs);
}

/// Runs the paced control workload while `operate` drives the control surface.
fn run_with_operator(
    run_id: u128,
    control: ControlHandle,
    operate: impl FnOnce(ControlHandle) + Send + 'static,
) -> engine_runtime::RunSummary {
    let operator = std::thread::spawn({
        let control = control.clone();
        move || operate(control)
    });
    let summary = engine_runtime::run(
        EngineConfig::single_instrument(run_id),
        PipelineConfig {
            capture_inputs: true,
            paced: true,
            ..PipelineConfig::default()
        },
        FeedSource::Memory(kill_switch_workload()),
        control,
    )
    .unwrap();
    operator.join().unwrap();
    summary
}

#[test]
fn a_run_without_control_replays_from_the_recorded_inputs() {
    let summary = engine_runtime::run(
        EngineConfig::single_instrument(51),
        PipelineConfig {
            capture_inputs: true,
            ..PipelineConfig::default()
        },
        FeedSource::Memory(workload(4242, 20_000)),
        ControlHandle::default(),
    )
    .unwrap();

    assert_eq!(summary.inputs, summary.feed_inputs);
    assert_replays_exactly(51, &summary);
}

#[test]
fn an_account_disable_is_part_of_the_recorded_input_stream() {
    let summary = run_with_operator(52, ControlHandle::default(), |control| {
        std::thread::sleep(std::time::Duration::from_millis(750));
        assert!(control.submit(ControlCommand::SetAccountEnabled {
            account: AccountId(1),
            enabled: false,
        }));
    });

    assert_eq!(summary.inputs, summary.feed_inputs + 1);
    let controls = summary
        .captured_inputs
        .iter()
        .filter(|input| matches!(input.event, InputEvent::Control(_)))
        .count();
    assert_eq!(controls, 1);
    assert_replays_exactly(52, &summary);
}

#[test]
fn a_risk_limit_change_is_part_of_the_recorded_input_stream() {
    let limits = protocol::AccountLimits {
        max_order_quantity: QuantityLots(3),
        max_order_notional: protocol::Notional(1_000_000),
        max_position_lots: 100,
        max_gross_exposure: protocol::Notional(10_000_000),
        price_collar_ticks: 5_000,
    };
    let summary = run_with_operator(53, ControlHandle::default(), move |control| {
        std::thread::sleep(std::time::Duration::from_millis(750));
        assert!(control.submit(ControlCommand::SetAccountLimits {
            account: AccountId(1),
            limits,
        }));
    });

    assert_eq!(summary.inputs, summary.feed_inputs + 1);
    assert_replays_exactly(53, &summary);
}

#[test]
fn kill_transitions_are_part_of_the_recorded_input_stream() {
    let summary = run_with_operator(54, ControlHandle::default(), |control| {
        let phase = std::time::Duration::from_millis(500);
        std::thread::sleep(phase + phase / 2);
        control.engage_kill();
        std::thread::sleep(phase);
        control.release_kill();
        std::thread::sleep(phase);
        control.engage_kill();
    });

    assert_eq!(summary.inputs, summary.feed_inputs + 3);
    assert_replays_exactly(54, &summary);
}

#[test]
fn group_commit_publishes_records_while_the_run_is_still_going() {
    let path = temp_path("group-commit.journal");
    let control = ControlHandle::default();
    let seen = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = std::sync::Arc::clone(&seen);
    let live = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let still_running = std::sync::Arc::clone(&live);
    let watch_path = path.clone();

    // Tail the journal while the paced run is still producing output.
    let reader = std::thread::spawn(move || {
        let mut tail = attach_tail(&watch_path);
        while still_running.load(std::sync::atomic::Ordering::Acquire) {
            let events = tail.poll(1_000).expect("tail poll");
            counted.fetch_add(events.len(), std::sync::atomic::Ordering::Relaxed);
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    });

    let summary = engine_runtime::run(
        EngineConfig::single_instrument(61),
        PipelineConfig {
            journal_path: Some(path.clone()),
            journal_sync: engine_runtime::JournalSync::GroupCommit(8),
            paced: true,
            ..PipelineConfig::default()
        },
        FeedSource::Memory(kill_switch_workload()),
        control.clone(),
    )
    .unwrap();

    live.store(false, std::sync::atomic::Ordering::Release);
    control.request_shutdown();
    reader.join().unwrap();

    let during = seen.load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        during > 0,
        "the tail saw nothing while the run was in progress"
    );

    let recovered = read_journal(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    assert!(recovered.truncated_at.is_none());
    assert_eq!(recovered.events.len() as u64, summary.outputs);
}

/// Builds a core holding `orders` resting limit orders spread over 200 prices
/// on each side, so the book is far larger than any snapshot depth.
fn book_core(orders: u64) -> TradingCore {
    let mut core = TradingCore::new(EngineConfig::single_instrument(81));
    let mut out = Vec::new();
    let mut push = |core: &mut TradingCore, seq: u64, event: InputEvent| {
        core.apply(
            &EngineInput {
                ingress_seq: IngressSeq(seq),
                recv_time_ns: seq * 1_000,
                event,
            },
            &mut out,
        );
    };

    let mut seq = 0;
    for kind in [
        MarketEventKind::SnapshotBegin { snapshot_seq: 1 },
        MarketEventKind::SnapshotLevel {
            side: Side::Buy,
            price: PriceTicks(1_000),
            quantity: QuantityLots(50),
        },
        MarketEventKind::SnapshotLevel {
            side: Side::Sell,
            price: PriceTicks(1_400),
            quantity: QuantityLots(50),
        },
        MarketEventKind::SnapshotEnd {
            snapshot_seq: 1,
            level_count: 2,
        },
    ] {
        seq += 1;
        push(
            &mut core,
            seq,
            InputEvent::Market(MarketEvent {
                instrument: InstrumentId(1),
                source_seq: seq,
                source_time_ns: seq * 1_000,
                kind,
            }),
        );
    }

    for index in 0..orders {
        seq += 1;
        // Bids sit below the market, asks above it, so nothing crosses.
        let buy = index % 2 == 0;
        let offset = (index / 2) % 200;
        let (side, price) = if buy {
            (Side::Buy, 999 - offset as i64)
        } else {
            (Side::Sell, 1_401 + offset as i64)
        };
        let account = AccountId((index % 4) as u32 + 1);
        push(
            &mut core,
            seq,
            InputEvent::Order(OrderRequest {
                kind: RequestKind::New,
                account,
                instrument: InstrumentId(1),
                request_id: RequestId(index + 1),
                client_seq: index / 4 + 1,
                client_order_id: ClientOrderId(index + 1),
                target_client_order_id: ClientOrderId(0),
                side,
                order_type: OrderType::Limit,
                price: PriceTicks(price),
                quantity: QuantityLots(1),
            }),
        );
    }
    core
}

#[test]
fn producer_delay_lands_on_arrival_latency_and_not_on_enqueue_latency() {
    use engine_runtime::harness::{open_loop_pipeline, BenchConfig};

    let delay_ns = 200_000u64;
    let mut config = BenchConfig::new(1_024, engine_runtime::WaitStrategy::BusySpin, 20_000);
    config.producer_delay_ns = delay_ns;
    let result = open_loop_pipeline(
        EngineConfig::single_instrument(71),
        workload(9, 2_000),
        config,
    );

    assert!(result.arrival_to_report.count > 0);
    // The producer stall belongs to arrival latency. Compare the two
    // boundaries: enqueue latency can spike under load, but the extra
    // delay must still show up only on the arrival clock.
    let extra = result
        .arrival_to_report
        .p50_ns
        .saturating_sub(result.enqueue_to_report.p50_ns);
    assert!(
        extra >= delay_ns,
        "arrival-enqueue p50 gap {extra}ns did not include the {delay_ns}ns producer delay (arrival={} enqueue={})",
        result.arrival_to_report.p50_ns,
        result.enqueue_to_report.p50_ns
    );
}

#[test]
fn snapshot_capture_is_bounded_by_the_requested_depth() {
    use engine_runtime::snapshot::capture;

    let depth = 16;
    let small = book_core(100);
    let large = book_core(4_000);

    let small_snapshot = capture(&small, depth);
    let large_snapshot = capture(&large, depth);
    let bounded = &large_snapshot.instruments[0];

    assert!(bounded.levels.len() <= depth * 2);
    assert!(bounded.orders.len() <= depth * 8);
    assert_eq!(small_snapshot.instruments[0].levels[0].side, Side::Buy);

    // The captured levels are the best resting levels, in book order.
    assert_eq!(bounded.levels[0].price, PriceTicks(999));
    let bids: Vec<i64> = bounded
        .levels
        .iter()
        .filter(|level| level.side == Side::Buy)
        .map(|level| level.price.0)
        .collect();
    assert!(bids.windows(2).all(|pair| pair[0] > pair[1]), "{bids:?}");
    let asks: Vec<i64> = bounded
        .levels
        .iter()
        .filter(|level| level.side == Side::Sell)
        .map(|level| level.price.0)
        .collect();
    assert!(asks.windows(2).all(|pair| pair[0] < pair[1]), "{asks:?}");

    // Every captured order sits on a captured level.
    let captured: std::collections::HashSet<i64> =
        bounded.levels.iter().map(|level| level.price.0).collect();
    for order in &bounded.orders {
        assert!(captured.contains(&order.price.0));
    }
}

#[test]
fn snapshot_capture_cost_does_not_follow_the_live_order_count() {
    use engine_runtime::snapshot::capture;
    use std::time::Instant;

    let depth = 16;
    let small = book_core(100);
    let large = book_core(4_000);

    // Warm both paths before timing them.
    capture(&small, depth);
    capture(&large, depth);

    let measure = |core: &TradingCore| {
        let start = Instant::now();
        for _ in 0..200 {
            std::hint::black_box(capture(core, depth));
        }
        start.elapsed().as_nanos() as f64 / 200.0
    };
    let small_ns = measure(&small);
    let large_ns = measure(&large);

    println!("snapshot capture: 100 orders {small_ns:.0}ns, 4000 orders {large_ns:.0}ns");
    // Forty times the book must not cost anywhere near forty times the capture.
    assert!(
        large_ns < small_ns * 4.0,
        "capture cost scaled with the book: {small_ns:.0}ns -> {large_ns:.0}ns"
    );
}

/// Accepts a fixed number of writes, then fails like a full disk.
struct FailingSink {
    written: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
    remaining_writes: usize,
}

impl std::io::Write for FailingSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.remaining_writes == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::StorageFull,
                "journal device is full",
            ));
        }
        self.remaining_writes -= 1;
        self.written.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn an_unwritable_journal_stops_the_engine_instead_of_dropping_output() {
    let inputs = workload(31, 5_000);

    let good_path = temp_path("durable-good.journal");
    let good = engine_runtime::run(
        EngineConfig::single_instrument(41),
        PipelineConfig {
            journal_path: Some(good_path.clone()),
            ..PipelineConfig::default()
        },
        FeedSource::Memory(inputs.clone()),
        ControlHandle::default(),
    )
    .unwrap();
    let expected = read_journal(&good_path).unwrap().events;
    std::fs::remove_file(&good_path).unwrap();
    assert_eq!(expected.len() as u64, good.outputs);

    let written = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = FailingSink {
        written: written.clone(),
        remaining_writes: 40,
    };
    let control = ControlHandle::default();
    let error = engine_runtime::run_with_journal(
        EngineConfig::single_instrument(41),
        PipelineConfig {
            journal_sync: engine_runtime::JournalSync::Durable,
            ..PipelineConfig::default()
        },
        FeedSource::Memory(inputs.clone()),
        control.clone(),
        Some(engine_runtime::JournalWriter::with_sink(Box::new(sink), 41).unwrap()),
    )
    .unwrap_err();

    match error {
        engine_runtime::RuntimeError::Journal(engine_runtime::JournalError::Io(io)) => {
            assert_eq!(io.kind(), std::io::ErrorKind::StorageFull);
        }
        other => panic!("expected a journal io failure, got {other:?}"),
    }
    assert!(
        control.shutdown_requested(),
        "the engine must fail closed when output cannot be recorded"
    );

    // Whatever reached the journal is an exact prefix of the healthy run, and
    // the run stopped well before the end of the input.
    let partial_path = temp_path("durable-partial.journal");
    std::fs::write(&partial_path, written.lock().unwrap().as_slice()).unwrap();
    let partial = read_journal(&partial_path).unwrap();
    std::fs::remove_file(&partial_path).unwrap();

    assert!(partial.truncated_at.is_none());
    assert!(!partial.events.is_empty());
    assert!(partial.events.len() < expected.len());
    assert_eq!(partial.events, expected[..partial.events.len()]);
}

#[test]
fn control_commands_are_applied_between_inputs() {
    let control = ControlHandle::default();
    assert!(control.submit(ControlCommand::SetAccountEnabled {
        account: AccountId(1),
        enabled: false,
    }));

    let summary = engine_runtime::run(
        EngineConfig::single_instrument(2),
        PipelineConfig::default(),
        FeedSource::Memory(workload(12, 1_000)),
        control,
    )
    .unwrap();

    assert!(summary.outputs > 0);
}

#[test]
fn snapshots_are_published_with_their_engine_sequence() {
    let control = ControlHandle::new(16, 8);
    let summary = engine_runtime::run(
        EngineConfig::single_instrument(2),
        PipelineConfig {
            snapshot_interval: 100,
            snapshot_depth: 8,
            ..PipelineConfig::default()
        },
        FeedSource::Memory(workload(21, 3_000)),
        control.clone(),
    )
    .unwrap();

    let snapshot = control.take_snapshot().expect("at least one snapshot");
    assert!(snapshot.as_of_engine_seq.0 > 0);
    assert_eq!(snapshot.instruments.len(), 1);
    assert!(summary.telemetry_dropped > 0, "expected counted drops");
}
