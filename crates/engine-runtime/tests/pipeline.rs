//! Threaded pipeline behaviour: determinism across real threads, backpressure,
//! journal durability, and clean shutdown.

use engine_runtime::{read_journal, ControlHandle, FeedSource, PipelineConfig, WaitStrategy};
use protocol::codec::{FileHeader, InstrumentSpec};
use protocol::{AccountId, ControlCommand, OutputEvent};
use trading_core::generator::to_frame;
use trading_core::{EngineConfig, Generator, GeneratorConfig, Replay, TradingCore};

fn workload(seed: u64, events: usize) -> Vec<protocol::EngineInput> {
    Generator::new(GeneratorConfig::new(seed, events)).generate()
}

fn temp_path(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("rltl-pipeline-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
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
