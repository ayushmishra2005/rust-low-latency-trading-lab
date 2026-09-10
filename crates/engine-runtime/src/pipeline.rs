//! Three long-lived OS threads connected by bounded SPSC queues:
//! feed -> engine -> output. Only the engine thread touches trading state.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crossbeam_queue::ArrayQueue;
use protocol::codec::{FileHeader, Frame};
use protocol::{ControlCommand, EngineInput, IngressSeq, OutputEvent};
use trading_core::generator::from_frame;
use trading_core::replay::{state_digest, Digest, DigestRecorder};
use trading_core::{EngineConfig, TradingCore};

use crate::journal::{JournalError, JournalWriter};
use crate::queue::{bounded, QueueStats, WaitStrategy};
use crate::snapshot::{capture, EngineSnapshot};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineConfig {
    pub input_capacity: usize,
    pub output_capacity: usize,
    pub telemetry_capacity: usize,
    pub wait: WaitStrategy,
    pub journal_path: Option<PathBuf>,
    /// Flush and sync the journal on every batch instead of at the end.
    pub durable_ack: bool,
    /// Engine sequences between state checkpoints. Zero disables checkpoints.
    pub checkpoint_interval: u64,
    /// Engine sequences between read-model snapshots. Zero disables them.
    pub snapshot_interval: u64,
    pub snapshot_depth: usize,
}

impl Default for PipelineConfig {
    fn default() -> PipelineConfig {
        PipelineConfig {
            input_capacity: 8_192,
            output_capacity: 8_192,
            telemetry_capacity: 64,
            wait: WaitStrategy::default(),
            journal_path: None,
            durable_ack: false,
            checkpoint_interval: 0,
            snapshot_interval: 0,
            snapshot_depth: 16,
        }
    }
}

pub enum FeedSource {
    /// Already normalized inputs, used by tests and benchmarks.
    Memory(Vec<EngineInput>),
    /// Encoded feed file contents; the feed thread decodes and validates frames.
    Bytes(Vec<u8>),
}

/// Cold control surface. The kill latch is read by the engine before each
/// risk-increasing request; other commands are ordinary sequenced inputs.
#[derive(Clone)]
pub struct ControlHandle {
    kill: Arc<AtomicBool>,
    commands: Arc<ArrayQueue<ControlCommand>>,
    snapshots: Arc<ArrayQueue<EngineSnapshot>>,
    telemetry_dropped: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
}

impl ControlHandle {
    pub fn new(command_capacity: usize, snapshot_capacity: usize) -> ControlHandle {
        ControlHandle {
            kill: Arc::new(AtomicBool::new(false)),
            commands: Arc::new(ArrayQueue::new(command_capacity.max(1))),
            snapshots: Arc::new(ArrayQueue::new(snapshot_capacity.max(1))),
            telemetry_dropped: Arc::new(AtomicU64::new(0)),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Latches the emergency kill. Fail-closed: it is never cleared implicitly.
    pub fn engage_kill(&self) {
        self.kill.store(true, Ordering::Release);
    }

    pub fn kill_engaged(&self) -> bool {
        self.kill.load(Ordering::Acquire)
    }

    /// Returns false when the control queue is full; the caller must retry.
    pub fn submit(&self, command: ControlCommand) -> bool {
        self.commands.push(command).is_ok()
    }

    pub fn take_snapshot(&self) -> Option<EngineSnapshot> {
        self.snapshots.pop()
    }

    pub fn telemetry_dropped(&self) -> u64 {
        self.telemetry_dropped.load(Ordering::Relaxed)
    }

    pub fn request_shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    pub fn shutdown_requested(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }
}

impl Default for ControlHandle {
    fn default() -> ControlHandle {
        ControlHandle::new(64, 4)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueReport {
    pub capacity: usize,
    pub pushed: u64,
    pub popped: u64,
    pub full_events: u64,
    pub high_water: u64,
}

fn report(stats: &QueueStats, capacity: usize) -> QueueReport {
    QueueReport {
        capacity,
        pushed: stats.pushed(),
        popped: stats.popped(),
        full_events: stats.full_events(),
        high_water: stats.high_water(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSummary {
    pub inputs: u64,
    pub outputs: u64,
    pub trades: u64,
    pub journal_records: u64,
    pub input_digest: Digest,
    pub output_digest: Digest,
    pub state_digest: Digest,
    pub input_queue: QueueReport,
    pub output_queue: QueueReport,
    pub telemetry_dropped: u64,
    pub decode_errors: u64,
    pub elapsed_ns: u128,
}

#[derive(Debug)]
pub enum RuntimeError {
    Journal(JournalError),
    Feed(String),
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeError::Journal(error) => write!(f, "{error}"),
            RuntimeError::Feed(message) => write!(f, "feed error: {message}"),
        }
    }
}

impl std::error::Error for RuntimeError {}

struct EngineOutcome {
    state_digest: Digest,
    trades: u64,
}

struct OutputOutcome {
    outputs: u64,
    digest: Digest,
    journal_records: u64,
    journal_error: Option<JournalError>,
}

/// Runs the whole pipeline to completion and joins every thread.
pub fn run(
    engine_config: EngineConfig,
    pipeline_config: PipelineConfig,
    source: FeedSource,
    control: ControlHandle,
) -> Result<RunSummary, RuntimeError> {
    let (mut input_tx, mut input_rx) =
        bounded::<EngineInput>(pipeline_config.input_capacity, pipeline_config.wait);
    let (mut output_tx, mut output_rx) =
        bounded::<OutputEvent>(pipeline_config.output_capacity, pipeline_config.wait);
    let input_stats = input_tx.stats();
    let output_stats = output_tx.stats();
    let run_id = engine_config.run_id;

    let started = Instant::now();
    let feed_control = control.clone();
    let feed = std::thread::Builder::new()
        .name("feed".to_string())
        .spawn(move || {
            let mut recorder = DigestRecorder::new();
            let mut decode_errors = 0u64;
            match source {
                FeedSource::Memory(inputs) => {
                    for input in inputs {
                        if feed_control.shutdown_requested() {
                            break;
                        }
                        recorder.record_input(&input);
                        if !input_tx.send(input) {
                            break;
                        }
                    }
                }
                FeedSource::Bytes(bytes) => {
                    let mut offset = match FileHeader::decode(&bytes) {
                        Ok((_, consumed)) => consumed,
                        Err(_) => {
                            decode_errors += 1;
                            bytes.len()
                        }
                    };
                    let mut ingress = 0u64;
                    while offset < bytes.len() {
                        if feed_control.shutdown_requested() {
                            break;
                        }
                        match Frame::decode(&bytes[offset..]) {
                            Ok((frame, consumed)) => {
                                offset += consumed;
                                ingress += 1;
                                let input = from_frame(&frame, IngressSeq(ingress));
                                recorder.record_input(&input);
                                if !input_tx.send(input) {
                                    break;
                                }
                            }
                            Err(_) => {
                                // Strict replay stops at the first malformed frame.
                                decode_errors += 1;
                                break;
                            }
                        }
                    }
                }
            }
            (recorder.inputs, recorder.input_digest(), decode_errors)
        })
        .expect("feed thread");

    let engine_control = control.clone();
    let engine_pipeline = pipeline_config.clone();
    let engine = std::thread::Builder::new()
        .name("engine".to_string())
        .spawn(move || {
            let mut core = TradingCore::new(engine_config);
            let mut batch: Vec<OutputEvent> = Vec::with_capacity(64);
            let mut kill_applied = false;
            let mut ingress = 0u64;

            loop {
                // Cold control is applied between inputs and gets its own sequence.
                if engine_control.kill_engaged() && !kill_applied {
                    kill_applied = true;
                    ingress += 1;
                    apply_control(
                        &mut core,
                        &mut batch,
                        &mut output_tx,
                        ingress,
                        ControlCommand::SetGlobalKill { engaged: true },
                    );
                }
                while let Some(command) = engine_control.commands.pop() {
                    ingress += 1;
                    apply_control(&mut core, &mut batch, &mut output_tx, ingress, command);
                }

                let Some(input) = input_rx.recv() else { break };
                core.apply(&input, &mut batch);
                for event in &batch {
                    if !output_tx.send(*event) {
                        break;
                    }
                }

                let engine_seq = core.engine_seq().0;
                if engine_pipeline.snapshot_interval > 0
                    && engine_seq % engine_pipeline.snapshot_interval == 0
                {
                    let snapshot = capture(&core, engine_pipeline.snapshot_depth);
                    // Telemetry is best effort and every drop is counted.
                    if engine_control.snapshots.force_push(snapshot).is_some() {
                        engine_control
                            .telemetry_dropped
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            if engine_pipeline.snapshot_interval > 0 {
                let snapshot = capture(&core, engine_pipeline.snapshot_depth);
                if engine_control.snapshots.force_push(snapshot).is_some() {
                    engine_control
                        .telemetry_dropped
                        .fetch_add(1, Ordering::Relaxed);
                }
            }

            EngineOutcome {
                state_digest: state_digest(&core),
                trades: core.metrics().trades,
            }
        })
        .expect("engine thread");

    let journal_path = pipeline_config.journal_path.clone();
    let durable_ack = pipeline_config.durable_ack;
    let output = std::thread::Builder::new()
        .name("output".to_string())
        .spawn(move || {
            let mut writer = match journal_path {
                Some(path) => match JournalWriter::create(&path, run_id) {
                    Ok(writer) => Some(writer),
                    Err(error) => {
                        return OutputOutcome {
                            outputs: 0,
                            digest: [0u8; 32],
                            journal_records: 0,
                            journal_error: Some(error),
                        }
                    }
                },
                None => None,
            };
            let mut recorder = DigestRecorder::new();
            let mut journal_error = None;

            while let Some(event) = output_rx.recv() {
                recorder.record_output(&event);
                if let Some(writer) = writer.as_mut() {
                    if let Err(error) = writer.append(&event) {
                        journal_error = Some(error);
                        break;
                    }
                    if durable_ack {
                        if let Err(error) = writer.sync() {
                            journal_error = Some(error);
                            break;
                        }
                    }
                }
            }

            let mut journal_records = 0;
            if let Some(writer) = writer.as_mut() {
                journal_records = writer.records();
                if let Err(error) = writer.sync() {
                    journal_error = journal_error.or(Some(error));
                }
            }

            OutputOutcome {
                outputs: recorder.outputs,
                digest: recorder.output_digest(),
                journal_records,
                journal_error,
            }
        })
        .expect("output thread");

    let (inputs, input_digest, decode_errors) = feed.join().expect("feed thread panicked");
    let engine_outcome = engine.join().expect("engine thread panicked");
    let output_outcome = output.join().expect("output thread panicked");
    let elapsed_ns = started.elapsed().as_nanos();

    if let Some(error) = output_outcome.journal_error {
        return Err(RuntimeError::Journal(error));
    }

    Ok(RunSummary {
        inputs,
        outputs: output_outcome.outputs,
        trades: engine_outcome.trades,
        journal_records: output_outcome.journal_records,
        input_digest,
        output_digest: output_outcome.digest,
        state_digest: engine_outcome.state_digest,
        input_queue: report(&input_stats, pipeline_config.input_capacity),
        output_queue: report(&output_stats, pipeline_config.output_capacity),
        telemetry_dropped: control.telemetry_dropped(),
        decode_errors,
        elapsed_ns,
    })
}

fn apply_control(
    core: &mut TradingCore,
    batch: &mut Vec<OutputEvent>,
    output_tx: &mut crate::queue::Sender<OutputEvent>,
    ingress: u64,
    command: ControlCommand,
) {
    let input = EngineInput {
        ingress_seq: IngressSeq(ingress),
        recv_time_ns: core.engine_time_ns(),
        event: protocol::InputEvent::Control(command),
    };
    core.apply(&input, batch);
    for event in batch.iter() {
        if !output_tx.send(*event) {
            break;
        }
    }
}
