//! Three long-lived OS threads connected by bounded SPSC queues:
//! feed -> engine -> output. Only the engine thread touches trading state.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crossbeam_queue::ArrayQueue;
use protocol::codec::{FileHeader, Frame};
use protocol::{ControlCommand, EngineInput, IngressSeq, InputEvent, OutputEvent};
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
    pub journal_sync: JournalSync,
    /// Keep the applied input stream so the run can be replayed exactly.
    pub capture_inputs: bool,
    /// Engine sequences between state checkpoints. Zero disables checkpoints.
    pub checkpoint_interval: u64,
    /// Engine sequences between read-model snapshots. Zero disables them.
    pub snapshot_interval: u64,
    pub snapshot_depth: usize,
    /// Reproduce recorded inter-arrival deltas instead of publishing as fast as
    /// possible. Used for demos, not for latency measurement.
    pub paced: bool,
}

impl Default for PipelineConfig {
    fn default() -> PipelineConfig {
        PipelineConfig {
            input_capacity: 8_192,
            output_capacity: 8_192,
            telemetry_capacity: 64,
            wait: WaitStrategy::default(),
            journal_path: None,
            journal_sync: JournalSync::Buffered,
            capture_inputs: false,
            checkpoint_interval: 0,
            snapshot_interval: 0,
            snapshot_depth: 16,
            paced: false,
        }
    }
}

/// How often the output thread pushes journal records past its buffer.
///
/// Visibility means a reader such as [`crate::JournalTail`] can see the record.
/// Durability means the record survives a machine failure. They are different
/// costs and this enum keeps them apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalSync {
    /// Write into the buffer and flush when it fills or the run ends. A reader
    /// may not see recent records. Lowest cost, weakest contract.
    Buffered,
    /// Flush after at most this many records, and whenever the output queue
    /// drains, so a reader sees every record within that bound. No fsync, so
    /// this is a visibility contract, not a durability one.
    GroupCommit(u64),
    /// Flush and fsync every record before accepting the next one. Visible and
    /// durable, at the cost of one fsync per output event.
    Durable,
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

    /// Requests the emergency kill. Only an explicit release clears it.
    pub fn engage_kill(&self) {
        self.kill.store(true, Ordering::Release);
    }

    /// Releases the kill so trading can resume, and so it can be engaged again.
    pub fn release_kill(&self) {
        self.kill.store(false, Ordering::Release);
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
    /// Inputs the engine applied, including control commands.
    pub inputs: u64,
    /// Inputs the feed produced. Control commands are not counted here.
    pub feed_inputs: u64,
    /// The applied input stream, present when `capture_inputs` is set.
    pub captured_inputs: Vec<EngineInput>,
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
    /// The engine stopped because its output could no longer be recorded.
    OutputUnavailable {
        inputs_applied: u64,
    },
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeError::Journal(error) => write!(f, "{error}"),
            RuntimeError::Feed(message) => write!(f, "feed error: {message}"),
            RuntimeError::OutputUnavailable { inputs_applied } => write!(
                f,
                "output unavailable: engine stopped after {inputs_applied} inputs"
            ),
        }
    }
}

impl std::error::Error for RuntimeError {}

struct EngineOutcome {
    state_digest: Digest,
    trades: u64,
    inputs_applied: u64,
    input_digest: Digest,
    captured_inputs: Vec<EngineInput>,
    output_lost: bool,
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
    run_with_journal(engine_config, pipeline_config, source, control, None)
}

/// Same as [`run`], with a caller supplied journal instead of one opened from
/// the configured path.
pub fn run_with_journal(
    engine_config: EngineConfig,
    pipeline_config: PipelineConfig,
    source: FeedSource,
    control: ControlHandle,
    journal: Option<JournalWriter>,
) -> Result<RunSummary, RuntimeError> {
    let (mut input_tx, mut input_rx) =
        bounded::<EngineInput>(pipeline_config.input_capacity, pipeline_config.wait);
    let (mut output_tx, mut output_rx) =
        bounded::<OutputEvent>(pipeline_config.output_capacity, pipeline_config.wait);
    // Raised by the output thread when it can no longer record anything.
    let output_failed = Arc::new(AtomicBool::new(false));
    let input_stats = input_tx.stats();
    let output_stats = output_tx.stats();
    let run_id = engine_config.run_id;

    let started = Instant::now();
    let feed_control = control.clone();
    let paced = pipeline_config.paced;
    let feed = std::thread::Builder::new()
        .name("feed".to_string())
        .spawn(move || {
            let mut produced = 0u64;
            let mut decode_errors = 0u64;
            match source {
                FeedSource::Memory(inputs) => {
                    let mut previous_recv_ns = inputs.first().map_or(0, |first| first.recv_time_ns);
                    for input in inputs {
                        if feed_control.shutdown_requested() {
                            break;
                        }
                        if paced {
                            let delta = input.recv_time_ns.saturating_sub(previous_recv_ns);
                            previous_recv_ns = input.recv_time_ns;
                            if delta > 0 {
                                std::thread::sleep(std::time::Duration::from_nanos(delta));
                            }
                        }
                        produced += 1;
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
                    while offset < bytes.len() {
                        if feed_control.shutdown_requested() {
                            break;
                        }
                        match Frame::decode(&bytes[offset..]) {
                            Ok((frame, consumed)) => {
                                offset += consumed;
                                produced += 1;
                                // The engine assigns the canonical sequence.
                                let input = from_frame(&frame, IngressSeq(produced));
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
            (produced, decode_errors)
        })
        .expect("feed thread");

    let engine_control = control.clone();
    let engine_pipeline = pipeline_config.clone();
    let engine_output_failed = Arc::clone(&output_failed);
    let engine = std::thread::Builder::new()
        .name("engine".to_string())
        .spawn(move || {
            let mut engine = Engine::new(engine_config, engine_pipeline.capture_inputs);
            let mut output_lost = false;

            let mut pending: Option<EngineInput> = None;
            loop {
                // Control is applied just before the next input, so a kill can
                // never be overtaken by an order that is already queued.
                if pending.is_none() {
                    pending = input_rx.recv();
                    if pending.is_none() {
                        break;
                    }
                }
                if engine_output_failed.load(Ordering::Acquire) {
                    output_lost = true;
                    break;
                }

                let requested_kill = engine_control.kill_engaged();
                if requested_kill != engine.core.global_kill() {
                    let command = ControlCommand::SetGlobalKill {
                        engaged: requested_kill,
                    };
                    if !engine.apply(&mut output_tx, InputEvent::Control(command), None) {
                        output_lost = true;
                        break;
                    }
                }
                while let Some(command) = engine_control.commands.pop() {
                    if !engine.apply(&mut output_tx, InputEvent::Control(command), None) {
                        output_lost = true;
                        break;
                    }
                }
                if output_lost {
                    break;
                }

                let input = pending.take().expect("input is present");
                if !engine.apply(&mut output_tx, input.event, Some(input.recv_time_ns)) {
                    output_lost = true;
                    break;
                }

                let engine_seq = engine.core.engine_seq().0;
                if engine_pipeline.snapshot_interval > 0
                    && engine_seq % engine_pipeline.snapshot_interval == 0
                {
                    let snapshot = capture(&engine.core, engine_pipeline.snapshot_depth);
                    // Telemetry is best effort and every drop is counted.
                    if engine_control.snapshots.force_push(snapshot).is_some() {
                        engine_control
                            .telemetry_dropped
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            if output_lost {
                // Nothing downstream can record further execution.
                engine_control.request_shutdown();
            }
            if engine_pipeline.snapshot_interval > 0 {
                let snapshot = capture(&engine.core, engine_pipeline.snapshot_depth);
                if engine_control.snapshots.force_push(snapshot).is_some() {
                    engine_control
                        .telemetry_dropped
                        .fetch_add(1, Ordering::Relaxed);
                }
            }

            EngineOutcome {
                state_digest: state_digest(&engine.core),
                trades: engine.core.metrics().trades,
                inputs_applied: engine.recorder.inputs,
                input_digest: engine.recorder.input_digest(),
                captured_inputs: engine.captured.unwrap_or_default(),
                output_lost,
            }
        })
        .expect("engine thread");

    let journal_path = pipeline_config.journal_path.clone();
    let journal_sync = pipeline_config.journal_sync;
    let output = std::thread::Builder::new()
        .name("output".to_string())
        .spawn(move || {
            let mut writer = match (journal, journal_path) {
                (Some(writer), _) => Some(writer),
                (None, Some(path)) => match JournalWriter::create(&path, run_id) {
                    Ok(writer) => Some(writer),
                    Err(error) => {
                        output_failed.store(true, Ordering::Release);
                        return OutputOutcome {
                            outputs: 0,
                            digest: [0u8; 32],
                            journal_records: 0,
                            journal_error: Some(error),
                        };
                    }
                },
                (None, None) => None,
            };
            let mut recorder = DigestRecorder::new();
            let mut journal_error = None;
            let mut since_flush = 0u64;

            loop {
                let event = match output_rx.try_recv() {
                    Some(event) => event,
                    None => {
                        // The queue drained, so publish the partial group.
                        if since_flush > 0 && matches!(journal_sync, JournalSync::GroupCommit(_)) {
                            if let Some(writer) = writer.as_mut() {
                                if let Err(error) = writer.flush() {
                                    journal_error = Some(error);
                                    break;
                                }
                            }
                            since_flush = 0;
                        }
                        match output_rx.recv() {
                            Some(event) => event,
                            None => break,
                        }
                    }
                };
                recorder.record_output(&event);
                let Some(writer) = writer.as_mut() else {
                    continue;
                };
                if let Err(error) = writer.append(&event) {
                    journal_error = Some(error);
                    break;
                }
                since_flush += 1;
                let result = match journal_sync {
                    JournalSync::Buffered => None,
                    JournalSync::GroupCommit(records) if since_flush >= records.max(1) => {
                        Some(writer.flush())
                    }
                    JournalSync::GroupCommit(_) => None,
                    JournalSync::Durable => Some(writer.sync()),
                };
                match result {
                    Some(Err(error)) => {
                        journal_error = Some(error);
                        break;
                    }
                    Some(Ok(())) => since_flush = 0,
                    None => {}
                }
            }
            if journal_error.is_some() {
                // Tell the engine at once instead of waiting for the queue to fill.
                output_failed.store(true, Ordering::Release);
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

    let (feed_inputs, decode_errors) = feed.join().expect("feed thread panicked");
    let engine_outcome = engine.join().expect("engine thread panicked");
    let output_outcome = output.join().expect("output thread panicked");
    let elapsed_ns = started.elapsed().as_nanos();

    if let Some(error) = output_outcome.journal_error {
        return Err(RuntimeError::Journal(error));
    }
    if engine_outcome.output_lost {
        return Err(RuntimeError::OutputUnavailable {
            inputs_applied: engine_outcome.inputs_applied,
        });
    }

    Ok(RunSummary {
        inputs: engine_outcome.inputs_applied,
        feed_inputs,
        captured_inputs: engine_outcome.captured_inputs,
        outputs: output_outcome.outputs,
        trades: engine_outcome.trades,
        journal_records: output_outcome.journal_records,
        input_digest: engine_outcome.input_digest,
        output_digest: output_outcome.digest,
        state_digest: engine_outcome.state_digest,
        input_queue: report(&input_stats, pipeline_config.input_capacity),
        output_queue: report(&output_stats, pipeline_config.output_capacity),
        telemetry_dropped: control.telemetry_dropped(),
        decode_errors,
        elapsed_ns,
    })
}

/// Owns the trading core and the one canonical input stream. Feed events and
/// control commands are sequenced here, in the order the engine applies them,
/// so a recorded run replays exactly.
struct Engine {
    core: TradingCore,
    recorder: DigestRecorder,
    captured: Option<Vec<EngineInput>>,
    batch: Vec<OutputEvent>,
}

impl Engine {
    fn new(config: EngineConfig, capture_inputs: bool) -> Engine {
        Engine {
            core: TradingCore::new(config),
            recorder: DigestRecorder::new(),
            captured: capture_inputs.then(Vec::new),
            batch: Vec::with_capacity(64),
        }
    }

    /// Applies one input. Control commands carry no arrival time and take the
    /// engine clock. Returns false when the output side is gone.
    fn apply(
        &mut self,
        output_tx: &mut crate::queue::Sender<OutputEvent>,
        event: InputEvent,
        recv_time_ns: Option<u64>,
    ) -> bool {
        let input = EngineInput {
            ingress_seq: IngressSeq(self.recorder.inputs + 1),
            recv_time_ns: recv_time_ns.unwrap_or_else(|| self.core.engine_time_ns()),
            event,
        };
        self.recorder.record_input(&input);
        if let Some(captured) = self.captured.as_mut() {
            captured.push(input);
        }
        self.core.apply(&input, &mut self.batch);
        for event in self.batch.iter() {
            // Execution output is never dropped: stop the engine instead.
            if !output_tx.send(*event) {
                return false;
            }
        }
        true
    }
}
