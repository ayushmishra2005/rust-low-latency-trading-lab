//! Feed generator, deterministic replay driver, and benchmark runner.

mod environment;

use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand, ValueEnum};
use engine_runtime::harness::{
    open_loop_pipeline, queue_round_trip, timer_read_pair_baseline, BenchConfig, LatencyStats,
};
use engine_runtime::{
    ControlHandle, FeedSource, JournalSync, PipelineConfig, RunSummary, WaitStrategy,
};
use protocol::codec::{FileHeader, InstrumentSpec};
use protocol::EngineInput;
use trading_core::generator::to_frame;
use trading_core::replay::digest_hex;
use trading_core::{EngineConfig, Generator, GeneratorConfig, Replay, TradingCore};

use environment::Environment;

#[derive(Parser)]
#[command(
    name = "simulator",
    about = "Deterministic market-data generator, replay driver, and benchmark runner"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Write a deterministic binary feed file.
    Generate {
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value_t = 10_000)]
        events: usize,
        #[arg(long)]
        out: PathBuf,
        #[arg(long)]
        inject_gaps: bool,
        #[arg(long)]
        inject_duplicates: bool,
    },
    /// Replay a feed through the threaded pipeline and print the digests.
    Replay {
        #[arg(long)]
        input: Option<PathBuf>,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value_t = 10_000)]
        events: usize,
        #[arg(long)]
        journal: Option<PathBuf>,
        #[arg(long, default_value_t = 0)]
        checkpoint_interval: u64,
        #[arg(long, default_value_t = 8_192)]
        capacity: usize,
        #[arg(long, value_enum, default_value_t = Wait::Adaptive)]
        wait: Wait,
        /// Journal write mode: buffered, group commit size, or durable.
        #[arg(long, value_enum, default_value_t = Journal::GroupCommit)]
        journal_sync: Journal,
        #[arg(long, default_value_t = 256)]
        group_commit_records: u64,
    },
    /// Replay in a single thread, optionally stopping at an engine sequence.
    Step {
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value_t = 10_000)]
        events: usize,
        /// Stop after this engine sequence and print the state digest.
        #[arg(long)]
        until: Option<u64>,
        #[arg(long, default_value_t = 1_000)]
        checkpoint_interval: u64,
        /// Reproduce the recorded inter-arrival gaps.
        #[arg(long)]
        paced: bool,
    },
    /// Replay the same input twice and compare every digest.
    Verify {
        #[arg(long)]
        input: Option<PathBuf>,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value_t = 20_000)]
        events: usize,
    },
    /// Measure queue and pipeline latency at a stated offered load.
    Bench {
        #[arg(long, default_value_t = 2_026)]
        seed: u64,
        #[arg(long, default_value_t = 200_000)]
        events: usize,
        /// Offered load in messages per second. Zero means unpaced.
        #[arg(long, default_values_t = [0u64, 100_000, 500_000])]
        rate: Vec<u64>,
        #[arg(long, default_value_t = 4_096)]
        capacity: usize,
        #[arg(long, value_enum, default_value_t = Wait::Adaptive)]
        wait: Wait,
        #[arg(long, default_value_t = 100_000)]
        queue_samples: usize,
        /// Measured runs per offered rate. One warm-up run is never published.
        #[arg(long, default_value_t = 5)]
        runs: usize,
        /// Events for the runtime-with-journal section. Durable acknowledgement
        /// syncs every record, so this stays smaller than the pipeline workload.
        #[arg(long, default_value_t = 5_000)]
        journal_events: usize,
    },
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
enum Journal {
    Buffered,
    GroupCommit,
    Durable,
}

impl Journal {
    fn resolve(self, records: u64) -> JournalSync {
        match self {
            Journal::Buffered => JournalSync::Buffered,
            Journal::GroupCommit => JournalSync::GroupCommit(records),
            Journal::Durable => JournalSync::Durable,
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
enum Wait {
    Adaptive,
    BusySpin,
    Sleep,
}

impl From<Wait> for WaitStrategy {
    fn from(value: Wait) -> WaitStrategy {
        match value {
            Wait::Adaptive => WaitStrategy::default(),
            Wait::BusySpin => WaitStrategy::BusySpin,
            Wait::Sleep => WaitStrategy::Sleep,
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    match Cli::parse().command {
        Command::Generate {
            seed,
            events,
            out,
            inject_gaps,
            inject_duplicates,
        } => generate(seed, events, &out, inject_gaps, inject_duplicates),
        Command::Replay {
            input,
            seed,
            events,
            journal,
            checkpoint_interval,
            capacity,
            wait,
            journal_sync,
            group_commit_records,
        } => replay(
            input,
            seed,
            events,
            journal,
            checkpoint_interval,
            capacity,
            wait.into(),
            journal_sync.resolve(group_commit_records),
        ),
        Command::Step {
            seed,
            events,
            until,
            checkpoint_interval,
            paced,
        } => step(seed, events, until, checkpoint_interval, paced),
        Command::Verify {
            input,
            seed,
            events,
        } => verify(input, seed, events),
        Command::Bench {
            seed,
            events,
            rate,
            capacity,
            wait,
            queue_samples,
            runs,
            journal_events,
        } => bench(
            seed,
            events,
            &rate,
            capacity,
            wait.into(),
            queue_samples,
            runs,
            journal_events,
        ),
    }
}

fn workload(seed: u64, events: usize, gaps: bool, duplicates: bool) -> Vec<EngineInput> {
    let mut config = GeneratorConfig::new(seed, events);
    config.inject_gaps = gaps;
    config.inject_duplicates = duplicates;
    Generator::new(config).generate()
}

fn generate(
    seed: u64,
    events: usize,
    out: &PathBuf,
    inject_gaps: bool,
    inject_duplicates: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let inputs = workload(seed, events, inject_gaps, inject_duplicates);
    let mut bytes = Vec::with_capacity(inputs.len() * 64);
    FileHeader {
        run_id: u128::from(seed),
        seed,
        start_wall_time_ns: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|value| value.as_nanos() as u64)
            .unwrap_or(0),
        instruments: vec![InstrumentSpec::new(1, "LAB-USD", 1, 1)],
    }
    .encode(&mut bytes);

    let mut frames = 0u64;
    for input in &inputs {
        if let Some(frame) = to_frame(input) {
            frame.encode(&mut bytes);
            frames += 1;
        }
    }
    std::fs::write(out, &bytes)?;
    println!(
        "wrote {} frames ({} bytes) to {}",
        frames,
        bytes.len(),
        out.display()
    );
    Ok(())
}

/// A recorded feed owns its run identity; only generated workloads use the seed.
fn load_source(
    input: Option<PathBuf>,
    seed: u64,
    events: usize,
) -> Result<(FeedSource, u128), Box<dyn std::error::Error>> {
    match input {
        Some(path) => {
            let bytes = std::fs::read(path)?;
            let (header, _) = FileHeader::decode(&bytes)?;
            Ok((FeedSource::Bytes(bytes), header.run_id))
        }
        None => Ok((
            FeedSource::Memory(workload(seed, events, false, false)),
            u128::from(seed),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn replay(
    input: Option<PathBuf>,
    seed: u64,
    events: usize,
    journal: Option<PathBuf>,
    checkpoint_interval: u64,
    capacity: usize,
    wait: WaitStrategy,
    journal_sync: JournalSync,
) -> Result<(), Box<dyn std::error::Error>> {
    let (source, run_id) = load_source(input, seed, events)?;
    let summary = engine_runtime::run(
        EngineConfig::single_instrument(run_id),
        PipelineConfig {
            input_capacity: capacity,
            output_capacity: capacity,
            wait,
            journal_path: journal,
            journal_sync,
            checkpoint_interval,
            snapshot_interval: 0,
            ..PipelineConfig::default()
        },
        source,
        ControlHandle::default(),
    )?;
    print_summary(&summary, wait);
    Ok(())
}

fn print_summary(summary: &RunSummary, wait: WaitStrategy) {
    let seconds = summary.elapsed_ns as f64 / 1e9;
    println!("replay summary:");
    println!("  inputs           {}", summary.inputs);
    println!("  outputs          {}", summary.outputs);
    println!("  trades           {}", summary.trades);
    println!("  decode errors    {}", summary.decode_errors);
    println!("  journal records  {}", summary.journal_records);
    println!("  wall time        {seconds:.3}s");
    if seconds > 0.0 {
        println!(
            "  input rate       {:.0} msg/s",
            summary.inputs as f64 / seconds
        );
    }
    println!(
        "  input queue      cap={} high_water={} full_events={}",
        summary.input_queue.capacity,
        summary.input_queue.high_water,
        summary.input_queue.full_events
    );
    println!(
        "  output queue     cap={} high_water={} full_events={}",
        summary.output_queue.capacity,
        summary.output_queue.high_water,
        summary.output_queue.full_events
    );
    println!("  wait strategy    {}", wait.label());
    println!("  input digest     {}", digest_hex(&summary.input_digest));
    println!("  output digest    {}", digest_hex(&summary.output_digest));
    println!("  state digest     {}", digest_hex(&summary.state_digest));
}

fn step(
    seed: u64,
    events: usize,
    until: Option<u64>,
    checkpoint_interval: u64,
    paced: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let inputs = workload(seed, events, false, false);
    let mut replay = Replay::new(
        TradingCore::new(EngineConfig::single_instrument(u128::from(seed))),
        checkpoint_interval,
    );

    let mut previous_recv_ns = inputs.first().map(|input| input.recv_time_ns).unwrap_or(0);
    let started = Instant::now();
    for input in &inputs {
        if paced {
            let delta = input.recv_time_ns.saturating_sub(previous_recv_ns);
            previous_recv_ns = input.recv_time_ns;
            if delta > 0 {
                std::thread::sleep(Duration::from_nanos(delta));
            }
        }
        replay.step(input);
        if let Some(until) = until {
            if replay.core().engine_seq().0 >= until {
                break;
            }
        }
    }

    let result = replay.finish();
    println!("step replay:");
    println!("  engine seq       {}", replay.core().engine_seq());
    println!("  inputs applied   {}", result.inputs);
    println!("  outputs          {}", result.outputs);
    println!("  live orders      {}", replay.core().live_order_count());
    println!("  wall time        {:.3}s", started.elapsed().as_secs_f64());
    println!("  input digest     {}", digest_hex(&result.input_digest));
    println!("  output digest    {}", digest_hex(&result.output_digest));
    println!("  state digest     {}", digest_hex(&result.state_digest));
    for checkpoint in result.checkpoints.iter().rev().take(3).rev() {
        println!(
            "  checkpoint {:<8} {}",
            checkpoint.engine_seq,
            digest_hex(&checkpoint.state)
        );
    }
    Ok(())
}

fn verify(
    input: Option<PathBuf>,
    seed: u64,
    events: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut results = Vec::new();
    for _ in 0..2 {
        let (source, run_id) = load_source(input.clone(), seed, events)?;
        results.push(engine_runtime::run(
            EngineConfig::single_instrument(run_id),
            PipelineConfig::default(),
            source,
            ControlHandle::default(),
        )?);
    }

    let matches = results[0].input_digest == results[1].input_digest
        && results[0].output_digest == results[1].output_digest
        && results[0].state_digest == results[1].state_digest;

    println!("input digest   {}", digest_hex(&results[0].input_digest));
    println!("output digest  {}", digest_hex(&results[0].output_digest));
    println!("state digest   {}", digest_hex(&results[0].state_digest));
    if matches {
        println!("two runs produced identical digests");
        Ok(())
    } else {
        Err("replay digests differ between runs".into())
    }
}

#[allow(clippy::too_many_arguments)]
fn bench(
    seed: u64,
    events: usize,
    rates: &[u64],
    capacity: usize,
    wait: WaitStrategy,
    queue_samples: usize,
    runs: usize,
    journal_events: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    Environment::capture().print();
    println!();
    println!("workload: seeded generator seed={seed} events={events}");
    println!("queue: rtrb SPSC capacity={capacity} wait={}", wait.label());
    println!();

    println!("timer read-pair baseline (1000000 samples):");
    println!("  {}", timer_read_pair_baseline(1_000_000));
    println!("  not subtracted from the latency numbers below");
    println!();

    println!("queue round-trip latency ({queue_samples} samples):");
    for strategy in [
        WaitStrategy::default(),
        WaitStrategy::BusySpin,
        WaitStrategy::Sleep,
    ] {
        println!(
            "  {:<10} {}",
            strategy.label(),
            queue_round_trip(queue_samples, strategy)
        );
    }
    println!();

    let inputs = workload(seed, events, false, false);
    println!("engine pipeline: feed -> queue -> engine -> report");
    println!("no journal, no snapshots, no control API");
    println!("runs={runs} measured per offered rate, after one unpublished warm-up run");
    for rate in rates {
        let label = if *rate == 0 {
            "unpaced".to_string()
        } else {
            format!("{rate}/s")
        };
        let config = BenchConfig::new(capacity, wait, *rate);
        // The warm-up result is measured but never published.
        open_loop_pipeline(
            EngineConfig::single_instrument(u128::from(seed)),
            inputs.clone(),
            config,
        );

        let mut results = Vec::with_capacity(runs);
        for run in 1..=runs {
            let result = open_loop_pipeline(
                EngineConfig::single_instrument(u128::from(seed)),
                inputs.clone(),
                config,
            );
            println!("  offered={label} run={run}");
            println!(
                "    scheduled-arrival-to-report {}",
                result.arrival_to_report
            );
            println!(
                "    enqueue-to-report           {}",
                result.enqueue_to_report
            );
            println!(
                "    throughput={:.0} msg/s outputs={} generator_behind={} input_high_water={} output_high_water={}",
                result.messages_per_second(),
                result.outputs,
                result.generator_behind,
                result.input_high_water,
                result.output_high_water
            );
            results.push(result);
        }
        println!("  offered={label} aggregate over {runs} runs");
        aggregate(
            "scheduled-arrival-to-report",
            &results
                .iter()
                .map(|r| r.arrival_to_report)
                .collect::<Vec<_>>(),
        );
        aggregate(
            "enqueue-to-report",
            &results
                .iter()
                .map(|r| r.enqueue_to_report)
                .collect::<Vec<_>>(),
        );
        let mut throughput: Vec<u64> = results
            .iter()
            .map(|r| r.messages_per_second() as u64)
            .collect();
        let mut behind: Vec<u64> = results.iter().map(|r| r.generator_behind).collect();
        println!(
            "    throughput median={} msg/s min={} max={}",
            median(&mut throughput),
            throughput.iter().min().copied().unwrap_or(0),
            throughput.iter().max().copied().unwrap_or(0)
        );
        println!(
            "    generator_behind median={} max={} samples={}",
            median(&mut behind),
            behind.iter().max().copied().unwrap_or(0),
            results[0].arrival_to_report.count
        );
    }

    println!();
    println!("runtime with journal: the shipped run() path");
    println!("canonical input recording, journal enabled, snapshots every 1000");
    println!("workload: {journal_events} events");
    println!("boundary: whole-run throughput. This path carries no per-event");
    println!("wall-clock stamp, so no latency percentile is reported for it.");
    let journal_inputs = workload(seed, journal_events, false, false);
    let journal_dir = std::env::temp_dir().join("rltl-bench-journal");
    std::fs::create_dir_all(&journal_dir)?;
    for (name, sync) in [
        ("buffered", JournalSync::Buffered),
        ("group-commit-64", JournalSync::GroupCommit(64)),
        ("durable-ack", JournalSync::Durable),
    ] {
        let path = journal_dir.join(format!("{name}.journal"));
        let mut throughput = Vec::with_capacity(runs);
        for run in 1..=runs {
            let summary = engine_runtime::run(
                EngineConfig::single_instrument(u128::from(seed)),
                PipelineConfig {
                    input_capacity: capacity,
                    output_capacity: capacity,
                    wait,
                    journal_path: Some(path.clone()),
                    journal_sync: sync,
                    snapshot_interval: 1_000,
                    snapshot_depth: 16,
                    ..PipelineConfig::default()
                },
                FeedSource::Memory(journal_inputs.clone()),
                ControlHandle::default(),
            )?;
            let rate = summary.inputs as f64 * 1e9 / summary.elapsed_ns as f64;
            println!(
                "  journal={name} run={run} throughput={rate:.0} msg/s inputs={} outputs={} journal_records={}",
                summary.inputs, summary.outputs, summary.journal_records
            );
            throughput.push(rate as u64);
        }
        println!(
            "  journal={name} aggregate throughput median={} msg/s min={} max={}",
            median(&mut throughput),
            throughput.iter().min().copied().unwrap_or(0),
            throughput.iter().max().copied().unwrap_or(0)
        );
        std::fs::remove_file(&path).ok();
    }
    Ok(())
}

fn median(values: &mut [u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    values[values.len() / 2]
}

/// Prints the median across runs, with the spread across runs beside it.
fn aggregate(name: &str, runs: &[LatencyStats]) {
    let spread = |mut values: Vec<u64>| {
        let min = values.iter().min().copied().unwrap_or(0);
        let max = values.iter().max().copied().unwrap_or(0);
        format!("median={}ns min={min}ns max={max}ns", median(&mut values))
    };
    println!(
        "    {name} p50 {}",
        spread(runs.iter().map(|r| r.p50_ns).collect())
    );
    println!(
        "    {name} p99 {}",
        spread(runs.iter().map(|r| r.p99_ns).collect())
    );
    println!(
        "    {name} p99.9 {}",
        spread(runs.iter().map(|r| r.p999_ns).collect())
    );
    println!(
        "    {name} max {}",
        spread(runs.iter().map(|r| r.max_ns).collect())
    );
}
