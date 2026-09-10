//! Runs the trading engine on dedicated threads and serves the cold control
//! socket from a separate Tokio runtime.

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use engine_runtime::{ControlHandle, FeedSource, PipelineConfig};
use trading_core::{EngineConfig, Generator, GeneratorConfig};

use control_gateway::server;
use control_gateway::state::Gateway;

#[derive(Parser)]
#[command(
    name = "control-gateway",
    about = "Cold control boundary for the trading engine"
)]
struct Cli {
    /// Local socket path. Loopback-only by design.
    #[arg(long, default_value = "/tmp/rltl-control.sock")]
    socket: PathBuf,
    #[arg(long, default_value = "/tmp/rltl-engine.journal")]
    journal: PathBuf,
    /// Shared secret required for every mutation. Mutations are refused without it.
    #[arg(long)]
    token: Option<String>,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    #[arg(long, default_value_t = 200_000)]
    events: usize,
    /// Slow the feed down so the demo engine keeps running while you query it.
    #[arg(long, default_value_t = 2_000)]
    events_per_second: u64,
    #[arg(long, default_value_t = 200)]
    snapshot_interval: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let token = cli
        .token
        .clone()
        .or_else(|| std::env::var("RLTL_CONTROL_TOKEN").ok());
    let control = ControlHandle::new(256, 4);
    let journal = cli.journal.clone();

    let engine_control = control.clone();
    let engine_journal = journal.clone();
    let inputs = paced(
        Generator::new(GeneratorConfig::new(cli.seed, cli.events)).generate(),
        cli.events_per_second,
    );

    let engine = std::thread::Builder::new()
        .name("pipeline".to_string())
        .spawn(move || {
            let summary = engine_runtime::run(
                EngineConfig::single_instrument(u128::from(cli.seed)),
                PipelineConfig {
                    journal_path: Some(engine_journal),
                    snapshot_interval: cli.snapshot_interval,
                    snapshot_depth: 16,
                    paced: true,
                    ..PipelineConfig::default()
                },
                FeedSource::Memory(inputs),
                engine_control,
            );
            match summary {
                Ok(summary) => println!(
                    "engine finished: {} inputs, {} outputs, {} trades",
                    summary.inputs, summary.outputs, summary.trades
                ),
                Err(error) => eprintln!("engine failed: {error}"),
            }
        })?;

    // Give the output thread time to create the journal before tailing it.
    while !journal.exists() {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    if token.is_none() {
        eprintln!("warning: no control token configured; mutating methods will be refused");
    }
    let gateway = Arc::new(Gateway::new(control, journal, token));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let result = runtime.block_on(server::serve(&cli.socket, gateway));

    let _ = engine.join();
    result?;
    Ok(())
}

/// Spreads recorded logical time so the demo feed lasts long enough to inspect.
fn paced(
    mut inputs: Vec<protocol::EngineInput>,
    events_per_second: u64,
) -> Vec<protocol::EngineInput> {
    if events_per_second == 0 {
        return inputs;
    }
    let step = 1_000_000_000 / events_per_second.max(1);
    for (index, input) in inputs.iter_mut().enumerate() {
        input.recv_time_ns = 1_000_000 + step * index as u64;
    }
    inputs
}
