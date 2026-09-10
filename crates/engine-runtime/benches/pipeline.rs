//! Custom benchmark entry point for queue and pipeline behaviour.
//!
//! Criterion is not used here because these measurements are multi-thread and
//! externally paced. Run with `cargo bench -p engine-runtime`.

use engine_runtime::harness::{open_loop_pipeline, queue_round_trip};
use engine_runtime::WaitStrategy;
use trading_core::{EngineConfig, Generator, GeneratorConfig};

fn main() {
    let quick = std::env::args().any(|arg| arg == "--test");
    let samples = if quick { 2_000 } else { 200_000 };
    let events = if quick { 5_000 } else { 200_000 };

    println!("== queue round-trip latency ==");
    for wait in [
        WaitStrategy::Adaptive {
            spins: 200,
            yields: 20,
        },
        WaitStrategy::BusySpin,
        WaitStrategy::Sleep,
    ] {
        let stats = queue_round_trip(samples, wait);
        println!("wait={:<10} {}", wait.label(), stats);
    }

    println!();
    println!("== open-loop pipeline order-to-report ==");
    let inputs = Generator::new(GeneratorConfig::new(2_026, events)).generate();
    for rate in [0u64, 100_000, 500_000] {
        let result = open_loop_pipeline(
            EngineConfig::single_instrument(1),
            inputs.clone(),
            4_096,
            WaitStrategy::default(),
            rate,
        );
        let label = if rate == 0 {
            "unpaced".to_string()
        } else {
            format!("{rate}/s")
        };
        println!(
            "offered={:<9} {} | {:.0} msg/s | behind={} | in_hw={} out_hw={}",
            label,
            result.order_to_report,
            result.messages_per_second(),
            result.generator_behind,
            result.input_high_water,
            result.output_high_water
        );
    }
}
