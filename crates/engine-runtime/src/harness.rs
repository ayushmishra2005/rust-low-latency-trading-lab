//! Measurement harness.
//!
//! Criterion covers isolated functions. This harness covers what Criterion
//! cannot: an externally paced multi-thread latency distribution. The generator
//! schedules against an independent monotonic timeline so a stall reduces
//! observed throughput instead of quietly reducing offered load.

use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use protocol::{EngineInput, InputEvent, OutputEvent};
use trading_core::{EngineConfig, TradingCore};

use crate::queue::{bounded, WaitStrategy};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LatencyStats {
    pub count: u64,
    pub min_ns: u64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
}

impl LatencyStats {
    pub fn from(histogram: &Histogram<u64>) -> LatencyStats {
        LatencyStats {
            count: histogram.len(),
            min_ns: histogram.min(),
            p50_ns: histogram.value_at_quantile(0.50),
            p95_ns: histogram.value_at_quantile(0.95),
            p99_ns: histogram.value_at_quantile(0.99),
            p999_ns: histogram.value_at_quantile(0.999),
            max_ns: histogram.max(),
        }
    }
}

impl std::fmt::Display for LatencyStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "n={} min={}ns p50={}ns p95={}ns p99={}ns p99.9={}ns max={}ns",
            self.count,
            self.min_ns,
            self.p50_ns,
            self.p95_ns,
            self.p99_ns,
            self.p999_ns,
            self.max_ns
        )
    }
}

fn histogram() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).expect("valid histogram bounds")
}

/// Round-trip latency between two dedicated threads over two SPSC queues.
///
/// A ping-pong measures queue coordination itself. An unpaced one-way stream
/// measures queue residence under saturation instead, which is a different
/// question and is covered by the pipeline benchmark.
pub fn queue_round_trip(samples: usize, wait: WaitStrategy) -> LatencyStats {
    let (mut request_tx, mut request_rx) = bounded::<u64>(1, wait);
    let (mut response_tx, mut response_rx) = bounded::<u64>(1, wait);
    let start = Instant::now();

    let echo = std::thread::spawn(move || {
        while let Some(value) = request_rx.recv() {
            if !response_tx.send(value) {
                break;
            }
        }
    });

    let mut histogram = histogram();
    for _ in 0..samples {
        let sent = start.elapsed().as_nanos() as u64;
        if !request_tx.send(sent) {
            break;
        }
        if response_rx.recv().is_none() {
            break;
        }
        let now = start.elapsed().as_nanos() as u64;
        histogram.record(now.saturating_sub(sent).max(1)).ok();
    }
    drop(request_tx);
    echo.join().expect("echo thread");

    LatencyStats::from(&histogram)
}

/// Cost of two `Instant::elapsed` reads, the pair the harness uses as a stamp.
/// Reported separately and never subtracted from pipeline latency.
pub fn timer_read_pair_baseline(samples: usize) -> LatencyStats {
    let origin = Instant::now();
    for _ in 0..1_000 {
        let _ = origin.elapsed();
    }
    let mut histogram = histogram();
    for _ in 0..samples {
        let first = origin.elapsed().as_nanos() as u64;
        let second = origin.elapsed().as_nanos() as u64;
        histogram.record(second.saturating_sub(first).max(1)).ok();
    }
    LatencyStats::from(&histogram)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timer_read_pair_baseline_records_samples() {
        let stats = timer_read_pair_baseline(2_000);
        assert_eq!(stats.count, 2_000);
        assert!(stats.p50_ns >= 1);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineBench {
    /// Scheduled arrival to report. Includes time the producer spent behind
    /// its own schedule, so a slow producer shows up here.
    pub arrival_to_report: LatencyStats,
    /// Actual enqueue to report. Excludes producer scheduling debt.
    pub enqueue_to_report: LatencyStats,
    pub inputs: u64,
    pub outputs: u64,
    pub elapsed_ns: u128,
    pub offered_rate_hz: u64,
    /// Times the generator could not keep up with the requested schedule.
    pub generator_behind: u64,
    pub input_high_water: u64,
    pub output_high_water: u64,
}

/// One benchmark run. `producer_delay_ns` is a deliberate producer stall used
/// to prove the two latency boundaries measure different things.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BenchConfig {
    pub capacity: usize,
    pub wait: WaitStrategy,
    pub target_rate_hz: u64,
    pub producer_delay_ns: u64,
}

impl BenchConfig {
    pub fn new(capacity: usize, wait: WaitStrategy, target_rate_hz: u64) -> BenchConfig {
        BenchConfig {
            capacity,
            wait,
            target_rate_hz,
            producer_delay_ns: 0,
        }
    }
}

/// Arrival schedule and actual enqueue instant of one input.
#[derive(Debug, Clone, Copy)]
struct Stamp {
    scheduled_ns: u64,
    enqueue_ns: u64,
}

impl PipelineBench {
    pub fn messages_per_second(&self) -> f64 {
        if self.elapsed_ns == 0 {
            return 0.0;
        }
        self.inputs as f64 * 1e9 / self.elapsed_ns as f64
    }
}

/// Measures the lightweight engine pipeline: feed thread, bounded queue,
/// engine, output thread. No journal, no snapshots, no control API.
///
/// Two boundaries are reported. Scheduled-arrival-to-report starts at the
/// instant the input was due; enqueue-to-report starts when the producer
/// actually handed it over. `target_rate_hz` of zero means "as fast as possible".
pub fn open_loop_pipeline(
    engine_config: EngineConfig,
    inputs: Vec<EngineInput>,
    config: BenchConfig,
) -> PipelineBench {
    let capacity = config.capacity;
    let wait = config.wait;
    let (mut input_tx, mut input_rx) = bounded::<(EngineInput, Stamp)>(capacity, wait);
    let (mut output_tx, mut output_rx) = bounded::<(OutputEvent, Stamp)>(capacity, wait);
    let input_stats = input_tx.stats();
    let output_stats = output_tx.stats();

    // Engine sequence maps one-to-one onto scheduled inputs in this harness.
    let order_inputs: Arc<Vec<bool>> = Arc::new(
        std::iter::once(false)
            .chain(
                inputs
                    .iter()
                    .map(|input| matches!(input.event, InputEvent::Order(_))),
            )
            .collect(),
    );

    let start = Instant::now();
    let target_rate_hz = config.target_rate_hz;
    let producer_delay_ns = config.producer_delay_ns;
    let feed = std::thread::Builder::new()
        .name("bench-feed".to_string())
        .spawn(move || {
            let interval_ns = 1_000_000_000u64.checked_div(target_rate_hz).unwrap_or(0);
            let mut behind = 0u64;
            for (index, input) in inputs.into_iter().enumerate() {
                let mut scheduled_ns = start.elapsed().as_nanos() as u64;
                if interval_ns > 0 {
                    scheduled_ns = (index as u64 + 1) * interval_ns;
                    loop {
                        let now = start.elapsed().as_nanos() as u64;
                        if now >= scheduled_ns {
                            if now > scheduled_ns + interval_ns {
                                behind += 1;
                            }
                            break;
                        }
                        let remaining = scheduled_ns - now;
                        if remaining > 50_000 {
                            std::thread::sleep(Duration::from_nanos(remaining - 20_000));
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                }
                if producer_delay_ns > 0 {
                    std::thread::sleep(Duration::from_nanos(producer_delay_ns));
                }
                let enqueue_ns = start.elapsed().as_nanos() as u64;
                let stamp = Stamp {
                    scheduled_ns,
                    enqueue_ns,
                };
                if !input_tx.send((input, stamp)) {
                    break;
                }
            }
            behind
        })
        .expect("bench feed thread");

    let engine = std::thread::Builder::new()
        .name("bench-engine".to_string())
        .spawn(move || {
            let mut core = TradingCore::new(engine_config);
            let mut batch = Vec::with_capacity(64);
            let mut applied = 0u64;
            while let Some((input, stamp)) = input_rx.recv() {
                core.apply(&input, &mut batch);
                applied += 1;
                for event in &batch {
                    if !output_tx.send((*event, stamp)) {
                        break;
                    }
                }
            }
            applied
        })
        .expect("bench engine thread");

    let output = std::thread::Builder::new()
        .name("bench-output".to_string())
        .spawn(move || {
            let mut arrival = histogram();
            let mut enqueue = histogram();
            let mut outputs = 0u64;
            while let Some((event, stamp)) = output_rx.recv() {
                outputs += 1;
                if let OutputEvent::Report(report) = event {
                    let index = report.engine_seq.0 as usize;
                    if index < order_inputs.len() && order_inputs[index] {
                        let now = start.elapsed().as_nanos() as u64;
                        arrival
                            .record(now.saturating_sub(stamp.scheduled_ns).max(1))
                            .ok();
                        enqueue
                            .record(now.saturating_sub(stamp.enqueue_ns).max(1))
                            .ok();
                    }
                }
            }
            (arrival, enqueue, outputs)
        })
        .expect("bench output thread");

    let generator_behind = feed.join().expect("bench feed");
    let applied = engine.join().expect("bench engine");
    let (arrival, enqueue, outputs) = output.join().expect("bench output");
    let elapsed_ns = start.elapsed().as_nanos();

    PipelineBench {
        arrival_to_report: LatencyStats::from(&arrival),
        enqueue_to_report: LatencyStats::from(&enqueue),
        inputs: applied,
        outputs,
        elapsed_ns,
        offered_rate_hz: target_rate_hz,
        generator_behind,
        input_high_water: input_stats.high_water(),
        output_high_water: output_stats.high_water(),
    }
}
