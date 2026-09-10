//! Measurement harness.
//!
//! Criterion covers isolated functions. This harness covers what Criterion
//! cannot: an externally paced multi-thread latency distribution. The generator
//! schedules against an independent monotonic timeline so a stall reduces
//! observed throughput instead of quietly reducing offered load.

use std::sync::atomic::{AtomicU64, Ordering};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineBench {
    pub order_to_report: LatencyStats,
    pub inputs: u64,
    pub outputs: u64,
    pub elapsed_ns: u128,
    /// Times the generator could not keep up with the requested schedule.
    pub generator_behind: u64,
    pub input_high_water: u64,
    pub output_high_water: u64,
}

impl PipelineBench {
    pub fn messages_per_second(&self) -> f64 {
        if self.elapsed_ns == 0 {
            return 0.0;
        }
        self.inputs as f64 * 1e9 / self.elapsed_ns as f64
    }
}

/// Measures order-to-report latency at a requested offered load.
///
/// `target_rate_hz` of zero means "as fast as possible".
pub fn open_loop_pipeline(
    engine_config: EngineConfig,
    inputs: Vec<EngineInput>,
    capacity: usize,
    wait: WaitStrategy,
    target_rate_hz: u64,
) -> PipelineBench {
    let (mut input_tx, mut input_rx) = bounded::<(EngineInput, u64)>(capacity, wait);
    let (mut output_tx, mut output_rx) = bounded::<(OutputEvent, u64)>(capacity, wait);
    let input_stats = input_tx.stats();
    let output_stats = output_tx.stats();

    // Engine sequence maps one-to-one onto scheduled inputs in this harness.
    let send_times: Arc<Vec<AtomicU64>> = Arc::new(
        (0..inputs.len() + 1)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>(),
    );
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
    let feed_times = Arc::clone(&send_times);
    let total = inputs.len();
    let feed = std::thread::Builder::new()
        .name("bench-feed".to_string())
        .spawn(move || {
            let interval_ns = 1_000_000_000u64.checked_div(target_rate_hz).unwrap_or(0);
            let mut behind = 0u64;
            for (index, input) in inputs.into_iter().enumerate() {
                if interval_ns > 0 {
                    let scheduled = (index as u64 + 1) * interval_ns;
                    loop {
                        let now = start.elapsed().as_nanos() as u64;
                        if now >= scheduled {
                            if now > scheduled + interval_ns {
                                behind += 1;
                            }
                            break;
                        }
                        let remaining = scheduled - now;
                        if remaining > 50_000 {
                            std::thread::sleep(Duration::from_nanos(remaining - 20_000));
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                }
                let enqueue_ns = start.elapsed().as_nanos() as u64;
                feed_times[index + 1].store(enqueue_ns, Ordering::Relaxed);
                if !input_tx.send((input, enqueue_ns)) {
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
            while let Some((input, enqueue_ns)) = input_rx.recv() {
                core.apply(&input, &mut batch);
                applied += 1;
                for event in &batch {
                    if !output_tx.send((*event, enqueue_ns)) {
                        break;
                    }
                }
            }
            applied
        })
        .expect("bench engine thread");

    let report_times = Arc::clone(&send_times);
    let output = std::thread::Builder::new()
        .name("bench-output".to_string())
        .spawn(move || {
            let mut histogram = histogram();
            let mut outputs = 0u64;
            while let Some((event, enqueue_ns)) = output_rx.recv() {
                outputs += 1;
                if let OutputEvent::Report(report) = event {
                    let index = report.engine_seq.0 as usize;
                    if index < order_inputs.len() && order_inputs[index] {
                        let now = start.elapsed().as_nanos() as u64;
                        let sent = report_times[index].load(Ordering::Relaxed).max(enqueue_ns);
                        histogram.record(now.saturating_sub(sent).max(1)).ok();
                    }
                }
            }
            (histogram, outputs)
        })
        .expect("bench output thread");

    let generator_behind = feed.join().expect("bench feed");
    let applied = engine.join().expect("bench engine");
    let (histogram, outputs) = output.join().expect("bench output");
    let elapsed_ns = start.elapsed().as_nanos();
    let _ = total;

    PipelineBench {
        order_to_report: LatencyStats::from(&histogram),
        inputs: applied,
        outputs,
        elapsed_ns,
        generator_behind,
        input_high_water: input_stats.high_water(),
        output_high_water: output_stats.high_water(),
    }
}
