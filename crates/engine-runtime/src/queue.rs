//! Bounded SPSC queues with an explicit wait strategy.
//!
//! Trading requests and execution output are never dropped: a full queue makes
//! the producer wait, and the wait is counted.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossbeam_utils::CachePadded;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitStrategy {
    /// Lowest wakeup latency, one busy core. Only sensible on an isolated host.
    BusySpin,
    /// Spin, then yield, then sleep. The developer default.
    Adaptive { spins: u32, yields: u32 },
    /// Lowest idle CPU, highest wakeup jitter.
    Sleep,
}

impl WaitStrategy {
    pub fn label(self) -> &'static str {
        match self {
            WaitStrategy::BusySpin => "busy_spin",
            WaitStrategy::Adaptive { .. } => "adaptive",
            WaitStrategy::Sleep => "sleep",
        }
    }
}

impl Default for WaitStrategy {
    fn default() -> WaitStrategy {
        WaitStrategy::Adaptive {
            spins: 200,
            yields: 20,
        }
    }
}

/// Tracks how long one thread has been waiting and escalates accordingly.
pub struct Backoff {
    strategy: WaitStrategy,
    attempt: u32,
}

impl Backoff {
    pub fn new(strategy: WaitStrategy) -> Backoff {
        Backoff {
            strategy,
            attempt: 0,
        }
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    pub fn wait(&mut self) {
        self.attempt = self.attempt.saturating_add(1);
        match self.strategy {
            WaitStrategy::BusySpin => std::hint::spin_loop(),
            WaitStrategy::Adaptive { spins, yields } => {
                if self.attempt <= spins {
                    std::hint::spin_loop();
                } else if self.attempt <= spins + yields {
                    std::thread::yield_now();
                } else {
                    std::thread::sleep(Duration::from_micros(50));
                }
            }
            WaitStrategy::Sleep => std::thread::sleep(Duration::from_micros(50)),
        }
    }
}

#[derive(Debug, Default)]
pub struct QueueStats {
    pushed: CachePadded<AtomicU64>,
    popped: CachePadded<AtomicU64>,
    full_events: CachePadded<AtomicU64>,
    high_water: CachePadded<AtomicU64>,
}

impl QueueStats {
    pub fn pushed(&self) -> u64 {
        self.pushed.load(Ordering::Relaxed)
    }

    pub fn popped(&self) -> u64 {
        self.popped.load(Ordering::Relaxed)
    }

    pub fn full_events(&self) -> u64 {
        self.full_events.load(Ordering::Relaxed)
    }

    pub fn high_water(&self) -> u64 {
        self.high_water.load(Ordering::Relaxed)
    }

    pub fn depth(&self) -> u64 {
        self.pushed().saturating_sub(self.popped())
    }
}

pub struct Sender<T> {
    inner: rtrb::Producer<T>,
    stats: Arc<QueueStats>,
    strategy: WaitStrategy,
}

pub struct Receiver<T> {
    inner: rtrb::Consumer<T>,
    stats: Arc<QueueStats>,
    strategy: WaitStrategy,
}

pub fn bounded<T>(capacity: usize, strategy: WaitStrategy) -> (Sender<T>, Receiver<T>) {
    let (producer, consumer) = rtrb::RingBuffer::new(capacity);
    let stats = Arc::new(QueueStats::default());
    (
        Sender {
            inner: producer,
            stats: Arc::clone(&stats),
            strategy,
        },
        Receiver {
            inner: consumer,
            stats,
            strategy,
        },
    )
}

impl<T> Sender<T> {
    pub fn stats(&self) -> Arc<QueueStats> {
        Arc::clone(&self.stats)
    }

    /// Waits for space rather than dropping. Returns `false` if the consumer is gone.
    pub fn send(&mut self, mut value: T) -> bool {
        let mut backoff = Backoff::new(self.strategy);
        let mut counted_full = false;
        loop {
            match self.inner.push(value) {
                Ok(()) => {
                    let pushed = self.stats.pushed.fetch_add(1, Ordering::Relaxed) + 1;
                    // Sampled every 64 pushes from the existing counters so a
                    // successful send does not read the other core's free-slot
                    // count. Depth can lag by up to 63 events.
                    if pushed % 64 == 0 {
                        let depth = pushed.saturating_sub(self.stats.popped());
                        self.stats.high_water.fetch_max(depth, Ordering::Relaxed);
                    }
                    return true;
                }
                Err(rtrb::PushError::Full(returned)) => {
                    if self.inner.is_abandoned() {
                        return false;
                    }
                    if !counted_full {
                        self.stats.full_events.fetch_add(1, Ordering::Relaxed);
                        counted_full = true;
                    }
                    value = returned;
                    backoff.wait();
                }
            }
        }
    }
}

impl<T> Receiver<T> {
    pub fn stats(&self) -> Arc<QueueStats> {
        Arc::clone(&self.stats)
    }

    pub fn try_recv(&mut self) -> Option<T> {
        match self.inner.pop() {
            Ok(value) => {
                self.stats.popped.fetch_add(1, Ordering::Relaxed);
                Some(value)
            }
            Err(_) => None,
        }
    }

    /// Blocks until a value arrives or the producer is gone and the queue is drained.
    pub fn recv(&mut self) -> Option<T> {
        let mut backoff = Backoff::new(self.strategy);
        loop {
            if let Some(value) = self.try_recv() {
                return Some(value);
            }
            if self.inner.is_abandoned() {
                return self.try_recv();
            }
            backoff.wait();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn producer_waits_instead_of_dropping() {
        let (mut sender, mut receiver) = bounded::<u32>(2, WaitStrategy::Sleep);
        let handle = std::thread::spawn(move || {
            for value in 0..1_000 {
                assert!(sender.send(value));
            }
            sender.stats().full_events()
        });

        let mut received = Vec::new();
        while received.len() < 1_000 {
            if let Some(value) = receiver.recv() {
                received.push(value);
            } else {
                break;
            }
        }

        let full_events = handle.join().unwrap();
        assert_eq!(received.len(), 1_000);
        assert_eq!(received, (0..1_000).collect::<Vec<_>>());
        assert!(full_events > 0, "a capacity of two should have filled");
    }

    #[test]
    fn receiver_returns_none_after_the_producer_is_dropped() {
        let (mut sender, mut receiver) = bounded::<u32>(4, WaitStrategy::Sleep);
        sender.send(1);
        drop(sender);
        assert_eq!(receiver.recv(), Some(1));
        assert_eq!(receiver.recv(), None);
    }
}
