//! Runtime around the pure trading core: dedicated threads, bounded queues, the
//! durable journal, and cold control/telemetry surfaces.

pub mod harness;
pub mod journal;
pub mod pipeline;
pub mod queue;
pub mod snapshot;

pub use harness::{open_loop_pipeline, queue_round_trip, LatencyStats, PipelineBench};
pub use journal::{read_journal, JournalRecovery, JournalTail, JournalWriter};
pub use pipeline::{
    run, ControlHandle, FeedSource, PipelineConfig, QueueReport, RunSummary, RuntimeError,
};
pub use queue::{bounded, Backoff, QueueStats, WaitStrategy};
pub use snapshot::{capture, EngineSnapshot};
