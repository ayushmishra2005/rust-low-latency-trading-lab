//! Golden tests: committed binary feeds must always produce the recorded
//! digests. A change here means engine behaviour changed.

use std::path::{Path, PathBuf};

use engine_runtime::{ControlHandle, FeedSource, PipelineConfig};
use protocol::codec::FileHeader;
use trading_core::replay::digest_hex;
use trading_core::EngineConfig;

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures")
        .canonicalize()
        .expect("fixtures directory")
}

fn expected(name: &str) -> Vec<(String, String)> {
    std::fs::read_to_string(fixtures().join("expected").join(name))
        .expect("expected digest file")
        .lines()
        .filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
        .map(|line| {
            let (key, value) = line.split_once('=').expect("key=value line");
            (key.trim().to_string(), value.trim().to_string())
        })
        .collect()
}

fn check(feed: &str, expectations: &str) {
    let bytes = std::fs::read(fixtures().join("feeds").join(feed)).expect("feed fixture");
    // The recorded feed owns the run identity used by the state digest.
    let (header, _) = FileHeader::decode(&bytes).expect("feed header");
    let summary = engine_runtime::run(
        EngineConfig::single_instrument(header.run_id),
        PipelineConfig::default(),
        FeedSource::Bytes(bytes),
        ControlHandle::default(),
    )
    .expect("replay");

    let actual = [
        ("inputs".to_string(), summary.inputs.to_string()),
        ("outputs".to_string(), summary.outputs.to_string()),
        ("trades".to_string(), summary.trades.to_string()),
        (
            "input_digest".to_string(),
            digest_hex(&summary.input_digest),
        ),
        (
            "output_digest".to_string(),
            digest_hex(&summary.output_digest),
        ),
        (
            "state_digest".to_string(),
            digest_hex(&summary.state_digest),
        ),
    ];

    assert_eq!(summary.decode_errors, 0);
    for (key, value) in expected(expectations) {
        let (_, found) = actual
            .iter()
            .find(|(name, _)| *name == key)
            .unwrap_or_else(|| panic!("unknown golden key {key}"));
        assert_eq!(found, &value, "golden mismatch for {key}");
    }
}

#[test]
fn seed1_feed_matches_the_recorded_digests() {
    check("seed1-500.feed", "seed1-500.txt");
}

#[test]
fn feed_with_gaps_and_duplicates_matches_the_recorded_digests() {
    check("seed2-500-faults.feed", "seed2-500-faults.txt");
}
