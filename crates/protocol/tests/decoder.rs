//! Untrusted bytes must never panic the decoder, and any success must be
//! canonical. This runs on stable; the `fuzz/` targets do the same job with
//! coverage guidance.

use proptest::prelude::*;
use protocol::canonical::{decode_output, encode_output};
use protocol::codec::{FileHeader, Frame};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2_000))]

    #[test]
    fn arbitrary_bytes_never_panic_the_frame_decoder(bytes in prop::collection::vec(any::<u8>(), 0..256)) {
        if let Ok((frame, consumed)) = Frame::decode(&bytes) {
            let mut reencoded = Vec::new();
            frame.encode(&mut reencoded);
            prop_assert_eq!(reencoded.as_slice(), &bytes[..consumed]);
        }
    }

    #[test]
    fn arbitrary_bytes_never_panic_the_header_decoder(bytes in prop::collection::vec(any::<u8>(), 0..256)) {
        let _ = FileHeader::decode(&bytes);
    }

    #[test]
    fn arbitrary_bytes_never_panic_the_output_decoder(bytes in prop::collection::vec(any::<u8>(), 0..128)) {
        if let Ok((event, consumed)) = decode_output(&bytes) {
            let mut reencoded = Vec::new();
            encode_output(&event, &mut reencoded);
            prop_assert_eq!(reencoded.as_slice(), &bytes[..consumed]);
        }
    }
}

#[test]
fn every_prefix_of_a_valid_feed_is_handled() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/feeds/seed1-500.feed");
    let bytes = std::fs::read(path).expect("feed fixture");

    for len in 0..bytes.len().min(2_048) {
        let slice = &bytes[..len];
        if let Ok((_, consumed)) = FileHeader::decode(slice) {
            let mut offset = consumed;
            while offset < slice.len() {
                match Frame::decode(&slice[offset..]) {
                    Ok((_, used)) => offset += used,
                    Err(_) => break,
                }
            }
        }
    }
}

#[test]
fn single_bit_flips_are_detected() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/feeds/seed1-500.feed");
    let bytes = std::fs::read(path).expect("feed fixture");
    let (_, header_len) = FileHeader::decode(&bytes).unwrap();
    let (frame, frame_len) = Frame::decode(&bytes[header_len..]).unwrap();

    let original = &bytes[header_len..header_len + frame_len];
    for index in 0..original.len() {
        for bit in 0..8 {
            let mut corrupted = original.to_vec();
            corrupted[index] ^= 1 << bit;
            if let Ok((decoded, _)) = Frame::decode(&corrupted) {
                assert_eq!(decoded, frame, "silent corruption accepted");
            }
        }
    }
}
