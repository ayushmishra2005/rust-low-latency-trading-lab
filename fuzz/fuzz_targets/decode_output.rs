#![no_main]

use libfuzzer_sys::fuzz_target;
use protocol::canonical::{decode_output, encode_output};

fuzz_target!(|data: &[u8]| {
    if let Ok((event, consumed)) = decode_output(data) {
        let mut reencoded = Vec::with_capacity(consumed);
        encode_output(&event, &mut reencoded);
        assert_eq!(reencoded.as_slice(), &data[..consumed]);
    }
});
