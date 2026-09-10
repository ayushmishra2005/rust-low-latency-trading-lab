#![no_main]

use libfuzzer_sys::fuzz_target;
use protocol::codec::Frame;

// Success must imply a fully validated frame that re-encodes to the same bytes.
fuzz_target!(|data: &[u8]| {
    if let Ok((frame, consumed)) = Frame::decode(data) {
        let mut reencoded = Vec::with_capacity(consumed);
        frame.encode(&mut reencoded);
        assert_eq!(reencoded.as_slice(), &data[..consumed]);
    }
});
