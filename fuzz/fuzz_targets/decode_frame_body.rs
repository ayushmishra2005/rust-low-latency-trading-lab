#![no_main]

use libfuzzer_sys::fuzz_target;
use protocol::codec::Frame;
use protocol_fuzz::frame_from_body;

// A random mutation almost never produces a correct checksum, so the raw frame
// target rarely reaches payload parsing. This one normalizes the framing and
// lets the fuzzer explore valid-checksum payloads. The raw target still covers
// hostile framing.
fuzz_target!(|data: &[u8]| {
    let frame = frame_from_body(data);
    if let Ok((decoded, consumed)) = Frame::decode(&frame) {
        let mut reencoded = Vec::with_capacity(consumed);
        decoded.encode(&mut reencoded);
        assert_eq!(reencoded.as_slice(), &frame[..consumed]);
    }
});
