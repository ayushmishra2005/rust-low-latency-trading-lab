#![no_main]

use libfuzzer_sys::fuzz_target;
use protocol::codec::FileHeader;

fuzz_target!(|data: &[u8]| {
    if let Ok((header, consumed)) = FileHeader::decode(data) {
        let mut reencoded = Vec::with_capacity(consumed);
        header.encode(&mut reencoded);
        assert_eq!(reencoded.as_slice(), &data[..consumed]);
    }
});
