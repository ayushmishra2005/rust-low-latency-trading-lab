//! Input shaping shared by the fuzz targets.

use protocol::codec::{MAX_FRAME_LEN, SCHEMA_VERSION};

/// Frame bytes before the payload: length, type, flags, schema, sequence,
/// two timestamps, instrument.
pub const PREFIX_LEN: usize = 36;
pub const CRC_LEN: usize = 4;

/// Wraps fuzzer bytes in a frame that passes the framing checks, so payload
/// parsing is reachable. Only the length, the reserved flags, the schema
/// version and the checksum are normalized; the message type, the header
/// fields and the payload all come from the input. The decoder is not
/// bypassed and none of it is reimplemented here.
pub fn frame_from_body(data: &[u8]) -> Vec<u8> {
    let limit = MAX_FRAME_LEN as usize - PREFIX_LEN - CRC_LEN;
    let body = &data[..data.len().min(limit)];

    let mut frame = vec![0u8; PREFIX_LEN];
    let taken = body.len().min(PREFIX_LEN - 4);
    frame[4..4 + taken].copy_from_slice(&body[..taken]);
    frame[5] = 0;
    frame[6..8].copy_from_slice(&SCHEMA_VERSION.to_le_bytes());
    frame.extend_from_slice(&body[taken..]);

    let frame_len = (frame.len() + CRC_LEN) as u32;
    frame[..4].copy_from_slice(&frame_len.to_le_bytes());
    let crc = crc32fast::hash(&frame);
    frame.extend_from_slice(&crc.to_le_bytes());
    frame
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::codec::{Frame, FrameBody};
    use protocol::{InstrumentId, PriceTicks, QuantityLots, Side};

    #[test]
    fn a_real_frame_body_still_decodes_after_wrapping() {
        let frame = Frame {
            source_seq: 12,
            source_time_ns: 34,
            recv_time_ns: 56,
            instrument: InstrumentId(1),
            body: FrameBody::MarketTrade {
                aggressor: Side::Buy,
                price: PriceTicks(101),
                quantity: QuantityLots(7),
            },
        };
        let mut encoded = Vec::new();
        frame.encode(&mut encoded);

        // Strip the length prefix and the checksum, then rebuild them.
        let body = &encoded[4..encoded.len() - CRC_LEN];
        let wrapped = frame_from_body(body);
        assert_eq!(wrapped, encoded);

        let (decoded, consumed) = Frame::decode(&wrapped).expect("payload parsing is reachable");
        assert_eq!(consumed, wrapped.len());
        assert_eq!(decoded, frame);
    }
}
