//! Manually specified little-endian binary feed format, version 1.
//!
//! Layout is a file header followed by length-delimited frames. Nothing here
//! depends on the in-memory Rust representation, so the wire contract is stable.

use crate::enums::{OrderType, RequestKind, Side};
use crate::error::{DecodeError, DecodeErrorKind};
use crate::events::OrderRequest;
use crate::ids::{AccountId, ClientOrderId, InstrumentId, PriceTicks, QuantityLots, RequestId};
use crate::read::Reader;

pub const MAGIC: [u8; 4] = *b"RLTL";
pub const FORMAT_MAJOR: u16 = 1;
pub const FORMAT_MINOR: u16 = 0;
/// Reading this as little-endian proves the writer used little-endian.
pub const ENDIAN_MARKER: u16 = 0x1234;
pub const SCHEMA_VERSION: u16 = 1;

pub const MAX_FRAME_LEN: u32 = 4096;
pub const MAX_INSTRUMENTS: u16 = 256;

const FRAME_PREFIX_LEN: usize = 36;
const FRAME_CRC_LEN: usize = 4;
const INSTRUMENT_LEN: usize = 30;

const TYPE_SNAPSHOT_BEGIN: u8 = 1;
const TYPE_SNAPSHOT_LEVEL: u8 = 2;
const TYPE_SNAPSHOT_END: u8 = 3;
const TYPE_LEVEL_SET: u8 = 4;
const TYPE_MARKET_TRADE: u8 = 5;
const TYPE_HEARTBEAT: u8 = 6;
const TYPE_FEED_RESET: u8 = 7;
const TYPE_ORDER_REQUEST: u8 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstrumentSpec {
    pub id: InstrumentId,
    /// ASCII, right padded with zero bytes.
    pub symbol: [u8; 8],
    pub tick_size: i64,
    pub lot_size: u64,
    pub price_scale: u8,
    pub quantity_scale: u8,
}

impl InstrumentSpec {
    pub fn new(id: u32, symbol: &str, tick_size: i64, lot_size: u64) -> InstrumentSpec {
        let mut padded = [0u8; 8];
        let bytes = symbol.as_bytes();
        let len = bytes.len().min(8);
        padded[..len].copy_from_slice(&bytes[..len]);
        InstrumentSpec {
            id: InstrumentId(id),
            symbol: padded,
            tick_size,
            lot_size,
            price_scale: 2,
            quantity_scale: 0,
        }
    }

    pub fn symbol_str(&self) -> &str {
        let end = self.symbol.iter().position(|b| *b == 0).unwrap_or(8);
        std::str::from_utf8(&self.symbol[..end]).unwrap_or("")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    pub run_id: u128,
    pub seed: u64,
    /// Informational UTC nanoseconds. Never used for decisions.
    pub start_wall_time_ns: u64,
    pub instruments: Vec<InstrumentSpec>,
}

impl FileHeader {
    pub fn encode(&self, out: &mut Vec<u8>) {
        let start = out.len();
        let header_len = 46 + self.instruments.len() * INSTRUMENT_LEN + 4;
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&FORMAT_MAJOR.to_le_bytes());
        out.extend_from_slice(&FORMAT_MINOR.to_le_bytes());
        out.extend_from_slice(&ENDIAN_MARKER.to_le_bytes());
        out.extend_from_slice(&(header_len as u16).to_le_bytes());
        out.extend_from_slice(&self.run_id.to_le_bytes());
        out.extend_from_slice(&self.seed.to_le_bytes());
        out.extend_from_slice(&self.start_wall_time_ns.to_le_bytes());
        out.extend_from_slice(&(self.instruments.len() as u16).to_le_bytes());
        for instrument in &self.instruments {
            out.extend_from_slice(&instrument.id.0.to_le_bytes());
            out.extend_from_slice(&instrument.symbol);
            out.extend_from_slice(&instrument.tick_size.to_le_bytes());
            out.extend_from_slice(&instrument.lot_size.to_le_bytes());
            out.push(instrument.price_scale);
            out.push(instrument.quantity_scale);
        }
        let crc = crc32fast::hash(&out[start..]);
        out.extend_from_slice(&crc.to_le_bytes());
    }

    /// Decodes the header and returns the number of bytes consumed.
    pub fn decode(bytes: &[u8]) -> Result<(FileHeader, usize), DecodeError> {
        let mut reader = Reader::new(bytes);
        if reader.take(4)? != MAGIC {
            return Err(DecodeError::new(DecodeErrorKind::BadMagic, 0));
        }
        let major = reader.u16()?;
        let minor = reader.u16()?;
        if major != FORMAT_MAJOR || minor > FORMAT_MINOR {
            return Err(DecodeError::new(DecodeErrorKind::UnsupportedVersion, 4));
        }
        if reader.u16()? != ENDIAN_MARKER {
            return Err(DecodeError::new(DecodeErrorKind::BadEndian, 8));
        }
        let header_len = usize::from(reader.u16()?);
        if header_len < 50 || header_len > bytes.len() {
            return Err(DecodeError::new(DecodeErrorKind::Truncated, 10));
        }
        let run_id = reader.u128()?;
        let seed = reader.u64()?;
        let start_wall_time_ns = reader.u64()?;
        let instrument_count = reader.u16()?;
        if instrument_count > MAX_INSTRUMENTS {
            return Err(DecodeError::new(DecodeErrorKind::InstrumentTable, 44));
        }
        if header_len != 46 + usize::from(instrument_count) * INSTRUMENT_LEN + 4 {
            return Err(DecodeError::new(DecodeErrorKind::InstrumentTable, 10));
        }

        let mut instruments = Vec::with_capacity(usize::from(instrument_count));
        for _ in 0..instrument_count {
            let id = InstrumentId(reader.u32()?);
            let mut symbol = [0u8; 8];
            symbol.copy_from_slice(reader.take(8)?);
            let tick_size = reader.i64()?;
            let lot_size = reader.u64()?;
            let price_scale = reader.u8()?;
            let quantity_scale = reader.u8()?;
            if tick_size <= 0 || lot_size == 0 {
                return Err(DecodeError::new(
                    DecodeErrorKind::InstrumentTable,
                    reader.offset,
                ));
            }
            if instruments
                .iter()
                .any(|existing: &InstrumentSpec| existing.id == id)
            {
                return Err(DecodeError::new(
                    DecodeErrorKind::InstrumentTable,
                    reader.offset,
                ));
            }
            instruments.push(InstrumentSpec {
                id,
                symbol,
                tick_size,
                lot_size,
                price_scale,
                quantity_scale,
            });
        }

        let expected = crc32fast::hash(&bytes[..header_len - 4]);
        if reader.u32()? != expected {
            return Err(DecodeError::new(DecodeErrorKind::Checksum, header_len - 4));
        }

        Ok((
            FileHeader {
                run_id,
                seed,
                start_wall_time_ns,
                instruments,
            },
            header_len,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameBody {
    SnapshotBegin {
        snapshot_seq: u64,
    },
    SnapshotLevel {
        side: Side,
        price: PriceTicks,
        quantity: QuantityLots,
    },
    SnapshotEnd {
        snapshot_seq: u64,
        level_count: u32,
        state_crc: u32,
    },
    LevelSet {
        side: Side,
        price: PriceTicks,
        quantity: QuantityLots,
    },
    MarketTrade {
        aggressor: Side,
        price: PriceTicks,
        quantity: QuantityLots,
    },
    Heartbeat,
    FeedReset {
        new_epoch: u64,
    },
    OrderRequest(OrderRequest),
}

impl FrameBody {
    fn message_type(&self) -> u8 {
        match self {
            FrameBody::SnapshotBegin { .. } => TYPE_SNAPSHOT_BEGIN,
            FrameBody::SnapshotLevel { .. } => TYPE_SNAPSHOT_LEVEL,
            FrameBody::SnapshotEnd { .. } => TYPE_SNAPSHOT_END,
            FrameBody::LevelSet { .. } => TYPE_LEVEL_SET,
            FrameBody::MarketTrade { .. } => TYPE_MARKET_TRADE,
            FrameBody::Heartbeat => TYPE_HEARTBEAT,
            FrameBody::FeedReset { .. } => TYPE_FEED_RESET,
            FrameBody::OrderRequest(_) => TYPE_ORDER_REQUEST,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Frame {
    /// Per channel. Zero is reserved for non-sequenced messages.
    pub source_seq: u64,
    pub source_time_ns: u64,
    pub recv_time_ns: u64,
    pub instrument: InstrumentId,
    pub body: FrameBody,
}

impl Frame {
    pub fn encode(&self, out: &mut Vec<u8>) {
        let start = out.len();
        out.extend_from_slice(&0u32.to_le_bytes());
        out.push(self.body.message_type());
        out.push(0);
        out.extend_from_slice(&SCHEMA_VERSION.to_le_bytes());
        out.extend_from_slice(&self.source_seq.to_le_bytes());
        out.extend_from_slice(&self.source_time_ns.to_le_bytes());
        out.extend_from_slice(&self.recv_time_ns.to_le_bytes());
        out.extend_from_slice(&self.instrument.0.to_le_bytes());
        encode_payload(&self.body, out);
        let frame_len = (out.len() - start + FRAME_CRC_LEN) as u32;
        out[start..start + 4].copy_from_slice(&frame_len.to_le_bytes());
        let crc = crc32fast::hash(&out[start..]);
        out.extend_from_slice(&crc.to_le_bytes());
    }

    /// Decodes one frame and returns the bytes consumed. Never panics and never
    /// allocates from an unchecked length.
    pub fn decode(bytes: &[u8]) -> Result<(Frame, usize), DecodeError> {
        if bytes.len() < 4 {
            return Err(DecodeError::new(DecodeErrorKind::Truncated, 0));
        }
        let frame_len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        if frame_len < (FRAME_PREFIX_LEN + FRAME_CRC_LEN) as u32 || frame_len > MAX_FRAME_LEN {
            return Err(DecodeError::new(DecodeErrorKind::FrameLength, 0));
        }
        let frame_len = frame_len as usize;
        if bytes.len() < frame_len {
            return Err(DecodeError::new(DecodeErrorKind::Truncated, 0));
        }
        let frame = &bytes[..frame_len];

        let stored_crc = u32::from_le_bytes([
            frame[frame_len - 4],
            frame[frame_len - 3],
            frame[frame_len - 2],
            frame[frame_len - 1],
        ]);
        if stored_crc != crc32fast::hash(&frame[..frame_len - 4]) {
            return Err(DecodeError::new(DecodeErrorKind::Checksum, frame_len - 4));
        }

        let mut reader = Reader::new(&frame[..frame_len - FRAME_CRC_LEN]);
        reader.skip(4)?;
        let message_type = reader.u8()?;
        if reader.u8()? != 0 {
            return Err(DecodeError::new(DecodeErrorKind::ReservedFlags, 5));
        }
        if reader.u16()? != SCHEMA_VERSION {
            return Err(DecodeError::new(DecodeErrorKind::UnsupportedVersion, 6));
        }
        let source_seq = reader.u64()?;
        let source_time_ns = reader.u64()?;
        let recv_time_ns = reader.u64()?;
        let instrument = InstrumentId(reader.u32()?);

        let body = decode_payload(message_type, &mut reader, instrument)
            .map_err(|error| error.with_source_seq(source_seq))?;
        if reader.remaining() != 0 {
            return Err(
                DecodeError::new(DecodeErrorKind::PayloadLength, reader.offset)
                    .with_source_seq(source_seq),
            );
        }

        Ok((
            Frame {
                source_seq,
                source_time_ns,
                recv_time_ns,
                instrument,
                body,
            },
            frame_len,
        ))
    }
}

fn encode_payload(body: &FrameBody, out: &mut Vec<u8>) {
    match *body {
        FrameBody::SnapshotBegin { snapshot_seq } => {
            out.extend_from_slice(&snapshot_seq.to_le_bytes());
        }
        FrameBody::SnapshotLevel {
            side,
            price,
            quantity,
        }
        | FrameBody::LevelSet {
            side,
            price,
            quantity,
        } => {
            out.push(side.wire());
            out.extend_from_slice(&price.0.to_le_bytes());
            out.extend_from_slice(&quantity.0.to_le_bytes());
        }
        FrameBody::SnapshotEnd {
            snapshot_seq,
            level_count,
            state_crc,
        } => {
            out.extend_from_slice(&snapshot_seq.to_le_bytes());
            out.extend_from_slice(&level_count.to_le_bytes());
            out.extend_from_slice(&state_crc.to_le_bytes());
        }
        FrameBody::MarketTrade {
            aggressor,
            price,
            quantity,
        } => {
            out.push(aggressor.wire());
            out.extend_from_slice(&price.0.to_le_bytes());
            out.extend_from_slice(&quantity.0.to_le_bytes());
        }
        FrameBody::Heartbeat => {}
        FrameBody::FeedReset { new_epoch } => {
            out.extend_from_slice(&new_epoch.to_le_bytes());
        }
        FrameBody::OrderRequest(request) => {
            out.push(request.kind.wire());
            out.push(request.side.wire());
            out.push(request.order_type.wire());
            out.extend_from_slice(&request.account.0.to_le_bytes());
            out.extend_from_slice(&request.client_order_id.0.to_le_bytes());
            out.extend_from_slice(&request.target_client_order_id.0.to_le_bytes());
            out.extend_from_slice(&request.request_id.0.to_le_bytes());
            out.extend_from_slice(&request.client_seq.to_le_bytes());
            out.extend_from_slice(&request.price.0.to_le_bytes());
            out.extend_from_slice(&request.quantity.0.to_le_bytes());
        }
    }
}

fn decode_payload(
    message_type: u8,
    reader: &mut Reader<'_>,
    instrument: InstrumentId,
) -> Result<FrameBody, DecodeError> {
    match message_type {
        TYPE_SNAPSHOT_BEGIN => Ok(FrameBody::SnapshotBegin {
            snapshot_seq: reader.u64()?,
        }),
        TYPE_SNAPSHOT_LEVEL => {
            let (side, price, quantity) = read_level(reader)?;
            Ok(FrameBody::SnapshotLevel {
                side,
                price,
                quantity,
            })
        }
        TYPE_SNAPSHOT_END => Ok(FrameBody::SnapshotEnd {
            snapshot_seq: reader.u64()?,
            level_count: reader.u32()?,
            state_crc: reader.u32()?,
        }),
        TYPE_LEVEL_SET => {
            let (side, price, quantity) = read_level(reader)?;
            Ok(FrameBody::LevelSet {
                side,
                price,
                quantity,
            })
        }
        TYPE_MARKET_TRADE => {
            let (aggressor, price, quantity) = read_level(reader)?;
            if quantity.is_zero() {
                return Err(reader.invalid_field());
            }
            Ok(FrameBody::MarketTrade {
                aggressor,
                price,
                quantity,
            })
        }
        TYPE_HEARTBEAT => Ok(FrameBody::Heartbeat),
        TYPE_FEED_RESET => Ok(FrameBody::FeedReset {
            new_epoch: reader.u64()?,
        }),
        TYPE_ORDER_REQUEST => {
            let kind =
                RequestKind::from_wire(reader.u8()?).ok_or_else(|| reader.invalid_field())?;
            let side = Side::from_wire(reader.u8()?).ok_or_else(|| reader.invalid_field())?;
            let order_type =
                OrderType::from_wire(reader.u8()?).ok_or_else(|| reader.invalid_field())?;
            Ok(FrameBody::OrderRequest(OrderRequest {
                kind,
                side,
                order_type,
                account: AccountId(reader.u32()?),
                instrument,
                client_order_id: ClientOrderId(reader.u64()?),
                target_client_order_id: ClientOrderId(reader.u64()?),
                request_id: RequestId(reader.u64()?),
                client_seq: reader.u64()?,
                price: PriceTicks(reader.i64()?),
                quantity: QuantityLots(reader.u64()?),
            }))
        }
        _ => Err(DecodeError::new(
            DecodeErrorKind::UnknownMessageType,
            reader.offset,
        )),
    }
}

fn read_level(reader: &mut Reader<'_>) -> Result<(Side, PriceTicks, QuantityLots), DecodeError> {
    let side = Side::from_wire(reader.u8()?).ok_or_else(|| reader.invalid_field())?;
    let price = PriceTicks(reader.i64()?);
    let quantity = QuantityLots(reader.u64()?);
    Ok((side, price, quantity))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_frames() -> Vec<Frame> {
        vec![
            Frame {
                source_seq: 1,
                source_time_ns: 100,
                recv_time_ns: 110,
                instrument: InstrumentId(1),
                body: FrameBody::SnapshotBegin { snapshot_seq: 42 },
            },
            Frame {
                source_seq: 2,
                source_time_ns: 200,
                recv_time_ns: 210,
                instrument: InstrumentId(1),
                body: FrameBody::SnapshotLevel {
                    side: Side::Buy,
                    price: PriceTicks(10_000),
                    quantity: QuantityLots(5),
                },
            },
            Frame {
                source_seq: 3,
                source_time_ns: 300,
                recv_time_ns: 310,
                instrument: InstrumentId(1),
                body: FrameBody::SnapshotEnd {
                    snapshot_seq: 42,
                    level_count: 1,
                    state_crc: 7,
                },
            },
            Frame {
                source_seq: 4,
                source_time_ns: 400,
                recv_time_ns: 410,
                instrument: InstrumentId(1),
                body: FrameBody::LevelSet {
                    side: Side::Sell,
                    price: PriceTicks(-5),
                    quantity: QuantityLots(0),
                },
            },
            Frame {
                source_seq: 5,
                source_time_ns: 500,
                recv_time_ns: 510,
                instrument: InstrumentId(1),
                body: FrameBody::MarketTrade {
                    aggressor: Side::Buy,
                    price: PriceTicks(10_001),
                    quantity: QuantityLots(3),
                },
            },
            Frame {
                source_seq: 0,
                source_time_ns: 600,
                recv_time_ns: 610,
                instrument: InstrumentId(1),
                body: FrameBody::Heartbeat,
            },
            Frame {
                source_seq: 6,
                source_time_ns: 700,
                recv_time_ns: 710,
                instrument: InstrumentId(1),
                body: FrameBody::FeedReset { new_epoch: 2 },
            },
            Frame {
                source_seq: 7,
                source_time_ns: 800,
                recv_time_ns: 810,
                instrument: InstrumentId(1),
                body: FrameBody::OrderRequest(OrderRequest {
                    kind: RequestKind::New,
                    account: AccountId(9),
                    instrument: InstrumentId(1),
                    request_id: RequestId(1),
                    client_seq: 1,
                    client_order_id: ClientOrderId(1000),
                    target_client_order_id: ClientOrderId(0),
                    side: Side::Buy,
                    order_type: OrderType::Limit,
                    price: PriceTicks(9_999),
                    quantity: QuantityLots(4),
                }),
            },
        ]
    }

    #[test]
    fn frame_round_trip_is_canonical() {
        for frame in sample_frames() {
            let mut bytes = Vec::new();
            frame.encode(&mut bytes);
            let (decoded, consumed) = Frame::decode(&bytes).unwrap();
            assert_eq!(consumed, bytes.len());
            assert_eq!(decoded, frame);

            let mut reencoded = Vec::new();
            decoded.encode(&mut reencoded);
            assert_eq!(reencoded, bytes);
        }
    }

    #[test]
    fn header_round_trip() {
        let header = FileHeader {
            run_id: 0x1234_5678_9abc_def0_1234_5678_9abc_def0,
            seed: 7,
            start_wall_time_ns: 1_700_000_000_000_000_000,
            instruments: vec![
                InstrumentSpec::new(1, "LAB-USD", 1, 1),
                InstrumentSpec::new(2, "LAB2", 5, 10),
            ],
        };
        let mut bytes = Vec::new();
        header.encode(&mut bytes);
        let (decoded, consumed) = FileHeader::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, header);
        assert_eq!(decoded.instruments[0].symbol_str(), "LAB-USD");
    }

    #[test]
    fn truncation_is_reported_not_panicked() {
        let mut bytes = Vec::new();
        sample_frames()[1].encode(&mut bytes);
        for len in 0..bytes.len() {
            let error = Frame::decode(&bytes[..len]).unwrap_err();
            assert_eq!(error.kind, DecodeErrorKind::Truncated);
        }
    }

    #[test]
    fn corrupted_byte_fails_checksum() {
        let mut bytes = Vec::new();
        sample_frames()[4].encode(&mut bytes);
        bytes[20] ^= 0xff;
        assert_eq!(
            Frame::decode(&bytes).unwrap_err().kind,
            DecodeErrorKind::Checksum
        );
    }

    #[test]
    fn oversized_length_is_rejected_before_allocation() {
        let mut bytes = vec![0u8; 64];
        bytes[..4].copy_from_slice(&(MAX_FRAME_LEN + 1).to_le_bytes());
        assert_eq!(
            Frame::decode(&bytes).unwrap_err().kind,
            DecodeErrorKind::FrameLength
        );
    }

    #[test]
    fn unknown_message_type_is_rejected() {
        let mut bytes = Vec::new();
        sample_frames()[5].encode(&mut bytes);
        bytes[4] = 200;
        let len = bytes.len();
        let crc = crc32fast::hash(&bytes[..len - 4]);
        bytes[len - 4..].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(
            Frame::decode(&bytes).unwrap_err().kind,
            DecodeErrorKind::UnknownMessageType
        );
    }

    #[test]
    fn reserved_flags_must_be_zero() {
        let mut bytes = Vec::new();
        sample_frames()[5].encode(&mut bytes);
        bytes[5] = 1;
        let len = bytes.len();
        let crc = crc32fast::hash(&bytes[..len - 4]);
        bytes[len - 4..].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(
            Frame::decode(&bytes).unwrap_err().kind,
            DecodeErrorKind::ReservedFlags
        );
    }

    #[test]
    fn trailing_payload_bytes_are_rejected() {
        let mut bytes = Vec::new();
        sample_frames()[5].encode(&mut bytes);
        // Grow the frame by one byte without changing the message type.
        let len = bytes.len();
        bytes.insert(len - 4, 0);
        let new_len = bytes.len() as u32;
        bytes[..4].copy_from_slice(&new_len.to_le_bytes());
        let crc = crc32fast::hash(&bytes[..bytes.len() - 4]);
        let end = bytes.len();
        bytes[end - 4..].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(
            Frame::decode(&bytes).unwrap_err().kind,
            DecodeErrorKind::PayloadLength
        );
    }
}
