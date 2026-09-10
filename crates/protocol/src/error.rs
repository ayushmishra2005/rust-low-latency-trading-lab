use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DecodeErrorKind {
    #[error("truncated input")]
    Truncated,
    #[error("bad magic bytes")]
    BadMagic,
    #[error("unsupported format version")]
    UnsupportedVersion,
    #[error("unexpected endianness marker")]
    BadEndian,
    #[error("frame length outside allowed bounds")]
    FrameLength,
    #[error("unknown message type")]
    UnknownMessageType,
    #[error("reserved flag bits set")]
    ReservedFlags,
    #[error("payload length does not match message type")]
    PayloadLength,
    #[error("checksum mismatch")]
    Checksum,
    #[error("field value outside the allowed domain")]
    InvalidField,
    #[error("instrument table is invalid")]
    InstrumentTable,
}

/// Decode failures carry the byte offset so replay can stop at an exact position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("{kind} at offset {offset}")]
pub struct DecodeError {
    pub kind: DecodeErrorKind,
    pub offset: usize,
    pub source_seq: Option<u64>,
}

impl DecodeError {
    pub fn new(kind: DecodeErrorKind, offset: usize) -> DecodeError {
        DecodeError {
            kind,
            offset,
            source_seq: None,
        }
    }

    pub fn with_source_seq(mut self, source_seq: u64) -> DecodeError {
        self.source_seq = Some(source_seq);
        self
    }
}
