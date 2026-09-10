//! Bounds-checked cursor. Every read fails instead of panicking.

use crate::error::{DecodeError, DecodeErrorKind};

pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    pub(crate) offset: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Reader<'a> {
        Reader { bytes, offset: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }

    pub(crate) fn take(&mut self, len: usize) -> Result<&'a [u8], DecodeError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| DecodeError::new(DecodeErrorKind::Truncated, self.offset))?;
        if end > self.bytes.len() {
            return Err(DecodeError::new(DecodeErrorKind::Truncated, self.offset));
        }
        let slice = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(slice)
    }

    pub(crate) fn skip(&mut self, len: usize) -> Result<(), DecodeError> {
        self.take(len).map(|_| ())
    }

    pub(crate) fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u16(&mut self) -> Result<u16, DecodeError> {
        let bytes = self.take(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    pub(crate) fn u32(&mut self) -> Result<u32, DecodeError> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, DecodeError> {
        let bytes = self.take(8)?;
        let mut buffer = [0u8; 8];
        buffer.copy_from_slice(bytes);
        Ok(u64::from_le_bytes(buffer))
    }

    pub(crate) fn i64(&mut self) -> Result<i64, DecodeError> {
        self.u64().map(|value| value as i64)
    }

    pub(crate) fn u128(&mut self) -> Result<u128, DecodeError> {
        let bytes = self.take(16)?;
        let mut buffer = [0u8; 16];
        buffer.copy_from_slice(bytes);
        Ok(u128::from_le_bytes(buffer))
    }

    pub(crate) fn invalid_field(&self) -> DecodeError {
        DecodeError::new(DecodeErrorKind::InvalidField, self.offset)
    }
}
