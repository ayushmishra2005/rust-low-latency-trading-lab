//! Durable ordered output journal.
//!
//! Frames are length prefixed with a CRC so a truncated tail is detected instead
//! of invented. Only the output thread writes it; the engine thread never does.

use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;

use protocol::canonical;
use protocol::OutputEvent;

const MAGIC: [u8; 4] = *b"RLTJ";
const VERSION: u16 = 1;
const HEADER_LEN: usize = 24;
const MAX_RECORD_LEN: u32 = 4096;

#[derive(Debug)]
pub enum JournalError {
    Io(std::io::Error),
    BadHeader,
    /// Recovery stopped at this byte offset; everything before it is valid.
    TruncatedTail(usize),
    Corrupt(usize),
}

impl std::fmt::Display for JournalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JournalError::Io(error) => write!(f, "journal io error: {error}"),
            JournalError::BadHeader => write!(f, "journal header is not recognised"),
            JournalError::TruncatedTail(offset) => {
                write!(f, "journal truncated at offset {offset}")
            }
            JournalError::Corrupt(offset) => write!(f, "journal corrupt at offset {offset}"),
        }
    }
}

impl std::error::Error for JournalError {}

impl From<std::io::Error> for JournalError {
    fn from(error: std::io::Error) -> JournalError {
        JournalError::Io(error)
    }
}

pub struct JournalWriter {
    file: BufWriter<File>,
    scratch: Vec<u8>,
    records: u64,
}

impl JournalWriter {
    pub fn create(path: &Path, run_id: u128) -> Result<JournalWriter, JournalError> {
        let mut file = BufWriter::new(File::create(path)?);
        let mut header = Vec::with_capacity(HEADER_LEN);
        header.extend_from_slice(&MAGIC);
        header.extend_from_slice(&VERSION.to_le_bytes());
        header.extend_from_slice(&0u16.to_le_bytes());
        header.extend_from_slice(&run_id.to_le_bytes());
        file.write_all(&header)?;
        Ok(JournalWriter {
            file,
            scratch: Vec::with_capacity(256),
            records: 0,
        })
    }

    pub fn records(&self) -> u64 {
        self.records
    }

    pub fn append(&mut self, event: &OutputEvent) -> Result<(), JournalError> {
        self.scratch.clear();
        canonical::encode_output(event, &mut self.scratch);
        let len = self.scratch.len() as u32;
        let crc = crc32fast::hash(&self.scratch);
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&self.scratch)?;
        self.file.write_all(&crc.to_le_bytes())?;
        self.records += 1;
        Ok(())
    }

    /// Flushes buffered records. Durability mode decides how often this runs.
    pub fn flush(&mut self) -> Result<(), JournalError> {
        self.file.flush()?;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), JournalError> {
        self.file.flush()?;
        self.file.get_ref().sync_data()?;
        Ok(())
    }
}

pub struct JournalRecovery {
    pub run_id: u128,
    pub events: Vec<OutputEvent>,
    /// Set when the last record was incomplete or corrupt.
    pub truncated_at: Option<usize>,
}

/// Reads a journal, stopping at the last complete valid record.
pub fn read_journal(path: &Path) -> Result<JournalRecovery, JournalError> {
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    if bytes.len() < HEADER_LEN || bytes[..4] != MAGIC {
        return Err(JournalError::BadHeader);
    }
    if u16::from_le_bytes([bytes[4], bytes[5]]) != VERSION {
        return Err(JournalError::BadHeader);
    }
    let mut run_id_bytes = [0u8; 16];
    run_id_bytes.copy_from_slice(&bytes[8..24]);
    let run_id = u128::from_le_bytes(run_id_bytes);

    let mut events = Vec::new();
    let mut offset = HEADER_LEN;
    let mut truncated_at = None;
    while offset < bytes.len() {
        if bytes.len() - offset < 4 {
            truncated_at = Some(offset);
            break;
        }
        let len = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        if len == 0 || len > MAX_RECORD_LEN {
            truncated_at = Some(offset);
            break;
        }
        let len = len as usize;
        let end = offset + 4 + len + 4;
        if end > bytes.len() {
            truncated_at = Some(offset);
            break;
        }
        let payload = &bytes[offset + 4..offset + 4 + len];
        let stored = u32::from_le_bytes([
            bytes[end - 4],
            bytes[end - 3],
            bytes[end - 2],
            bytes[end - 1],
        ]);
        if stored != crc32fast::hash(payload) {
            truncated_at = Some(offset);
            break;
        }
        match canonical::decode_output(payload) {
            Ok((event, consumed)) if consumed == payload.len() => events.push(event),
            _ => {
                truncated_at = Some(offset);
                break;
            }
        }
        offset = end;
    }

    Ok(JournalRecovery {
        run_id,
        events,
        truncated_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::{AccountId, EngineSeq, EngineStateEvent, OutputSeq, StateEvent};

    fn sample(seq: u64) -> OutputEvent {
        OutputEvent::State(StateEvent {
            output_seq: OutputSeq(seq),
            engine_seq: EngineSeq(seq),
            engine_time_ns: seq * 10,
            event: EngineStateEvent::AccountEnabledChanged {
                account: AccountId(1),
                enabled: seq % 2 == 0,
            },
        })
    }

    #[test]
    fn journal_round_trips() {
        let dir = std::env::temp_dir().join(format!("rltl-journal-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("round-trip.journal");

        let mut writer = JournalWriter::create(&path, 42).unwrap();
        for seq in 1..=10 {
            writer.append(&sample(seq)).unwrap();
        }
        writer.sync().unwrap();
        drop(writer);

        let recovered = read_journal(&path).unwrap();
        assert_eq!(recovered.run_id, 42);
        assert_eq!(recovered.events.len(), 10);
        assert!(recovered.truncated_at.is_none());
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn truncated_tail_stops_at_the_last_complete_record() {
        let dir = std::env::temp_dir().join(format!("rltl-journal-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("truncated.journal");

        let mut writer = JournalWriter::create(&path, 7).unwrap();
        for seq in 1..=5 {
            writer.append(&sample(seq)).unwrap();
        }
        writer.sync().unwrap();
        drop(writer);

        let mut bytes = std::fs::read(&path).unwrap();
        bytes.truncate(bytes.len() - 3);
        std::fs::write(&path, &bytes).unwrap();

        let recovered = read_journal(&path).unwrap();
        assert_eq!(recovered.events.len(), 4);
        assert!(recovered.truncated_at.is_some());
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn corrupt_record_is_not_accepted() {
        let dir = std::env::temp_dir().join(format!("rltl-journal-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("corrupt.journal");

        let mut writer = JournalWriter::create(&path, 7).unwrap();
        for seq in 1..=3 {
            writer.append(&sample(seq)).unwrap();
        }
        writer.sync().unwrap();
        drop(writer);

        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 8;
        bytes[last] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();

        let recovered = read_journal(&path).unwrap();
        assert_eq!(recovered.events.len(), 2);
        std::fs::remove_file(&path).unwrap();
    }
}
