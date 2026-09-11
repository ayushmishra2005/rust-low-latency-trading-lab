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
    /// A complete header was present but magic or version is wrong.
    BadHeader,
    /// The file exists but the 24-byte header is not fully visible yet.
    /// Callers may retry; this is not corruption.
    IncompleteHeader,
    /// Recovery stopped at this byte offset; everything before it is valid.
    TruncatedTail(usize),
    Corrupt(usize),
}

impl std::fmt::Display for JournalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JournalError::Io(error) => write!(f, "journal io error: {error}"),
            JournalError::BadHeader => write!(f, "journal header is not recognised"),
            JournalError::IncompleteHeader => {
                write!(f, "journal header is not fully visible yet")
            }
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
    sink: BufWriter<Box<dyn Write + Send>>,
    /// Kept only for on-disk journals so `sync` can reach the file descriptor.
    file: Option<File>,
    scratch: Vec<u8>,
    records: u64,
}

impl JournalWriter {
    pub fn create(path: &Path, run_id: u128) -> Result<JournalWriter, JournalError> {
        let file = File::create(path)?;
        let handle = file.try_clone()?;
        let mut writer = JournalWriter::with_sink(Box::new(file), run_id)?;
        writer.file = Some(handle);
        Ok(writer)
    }

    /// Writes the journal to any sink. Used for in-memory and failure testing.
    pub fn with_sink(
        sink: Box<dyn Write + Send>,
        run_id: u128,
    ) -> Result<JournalWriter, JournalError> {
        let mut sink = BufWriter::new(sink);
        let mut header = Vec::with_capacity(HEADER_LEN);
        header.extend_from_slice(&MAGIC);
        header.extend_from_slice(&VERSION.to_le_bytes());
        header.extend_from_slice(&0u16.to_le_bytes());
        header.extend_from_slice(&run_id.to_le_bytes());
        // Publish the header at once so a reader can attach immediately.
        sink.write_all(&header)?;
        sink.flush()?;
        Ok(JournalWriter {
            sink,
            file: None,
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
        self.sink.write_all(&len.to_le_bytes())?;
        self.sink.write_all(&self.scratch)?;
        self.sink.write_all(&crc.to_le_bytes())?;
        self.records += 1;
        Ok(())
    }

    /// Flushes buffered records. Durability mode decides how often this runs.
    pub fn flush(&mut self) -> Result<(), JournalError> {
        self.sink.flush()?;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), JournalError> {
        self.sink.flush()?;
        if let Some(file) = self.file.as_ref() {
            file.sync_data()?;
        }
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
    if bytes.len() < HEADER_LEN {
        return Err(JournalError::IncompleteHeader);
    }
    if bytes[..4] != MAGIC || u16::from_le_bytes([bytes[4], bytes[5]]) != VERSION {
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

/// Incremental reader for a growing journal. The gateway uses it to stream
/// durable output events to cold consumers without touching engine state.
pub struct JournalTail {
    file: File,
    /// Offset of the first record not yet returned.
    offset: usize,
    /// Bytes read from `offset` onwards that did not form a whole record yet.
    pending: Vec<u8>,
    bytes_read: u64,
}

impl JournalTail {
    pub fn open(path: &Path) -> Result<JournalTail, JournalError> {
        let mut file = File::open(path)?;
        let mut header = [0u8; HEADER_LEN];
        let mut filled = 0;
        while filled < HEADER_LEN {
            match file.read(&mut header[filled..])? {
                0 => return Err(JournalError::IncompleteHeader),
                n => filled += n,
            }
        }
        if header[..4] != MAGIC || u16::from_le_bytes([header[4], header[5]]) != VERSION {
            return Err(JournalError::BadHeader);
        }
        Ok(JournalTail {
            file,
            offset: HEADER_LEN,
            pending: Vec::new(),
            bytes_read: 0,
        })
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Journal bytes this reader has pulled from the file. A poll must never
    /// re-read bytes it already consumed.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Returns records that became complete since the last poll. Only bytes
    /// appended since the last poll are read.
    pub fn poll(&mut self, limit: usize) -> Result<Vec<OutputEvent>, JournalError> {
        let mut chunk = [0u8; 8192];
        loop {
            let read = self.file.read(&mut chunk)?;
            if read == 0 {
                break;
            }
            self.pending.extend_from_slice(&chunk[..read]);
            self.bytes_read += read as u64;
        }

        let mut events = Vec::new();
        let mut consumed = 0usize;
        while events.len() < limit {
            let rest = &self.pending[consumed..];
            if rest.len() < 4 {
                break;
            }
            let len = u32::from_le_bytes([rest[0], rest[1], rest[2], rest[3]]);
            if len == 0 || len > MAX_RECORD_LEN {
                return Err(JournalError::Corrupt(self.offset + consumed));
            }
            let end = 4 + len as usize + 4;
            if rest.len() < end {
                // Hold an incomplete record until the writer finishes it.
                break;
            }
            let payload = &rest[4..end - 4];
            let stored =
                u32::from_le_bytes([rest[end - 4], rest[end - 3], rest[end - 2], rest[end - 1]]);
            if stored != crc32fast::hash(payload) {
                return Err(JournalError::Corrupt(self.offset + consumed));
            }
            match canonical::decode_output(payload) {
                Ok((event, used)) if used == payload.len() => events.push(event),
                _ => return Err(JournalError::Corrupt(self.offset + consumed)),
            }
            consumed += end;
        }

        if consumed > 0 {
            self.pending.drain(..consumed);
            self.offset += consumed;
        }
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::{AccountId, EngineSeq, EngineStateEvent, OutputSeq, StateEvent};
    use std::io::ErrorKind;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    fn unique_path(name: &str) -> std::path::PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "rltl-journal-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

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
        let path = unique_path("round-trip.journal");

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
        let path = unique_path("truncated.journal");

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
    fn the_tail_reads_each_record_once_and_only_reads_new_bytes() {
        let path = unique_path("tail-incremental.journal");

        let mut writer = JournalWriter::create(&path, 5).unwrap();
        writer.flush().unwrap();
        let mut tail = JournalTail::open(&path).unwrap();
        assert!(tail.poll(100).unwrap().is_empty());

        let mut seen = Vec::new();
        let mut seq = 0u64;
        for batch in 1..=4u64 {
            for _ in 0..batch {
                seq += 1;
                writer.append(&sample(seq)).unwrap();
            }
            writer.flush().unwrap();

            let before = tail.bytes_read();
            let events = tail.poll(100).unwrap();
            let read = tail.bytes_read() - before;
            assert_eq!(events.len() as u64, batch);
            // Only the newly appended bytes were pulled from the file.
            assert!(read > 0 && read < 200 * batch, "read {read} bytes");
            seen.extend(events);
        }

        assert_eq!(seen.len(), 10);
        for (index, event) in seen.iter().enumerate() {
            assert_eq!(*event, sample(index as u64 + 1));
        }
        assert_eq!(
            tail.offset(),
            std::fs::metadata(&path).unwrap().len() as usize
        );
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn the_tail_holds_a_partial_record_until_it_is_complete() {
        let path = unique_path("tail-partial.journal");

        // Build a complete two record journal, then publish it byte by byte.
        let mut writer = JournalWriter::create(&path, 5).unwrap();
        writer.append(&sample(1)).unwrap();
        writer.append(&sample(2)).unwrap();
        writer.sync().unwrap();
        drop(writer);
        let full = std::fs::read(&path).unwrap();

        let partial = path.with_extension("partial");
        std::fs::write(&partial, &full[..HEADER_LEN]).unwrap();
        let mut tail = JournalTail::open(&partial).unwrap();

        let mut delivered = Vec::new();
        for end in HEADER_LEN + 1..=full.len() {
            std::fs::write(&partial, &full[..end]).unwrap();
            delivered.extend(tail.poll(100).unwrap());
        }
        assert_eq!(delivered, vec![sample(1), sample(2)]);

        std::fs::remove_file(&path).unwrap();
        std::fs::remove_file(&partial).unwrap();
    }

    #[test]
    fn corrupt_record_is_not_accepted() {
        let path = unique_path("corrupt.journal");

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

    #[test]
    fn empty_journal_file_is_not_ready() {
        let path = unique_path("empty.journal");
        std::fs::write(&path, []).unwrap();
        assert!(matches!(
            JournalTail::open(&path),
            Err(JournalError::IncompleteHeader)
        ));
        assert!(matches!(
            read_journal(&path),
            Err(JournalError::IncompleteHeader)
        ));
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn partial_journal_header_is_not_ready() {
        let path = unique_path("partial-header.journal");
        std::fs::write(&path, &MAGIC[..]).unwrap();
        assert!(matches!(
            JournalTail::open(&path),
            Err(JournalError::IncompleteHeader)
        ));
        assert!(matches!(
            read_journal(&path),
            Err(JournalError::IncompleteHeader)
        ));
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn complete_valid_header_opens_the_tail() {
        let path = unique_path("valid-header.journal");
        JournalWriter::create(&path, 11).unwrap();
        let mut tail = JournalTail::open(&path).unwrap();
        assert!(tail.poll(8).unwrap().is_empty());
        let recovered = read_journal(&path).unwrap();
        assert_eq!(recovered.run_id, 11);
        assert!(recovered.events.is_empty());
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn complete_invalid_header_is_corruption() {
        let path = unique_path("bad-header.journal");
        let mut bytes = vec![0u8; HEADER_LEN];
        bytes[..4].copy_from_slice(b"XXXX");
        std::fs::write(&path, &bytes).unwrap();
        assert!(matches!(
            JournalTail::open(&path),
            Err(JournalError::BadHeader)
        ));
        assert!(matches!(read_journal(&path), Err(JournalError::BadHeader)));
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn tail_attaches_while_the_writer_creates_the_journal() {
        let path = unique_path("concurrent-create.journal");
        let writer_path = path.clone();
        let writer = std::thread::spawn(move || {
            let mut writer = JournalWriter::create(&writer_path, 99).unwrap();
            writer.append(&sample(1)).unwrap();
            writer.flush().unwrap();
            writer
        });

        let reader_path = path.clone();
        let reader = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut tail = loop {
                match JournalTail::open(&reader_path) {
                    Ok(tail) => break tail,
                    Err(JournalError::IncompleteHeader) => {}
                    Err(JournalError::Io(error)) if error.kind() == ErrorKind::NotFound => {}
                    Err(error) => panic!("unexpected attach error: {error}"),
                }
                if Instant::now() >= deadline {
                    panic!("timed out waiting for a complete journal header");
                }
                std::thread::yield_now();
            };
            loop {
                let events = tail.poll(8).unwrap();
                if !events.is_empty() {
                    return events;
                }
                if Instant::now() >= deadline {
                    panic!("timed out waiting for the first journal record");
                }
                std::thread::yield_now();
            }
        });

        drop(writer.join().unwrap());
        assert_eq!(reader.join().unwrap(), vec![sample(1)]);
        std::fs::remove_file(&path).unwrap();
    }
}
