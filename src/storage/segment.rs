//! Single append-only segment file.

use crate::error::{Result, ThorstreamError};
use crate::types::Record;
use parking_lot::RwLock;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};

const LENGTH_PREFIX_BYTES: usize = 4;
const MAX_RECORD_SIZE: usize = 1024 * 1024 * 1024; // 1GB

/// A single segment file: append-only, length-prefixed records.
pub struct Segment {
    base_offset: i64,
    path: std::path::PathBuf,
    file: RwLock<File>,
    /// Next offset to assign (current size in offsets).
    next_offset: AtomicI64,
    /// Current write position in file (bytes).
    write_position: AtomicI64,
}

impl Segment {
    /// Open or create a segment at `path` with records starting at `base_offset`.
    pub fn open(path: impl AsRef<Path>, base_offset: i64) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(path.parent().unwrap())?;
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)?;
        let (next_offset, write_position) = if file.metadata()?.len() == 0 {
            (base_offset, 0i64)
        } else {
            Self::recover_offsets(&mut file, base_offset)?
        };
        Ok(Self {
            base_offset,
            path,
            file: RwLock::new(file),
            next_offset: AtomicI64::new(next_offset),
            write_position: AtomicI64::new(write_position),
        })
    }

    fn recover_offsets(file: &mut File, base_offset: i64) -> Result<(i64, i64)> {
        file.seek(SeekFrom::Start(0))?;
        let mut next = base_offset;
        let mut pos: i64 = 0;
        loop {
            let mut len_buf = [0u8; LENGTH_PREFIX_BYTES];
            if file.read_exact(&mut len_buf).is_err() {
                break;
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            if len > MAX_RECORD_SIZE {
                return Err(ThorstreamError::Storage(format!(
                    "Invalid record length in segment: {}",
                    len
                )));
            }
            let mut buf = vec![0u8; len];
            file.read_exact(&mut buf)?;
            pos += LENGTH_PREFIX_BYTES as i64 + len as i64;
            next += 1;
        }
        Ok((next, pos))
    }

    /// Append a record; returns assigned offset.
    pub fn append(&self, record: &Record) -> Result<i64> {
        let bytes = bincode::serialize(record)
            .map_err(|e| ThorstreamError::Serialization(e.to_string()))?;
        if bytes.len() > MAX_RECORD_SIZE {
            return Err(ThorstreamError::Storage("Record too large".into()));
        }
        let len = bytes.len() as u32;
        let mut file = self.file.write();
        file.write_all(&len.to_be_bytes())?;
        file.write_all(&bytes)?;
        file.flush()?;
        let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);
        self.write_position.fetch_add(
            LENGTH_PREFIX_BYTES as i64 + bytes.len() as i64,
            Ordering::SeqCst,
        );
        Ok(offset)
    }

    /// Read a single record at the given offset (offset is logical within segment).
    pub fn read_at(&self, offset: i64) -> Result<Option<Record>> {
        if offset < self.base_offset {
            return Err(ThorstreamError::InvalidOffset(offset));
        }
        let local_index = (offset - self.base_offset) as u64;
        let mut file = self.file.write();
        file.seek(SeekFrom::Start(0))?;
        for i in 0..=local_index {
            let mut len_buf = [0u8; LENGTH_PREFIX_BYTES];
            if file.read_exact(&mut len_buf).is_err() {
                return Ok(None);
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            if len > MAX_RECORD_SIZE {
                return Err(ThorstreamError::Storage("Invalid record length".into()));
            }
            let mut buf = vec![0u8; len];
            if file.read_exact(&mut buf).is_err() {
                return Ok(None);
            }
            let record: Record = bincode::deserialize(&buf)
                .map_err(|e| ThorstreamError::Serialization(e.to_string()))?;
            if i == local_index {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    /// Read from `start_offset` (inclusive), up to `max_bytes` and `max_records`.
    /// Returns (records, next_offset).
    pub fn read_range(
        &self,
        start_offset: i64,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<(Vec<(i64, Record)>, i64)> {
        if start_offset < self.base_offset {
            return Err(ThorstreamError::InvalidOffset(start_offset));
        }
        let mut file = self.file.write();
        file.seek(SeekFrom::Start(0))?;
        let mut skip = start_offset - self.base_offset;
        let mut results = Vec::with_capacity(max_records.min(1024));
        let mut total_bytes = 0usize;
        let mut current_offset = self.base_offset;
        let mut next_offset = start_offset;

        loop {
            let mut len_buf = [0u8; LENGTH_PREFIX_BYTES];
            if file.read_exact(&mut len_buf).is_err() {
                break;
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            if len > MAX_RECORD_SIZE {
                return Err(ThorstreamError::Storage("Invalid record length".into()));
            }
            let mut buf = vec![0u8; len];
            file.read_exact(&mut buf)?;
            if skip > 0 {
                skip -= 1;
                current_offset += 1;
                continue;
            }
            let record: Record = bincode::deserialize(&buf)
                .map_err(|e| ThorstreamError::Serialization(e.to_string()))?;
            next_offset = current_offset + 1;
            total_bytes += LENGTH_PREFIX_BYTES + len;
            results.push((current_offset, record));
            current_offset += 1;
            if results.len() >= max_records || total_bytes >= max_bytes {
                break;
            }
        }
        Ok((results, next_offset))
    }

    pub fn base_offset(&self) -> i64 {
        self.base_offset
    }

    pub fn next_offset(&self) -> i64 {
        self.next_offset.load(Ordering::SeqCst)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn file_size_bytes(&self) -> Result<u64> {
        let file = self.file.read();
        Ok(file.metadata()?.len())
    }

    pub fn read_all(&self) -> Result<Vec<(i64, Record)>> {
        self.read_range(self.base_offset, usize::MAX, usize::MAX)
            .map(|(records, _)| records)
    }

    pub fn rewrite_all(&self, records: &[(i64, Record)]) -> Result<()> {
        let mut file = self.file.write();
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;

        let mut bytes_written: i64 = 0;
        let mut next = self.base_offset;
        for (_, record) in records {
            let bytes = bincode::serialize(record)
                .map_err(|e| ThorstreamError::Serialization(e.to_string()))?;
            if bytes.len() > MAX_RECORD_SIZE {
                return Err(ThorstreamError::Storage("Record too large".into()));
            }
            let len = bytes.len() as u32;
            file.write_all(&len.to_be_bytes())?;
            file.write_all(&bytes)?;
            bytes_written += LENGTH_PREFIX_BYTES as i64 + bytes.len() as i64;
            next += 1;
        }
        file.flush()?;
        self.write_position.store(bytes_written, Ordering::SeqCst);
        self.next_offset.store(next, Ordering::SeqCst);
        Ok(())
    }
}
