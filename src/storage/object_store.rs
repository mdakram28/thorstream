use crate::error::{Result, ThorstreamError};
use crate::types::Record;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

const SEGMENT_FILE: &str = "00000000000000000000.log";

fn object_store_root() -> Option<PathBuf> {
    std::env::var("THORSTREAM_OBJECT_STORE_DIR")
        .ok()
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

fn object_segment_path(topic: &str, partition: i32) -> Option<PathBuf> {
    object_store_root().map(|root| {
        root.join(topic)
            .join(partition.to_string())
            .join(SEGMENT_FILE)
    })
}

fn strict_mode() -> bool {
    std::env::var("THORSTREAM_OBJECT_STORE_REQUIRED")
        .ok()
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

pub fn restore_segment_if_needed(
    topic: &str,
    partition: i32,
    local_segment_path: &Path,
) -> Result<()> {
    let Some(remote) = object_segment_path(topic, partition) else {
        return Ok(());
    };

    let local_needs_restore = match std::fs::metadata(local_segment_path) {
        Ok(meta) => meta.len() == 0,
        Err(_) => true,
    };

    if !local_needs_restore || !remote.exists() {
        return Ok(());
    }

    if let Some(parent) = local_segment_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    std::fs::copy(&remote, local_segment_path).map_err(|e| {
        ThorstreamError::Storage(format!("restore from object store failed: {}", e))
    })?;
    Ok(())
}

pub fn append_record(topic: &str, partition: i32, record: &Record) -> Result<()> {
    let Some(remote) = object_segment_path(topic, partition) else {
        return Ok(());
    };

    if let Some(parent) = remote.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let bytes =
        bincode::serialize(record).map_err(|e| ThorstreamError::Serialization(e.to_string()))?;
    let len = bytes.len() as u32;

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&remote)
        .map_err(|e| ThorstreamError::Storage(format!("object store append open failed: {}", e)))?;

    file.write_all(&len.to_be_bytes())
        .map_err(|e| ThorstreamError::Storage(format!("object store append len failed: {}", e)))?;
    file.write_all(&bytes).map_err(|e| {
        ThorstreamError::Storage(format!("object store append payload failed: {}", e))
    })?;
    file.flush().map_err(|e| {
        ThorstreamError::Storage(format!("object store append flush failed: {}", e))
    })?;
    Ok(())
}

pub fn mirror_append(topic: &str, partition: i32, record: &Record) -> Result<()> {
    match append_record(topic, partition, record) {
        Ok(()) => Ok(()),
        Err(e) if strict_mode() => Err(e),
        Err(e) => {
            tracing::warn!(topic, partition, error = %e, "object store mirror append failed (best-effort)");
            Ok(())
        }
    }
}
