// Checkpoint metadata file management
//
// Format:
//   - 8 bytes: checkpoint_lsn
//   - 8 bytes: next_txn_id

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use crate::wal::Lsn;

const CHECKPOINT_META_FILE: &str = "checkpoint.meta";

#[derive(Debug, Clone, Default)]
pub struct CheckpointMeta {
    pub checkpoint_lsn: Option<Lsn>,
    pub next_txn_id: u64,
}

// Read checkpoint metadata from file
pub fn read_checkpoint_meta(data_dir: &str) -> std::io::Result<CheckpointMeta> {
    let path = Path::new(data_dir).join(CHECKPOINT_META_FILE);

    let mut file = match File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(CheckpointMeta::default());
        }
        Err(e) => return Err(e),
    };

    let mut lsn_buf = [0u8; 8];
    let mut txn_buf = [0u8; 8];

    if file.read_exact(&mut lsn_buf).is_err() {
        return Ok(CheckpointMeta::default());
    }

    let checkpoint_lsn = u64::from_le_bytes(lsn_buf);

    // Try to read next_txn_id (might not exist in old format)
    let next_txn_id = if file.read_exact(&mut txn_buf).is_ok() {
        u64::from_le_bytes(txn_buf)
    } else {
        1 // Default
    };

    Ok(CheckpointMeta {
        checkpoint_lsn: Some(checkpoint_lsn),
        next_txn_id,
    })
}

// Read checkpoint LSN from meta file (backward compatible)
#[allow(dead_code)]
pub fn read_checkpoint_lsn(data_dir: &str) -> std::io::Result<Option<Lsn>> {
    let meta = read_checkpoint_meta(data_dir)?;
    Ok(meta.checkpoint_lsn)
}

// Write checkpoint metadata to file
pub fn write_checkpoint_meta(data_dir: &str, lsn: Lsn, next_txn_id: u64) -> std::io::Result<()> {
    let path = Path::new(data_dir).join(CHECKPOINT_META_FILE);

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)?;

    file.write_all(&lsn.to_le_bytes())?;
    file.write_all(&next_txn_id.to_le_bytes())?;
    file.sync_all()?;

    Ok(())
}

// Delete checkpoint meta file (for --init)
pub fn delete_checkpoint_meta(data_dir: &str) -> std::io::Result<()> {
    let path = Path::new(data_dir).join(CHECKPOINT_META_FILE);
    match std::fs::remove_file(&path) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}
