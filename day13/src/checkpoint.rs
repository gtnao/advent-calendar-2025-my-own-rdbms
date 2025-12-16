// Checkpoint metadata file management

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use crate::wal::Lsn;

const CHECKPOINT_META_FILE: &str = "checkpoint.meta";

// Read checkpoint LSN from meta file
// Returns None if file doesn't exist or is invalid
pub fn read_checkpoint_lsn(data_dir: &str) -> std::io::Result<Option<Lsn>> {
    let path = Path::new(data_dir).join(CHECKPOINT_META_FILE);

    let mut file = match File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };

    let mut buf = [0u8; 8];
    match file.read_exact(&mut buf) {
        Ok(_) => Ok(Some(u64::from_le_bytes(buf))),
        Err(_) => Ok(None), // Invalid file, treat as no checkpoint
    }
}

// Write checkpoint LSN to meta file
pub fn write_checkpoint_lsn(data_dir: &str, lsn: Lsn) -> std::io::Result<()> {
    let path = Path::new(data_dir).join(CHECKPOINT_META_FILE);

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)?;

    file.write_all(&lsn.to_le_bytes())?;
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
