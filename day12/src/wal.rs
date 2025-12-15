use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

use crate::executor::Rid;

pub type Lsn = u64;

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone)]
pub enum WalRecordType {
    Begin,
    Commit,
    Abort,
    Insert { rid: Rid, data: Vec<u8> },
    Delete { rid: Rid, data: Vec<u8> },
    // Compensation Log Record - written during undo
    CLR {
        undo_next_lsn: Lsn,  // Next LSN to undo (the prev_lsn of the undone record)
        redo: CLRRedo,       // What was done to compensate (for redo during recovery)
    },
}

// What the CLR did (for redo)
#[derive(Debug, Clone)]
pub enum CLRRedo {
    UndoInsert { rid: Rid },              // Undid an insert by deleting
    UndoDelete { rid: Rid, data: Vec<u8> }, // Undid a delete by restoring
}

// Record type tags for serialization
const TAG_BEGIN: u8 = 0;
const TAG_COMMIT: u8 = 1;
const TAG_ABORT: u8 = 2;
const TAG_INSERT: u8 = 3;
const TAG_DELETE: u8 = 4;
const TAG_CLR: u8 = 5;

// CLR redo type tags
const CLR_UNDO_INSERT: u8 = 0;
const CLR_UNDO_DELETE: u8 = 1;

#[derive(Debug, Clone)]
pub struct WalRecord {
    pub lsn: Lsn,
    pub txn_id: u64,
    pub prev_lsn: Lsn,  // Previous LSN for this transaction (0 = first record)
    pub record_type: WalRecordType,
}

impl WalRecord {
    // Serialize record to bytes
    // Format: [lsn:8][txn_id:8][prev_lsn:8][type:1][data_len:4][data:variable]
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // LSN (8 bytes)
        buf.extend_from_slice(&self.lsn.to_le_bytes());

        // Transaction ID (8 bytes)
        buf.extend_from_slice(&self.txn_id.to_le_bytes());

        // Previous LSN (8 bytes)
        buf.extend_from_slice(&self.prev_lsn.to_le_bytes());

        // Record type and data
        match &self.record_type {
            WalRecordType::Begin => {
                buf.push(TAG_BEGIN);
                buf.extend_from_slice(&0u32.to_le_bytes()); // data_len = 0
            }
            WalRecordType::Commit => {
                buf.push(TAG_COMMIT);
                buf.extend_from_slice(&0u32.to_le_bytes());
            }
            WalRecordType::Abort => {
                buf.push(TAG_ABORT);
                buf.extend_from_slice(&0u32.to_le_bytes());
            }
            WalRecordType::Insert { rid, data } => {
                buf.push(TAG_INSERT);
                // data_len = page_id(4) + slot_id(2) + data.len()
                let data_len = 4 + 2 + data.len();
                buf.extend_from_slice(&(data_len as u32).to_le_bytes());
                buf.extend_from_slice(&rid.page_id.to_le_bytes());
                buf.extend_from_slice(&rid.slot_id.to_le_bytes());
                buf.extend_from_slice(data);
            }
            WalRecordType::Delete { rid, data } => {
                buf.push(TAG_DELETE);
                let data_len = 4 + 2 + data.len();
                buf.extend_from_slice(&(data_len as u32).to_le_bytes());
                buf.extend_from_slice(&rid.page_id.to_le_bytes());
                buf.extend_from_slice(&rid.slot_id.to_le_bytes());
                buf.extend_from_slice(data);
            }
            WalRecordType::CLR { undo_next_lsn, redo } => {
                buf.push(TAG_CLR);
                match redo {
                    CLRRedo::UndoInsert { rid } => {
                        // data_len = undo_next_lsn(8) + redo_type(1) + page_id(4) + slot_id(2)
                        let data_len = 8 + 1 + 4 + 2;
                        buf.extend_from_slice(&(data_len as u32).to_le_bytes());
                        buf.extend_from_slice(&undo_next_lsn.to_le_bytes());
                        buf.push(CLR_UNDO_INSERT);
                        buf.extend_from_slice(&rid.page_id.to_le_bytes());
                        buf.extend_from_slice(&rid.slot_id.to_le_bytes());
                    }
                    CLRRedo::UndoDelete { rid, data } => {
                        // data_len = undo_next_lsn(8) + redo_type(1) + page_id(4) + slot_id(2) + data.len()
                        let data_len = 8 + 1 + 4 + 2 + data.len();
                        buf.extend_from_slice(&(data_len as u32).to_le_bytes());
                        buf.extend_from_slice(&undo_next_lsn.to_le_bytes());
                        buf.push(CLR_UNDO_DELETE);
                        buf.extend_from_slice(&rid.page_id.to_le_bytes());
                        buf.extend_from_slice(&rid.slot_id.to_le_bytes());
                        buf.extend_from_slice(data);
                    }
                }
            }
        }

        buf
    }
}

pub struct WalManager {
    writer: Mutex<BufWriter<File>>,
    current_lsn: AtomicU64,
    flushed_lsn: AtomicU64,
}

impl WalManager {
    pub fn new(path: &str) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        Ok(WalManager {
            writer: Mutex::new(BufWriter::new(file)),
            current_lsn: AtomicU64::new(1), // Start from 1, 0 means invalid
            flushed_lsn: AtomicU64::new(0),
        })
    }

    // Create WalManager and set current_lsn based on existing WAL records
    pub fn new_with_recovery(path: &str, max_lsn: Lsn) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        Ok(WalManager {
            writer: Mutex::new(BufWriter::new(file)),
            current_lsn: AtomicU64::new(max_lsn + 1),
            flushed_lsn: AtomicU64::new(max_lsn),
        })
    }

    // Append a WAL record and return its LSN
    pub fn append(&self, txn_id: u64, prev_lsn: Lsn, record_type: WalRecordType) -> Lsn {
        let lsn = self.current_lsn.fetch_add(1, Ordering::SeqCst);

        let record = WalRecord {
            lsn,
            txn_id,
            prev_lsn,
            record_type,
        };

        let data = record.serialize();

        let mut writer = self.writer.lock().unwrap();
        // Write record length first (for reading back)
        let len = data.len() as u32;
        writer.write_all(&len.to_le_bytes()).unwrap();
        writer.write_all(&data).unwrap();

        lsn
    }

    // Flush all pending WAL records to disk
    pub fn flush(&self) {
        let mut writer = self.writer.lock().unwrap();
        writer.flush().unwrap();
        writer.get_ref().sync_all().unwrap();

        // Update flushed_lsn to current
        let current = self.current_lsn.load(Ordering::SeqCst);
        self.flushed_lsn.store(current - 1, Ordering::SeqCst);
    }

    // Flush WAL up to the specified LSN
    pub fn flush_to(&self, lsn: Lsn) {
        if self.flushed_lsn.load(Ordering::SeqCst) >= lsn {
            return; // Already flushed
        }

        // For simplicity, flush everything
        self.flush();
    }

    #[allow(dead_code)]
    pub fn flushed_lsn(&self) -> Lsn {
        self.flushed_lsn.load(Ordering::SeqCst)
    }

    #[allow(dead_code)]
    pub fn current_lsn(&self) -> Lsn {
        self.current_lsn.load(Ordering::SeqCst)
    }
}

// Read all WAL records from file
pub fn read_wal_records(path: &str) -> std::io::Result<Vec<WalRecord>> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
        Err(e) => return Err(e),
    };

    let mut reader = BufReader::new(file);
    let mut records = Vec::new();

    loop {
        // Read record length
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        let len = u32::from_le_bytes(len_buf) as usize;

        // Read record data
        let mut data = vec![0u8; len];
        reader.read_exact(&mut data)?;

        // Parse record
        let lsn = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let txn_id = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let prev_lsn = u64::from_le_bytes(data[16..24].try_into().unwrap());
        let tag = data[24];
        let _data_len = u32::from_le_bytes(data[25..29].try_into().unwrap());

        let record_type = match tag {
            TAG_BEGIN => WalRecordType::Begin,
            TAG_COMMIT => WalRecordType::Commit,
            TAG_ABORT => WalRecordType::Abort,
            TAG_INSERT => {
                let page_id = u32::from_le_bytes(data[29..33].try_into().unwrap());
                let slot_id = u16::from_le_bytes(data[33..35].try_into().unwrap());
                let tuple_data = data[35..].to_vec();
                WalRecordType::Insert {
                    rid: Rid { page_id, slot_id },
                    data: tuple_data,
                }
            }
            TAG_DELETE => {
                let page_id = u32::from_le_bytes(data[29..33].try_into().unwrap());
                let slot_id = u16::from_le_bytes(data[33..35].try_into().unwrap());
                let tuple_data = data[35..].to_vec();
                WalRecordType::Delete {
                    rid: Rid { page_id, slot_id },
                    data: tuple_data,
                }
            }
            TAG_CLR => {
                let undo_next_lsn = u64::from_le_bytes(data[29..37].try_into().unwrap());
                let redo_type = data[37];
                let page_id = u32::from_le_bytes(data[38..42].try_into().unwrap());
                let slot_id = u16::from_le_bytes(data[42..44].try_into().unwrap());
                let redo = match redo_type {
                    CLR_UNDO_INSERT => CLRRedo::UndoInsert {
                        rid: Rid { page_id, slot_id },
                    },
                    CLR_UNDO_DELETE => {
                        let tuple_data = data[44..].to_vec();
                        CLRRedo::UndoDelete {
                            rid: Rid { page_id, slot_id },
                            data: tuple_data,
                        }
                    }
                    _ => continue,
                };
                WalRecordType::CLR { undo_next_lsn, redo }
            }
            _ => continue, // Unknown tag, skip
        };

        records.push(WalRecord {
            lsn,
            txn_id,
            prev_lsn,
            record_type,
        });
    }

    Ok(records)
}
