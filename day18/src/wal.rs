use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

use crate::executor::Rid;

pub type Lsn = u64;

// Maximum records per segment (for simplicity, using record count instead of byte size)
const MAX_RECORDS_PER_SEGMENT: usize = 1000;

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone)]
pub enum WalRecordType {
    Begin,
    Commit,
    Abort,
    Insert { rid: Rid, data: Vec<u8> },
    // MVCC logical delete: set xmax to mark tuple as deleted
    Delete { rid: Rid, xmax: u64 },
    // Compensation Log Record - written during undo
    CLR {
        undo_next_lsn: Lsn,
        redo: CLRRedo,
    },
    // Checkpoint record with ATT and DPT snapshots
    Checkpoint {
        att: HashMap<u64, Lsn>,  // txn_id -> last_lsn
        dpt: HashMap<u32, Lsn>,  // page_id -> rec_lsn
    },
    // Allocate a new page for a table
    AllocatePage {
        page_id: u32,
        table_id: u32,
        prev_page_id: u32,  // u32::MAX means no previous page (first page of table)
    },
}

#[derive(Debug, Clone)]
pub enum CLRRedo {
    // Undo INSERT: physically delete the tuple
    UndoInsert { rid: Rid },
    // Undo DELETE: reset xmax to restore visibility
    UndoDelete { rid: Rid, old_xmax: u64 },
}

// Record type tags for serialization
const TAG_BEGIN: u8 = 0;
const TAG_COMMIT: u8 = 1;
const TAG_ABORT: u8 = 2;
const TAG_INSERT: u8 = 3;
const TAG_DELETE: u8 = 4;
const TAG_CLR: u8 = 5;
const TAG_CHECKPOINT: u8 = 6;
const TAG_ALLOCATE_PAGE: u8 = 7;

// CLR redo type tags
const CLR_UNDO_INSERT: u8 = 0;
const CLR_UNDO_DELETE: u8 = 1;

#[derive(Debug, Clone)]
pub struct WalRecord {
    pub lsn: Lsn,
    pub txn_id: u64,
    pub prev_lsn: Lsn,
    pub record_type: WalRecordType,
}

impl WalRecord {
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        buf.extend_from_slice(&self.lsn.to_le_bytes());
        buf.extend_from_slice(&self.txn_id.to_le_bytes());
        buf.extend_from_slice(&self.prev_lsn.to_le_bytes());

        match &self.record_type {
            WalRecordType::Begin => {
                buf.push(TAG_BEGIN);
                buf.extend_from_slice(&0u32.to_le_bytes());
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
                let data_len = 4 + 2 + data.len();
                buf.extend_from_slice(&(data_len as u32).to_le_bytes());
                buf.extend_from_slice(&rid.page_id.to_le_bytes());
                buf.extend_from_slice(&rid.slot_id.to_le_bytes());
                buf.extend_from_slice(data);
            }
            WalRecordType::Delete { rid, xmax } => {
                buf.push(TAG_DELETE);
                let data_len = 4 + 2 + 8; // page_id + slot_id + xmax
                buf.extend_from_slice(&(data_len as u32).to_le_bytes());
                buf.extend_from_slice(&rid.page_id.to_le_bytes());
                buf.extend_from_slice(&rid.slot_id.to_le_bytes());
                buf.extend_from_slice(&xmax.to_le_bytes());
            }
            WalRecordType::CLR { undo_next_lsn, redo } => {
                buf.push(TAG_CLR);
                match redo {
                    CLRRedo::UndoInsert { rid } => {
                        let data_len = 8 + 1 + 4 + 2;
                        buf.extend_from_slice(&(data_len as u32).to_le_bytes());
                        buf.extend_from_slice(&undo_next_lsn.to_le_bytes());
                        buf.push(CLR_UNDO_INSERT);
                        buf.extend_from_slice(&rid.page_id.to_le_bytes());
                        buf.extend_from_slice(&rid.slot_id.to_le_bytes());
                    }
                    CLRRedo::UndoDelete { rid, old_xmax } => {
                        let data_len = 8 + 1 + 4 + 2 + 8; // undo_next_lsn + tag + page_id + slot_id + old_xmax
                        buf.extend_from_slice(&(data_len as u32).to_le_bytes());
                        buf.extend_from_slice(&undo_next_lsn.to_le_bytes());
                        buf.push(CLR_UNDO_DELETE);
                        buf.extend_from_slice(&rid.page_id.to_le_bytes());
                        buf.extend_from_slice(&rid.slot_id.to_le_bytes());
                        buf.extend_from_slice(&old_xmax.to_le_bytes());
                    }
                }
            }
            WalRecordType::Checkpoint { att, dpt } => {
                buf.push(TAG_CHECKPOINT);
                // Serialize ATT and DPT
                // Format: [att_count:4][att_entries: (txn_id:8, last_lsn:8)*][dpt_count:4][dpt_entries: (page_id:4, rec_lsn:8)*]
                let att_size = 4 + att.len() * 16;
                let dpt_size = 4 + dpt.len() * 12;
                let data_len = att_size + dpt_size;
                buf.extend_from_slice(&(data_len as u32).to_le_bytes());

                // ATT
                buf.extend_from_slice(&(att.len() as u32).to_le_bytes());
                for (&txn_id, &last_lsn) in att {
                    buf.extend_from_slice(&txn_id.to_le_bytes());
                    buf.extend_from_slice(&last_lsn.to_le_bytes());
                }

                // DPT
                buf.extend_from_slice(&(dpt.len() as u32).to_le_bytes());
                for (&page_id, &rec_lsn) in dpt {
                    buf.extend_from_slice(&page_id.to_le_bytes());
                    buf.extend_from_slice(&rec_lsn.to_le_bytes());
                }
            }
            WalRecordType::AllocatePage { page_id, table_id, prev_page_id } => {
                buf.push(TAG_ALLOCATE_PAGE);
                let data_len = 4 + 4 + 4; // page_id + table_id + prev_page_id
                buf.extend_from_slice(&(data_len as u32).to_le_bytes());
                buf.extend_from_slice(&page_id.to_le_bytes());
                buf.extend_from_slice(&table_id.to_le_bytes());
                buf.extend_from_slice(&prev_page_id.to_le_bytes());
            }
        }

        buf
    }
}

// WAL Manager with segment support
pub struct WalManager {
    wal_dir: PathBuf,
    current_segment: Mutex<SegmentWriter>,
    current_lsn: AtomicU64,
    flushed_lsn: AtomicU64,
}

struct SegmentWriter {
    segment_id: u64,
    writer: BufWriter<File>,
    record_count: usize,
}

impl WalManager {
    pub fn new(wal_dir: &str) -> std::io::Result<Self> {
        // Create WAL directory if not exists
        fs::create_dir_all(wal_dir)?;

        // Find existing segments and determine starting point
        let (max_segment_id, max_lsn) = Self::scan_existing_segments(wal_dir)?;

        let next_segment_id = if max_segment_id == 0 { 1 } else { max_segment_id + 1 };
        let next_lsn = if max_lsn == 0 { 1 } else { max_lsn + 1 };

        let segment_path = Self::segment_path(wal_dir, next_segment_id);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&segment_path)?;

        Ok(WalManager {
            wal_dir: PathBuf::from(wal_dir),
            current_segment: Mutex::new(SegmentWriter {
                segment_id: next_segment_id,
                writer: BufWriter::new(file),
                record_count: 0,
            }),
            current_lsn: AtomicU64::new(next_lsn),
            flushed_lsn: AtomicU64::new(if max_lsn == 0 { 0 } else { max_lsn }),
        })
    }

    pub fn new_for_init(wal_dir: &str) -> std::io::Result<Self> {
        // Remove existing WAL directory and create fresh
        let _ = fs::remove_dir_all(wal_dir);
        fs::create_dir_all(wal_dir)?;

        let segment_path = Self::segment_path(wal_dir, 1);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&segment_path)?;

        Ok(WalManager {
            wal_dir: PathBuf::from(wal_dir),
            current_segment: Mutex::new(SegmentWriter {
                segment_id: 1,
                writer: BufWriter::new(file),
                record_count: 0,
            }),
            current_lsn: AtomicU64::new(1),
            flushed_lsn: AtomicU64::new(0),
        })
    }

    fn segment_path(wal_dir: &str, segment_id: u64) -> PathBuf {
        Path::new(wal_dir).join(format!("wal_{:06}.log", segment_id))
    }

    fn scan_existing_segments(wal_dir: &str) -> std::io::Result<(u64, Lsn)> {
        let mut max_segment_id = 0u64;
        let mut max_lsn = 0u64;

        if let Ok(entries) = fs::read_dir(wal_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if name_str.starts_with("wal_") && name_str.ends_with(".log") {
                    if let Ok(id) = name_str[4..10].parse::<u64>() {
                        max_segment_id = max_segment_id.max(id);

                        // Read segment to find max LSN
                        if let Ok(records) = read_segment_file(&entry.path()) {
                            for record in records {
                                max_lsn = max_lsn.max(record.lsn);
                            }
                        }
                    }
                }
            }
        }

        Ok((max_segment_id, max_lsn))
    }

    pub fn append(&self, txn_id: u64, prev_lsn: Lsn, record_type: WalRecordType) -> Lsn {
        let lsn = self.current_lsn.fetch_add(1, Ordering::SeqCst);

        let record = WalRecord {
            lsn,
            txn_id,
            prev_lsn,
            record_type,
        };

        let data = record.serialize();

        let mut segment = self.current_segment.lock().unwrap();

        // Check if we need to rotate to a new segment
        if segment.record_count >= MAX_RECORDS_PER_SEGMENT {
            // Flush current segment
            segment.writer.flush().unwrap();
            segment.writer.get_ref().sync_all().unwrap();

            // Create new segment
            let new_segment_id = segment.segment_id + 1;
            let new_path = Self::segment_path(self.wal_dir.to_str().unwrap(), new_segment_id);
            let new_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&new_path)
                .unwrap();

            segment.segment_id = new_segment_id;
            segment.writer = BufWriter::new(new_file);
            segment.record_count = 0;
        }

        // Write record
        let len = data.len() as u32;
        segment.writer.write_all(&len.to_le_bytes()).unwrap();
        segment.writer.write_all(&data).unwrap();
        segment.record_count += 1;

        lsn
    }

    pub fn flush(&self) {
        let mut segment = self.current_segment.lock().unwrap();
        segment.writer.flush().unwrap();
        segment.writer.get_ref().sync_all().unwrap();

        let current = self.current_lsn.load(Ordering::SeqCst);
        self.flushed_lsn.store(current - 1, Ordering::SeqCst);
    }

    pub fn flush_to(&self, lsn: Lsn) {
        if self.flushed_lsn.load(Ordering::SeqCst) >= lsn {
            return;
        }
        self.flush();
    }

    #[allow(dead_code)]
    pub fn flushed_lsn(&self) -> Lsn {
        self.flushed_lsn.load(Ordering::SeqCst)
    }

    // Delete segments that are no longer needed
    pub fn delete_segments_before(&self, min_lsn: Lsn) -> std::io::Result<usize> {
        let mut deleted_count = 0;
        let wal_dir = self.wal_dir.to_str().unwrap();

        if let Ok(entries) = fs::read_dir(wal_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let name = entry.file_name();
                let name_str = name.to_string_lossy();

                if name_str.starts_with("wal_") && name_str.ends_with(".log") {
                    // Read first record to check if segment is safe to delete
                    if let Ok(records) = read_segment_file(&path) {
                        if !records.is_empty() {
                            let max_lsn_in_segment = records.iter().map(|r| r.lsn).max().unwrap_or(0);
                            if max_lsn_in_segment < min_lsn {
                                fs::remove_file(&path)?;
                                deleted_count += 1;
                            }
                        }
                    }
                }
            }
        }

        Ok(deleted_count)
    }
}

// Read records from a single segment file
fn read_segment_file(path: &Path) -> std::io::Result<Vec<WalRecord>> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
        Err(e) => return Err(e),
    };

    let mut reader = BufReader::new(file);
    let mut records = Vec::new();

    loop {
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        let len = u32::from_le_bytes(len_buf) as usize;

        let mut data = vec![0u8; len];
        reader.read_exact(&mut data)?;

        if let Some(record) = parse_record(&data) {
            records.push(record);
        }
    }

    Ok(records)
}

fn parse_record(data: &[u8]) -> Option<WalRecord> {
    if data.len() < 29 {
        return None;
    }

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
            let xmax = u64::from_le_bytes(data[35..43].try_into().unwrap());
            WalRecordType::Delete {
                rid: Rid { page_id, slot_id },
                xmax,
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
                    let old_xmax = u64::from_le_bytes(data[44..52].try_into().unwrap());
                    CLRRedo::UndoDelete {
                        rid: Rid { page_id, slot_id },
                        old_xmax,
                    }
                }
                _ => return None,
            };
            WalRecordType::CLR { undo_next_lsn, redo }
        }
        TAG_CHECKPOINT => {
            let mut offset = 29;

            // Read ATT
            let att_count = u32::from_le_bytes(data[offset..offset+4].try_into().unwrap()) as usize;
            offset += 4;
            let mut att = HashMap::new();
            for _ in 0..att_count {
                let txn_id = u64::from_le_bytes(data[offset..offset+8].try_into().unwrap());
                offset += 8;
                let last_lsn = u64::from_le_bytes(data[offset..offset+8].try_into().unwrap());
                offset += 8;
                att.insert(txn_id, last_lsn);
            }

            // Read DPT
            let dpt_count = u32::from_le_bytes(data[offset..offset+4].try_into().unwrap()) as usize;
            offset += 4;
            let mut dpt = HashMap::new();
            for _ in 0..dpt_count {
                let page_id = u32::from_le_bytes(data[offset..offset+4].try_into().unwrap());
                offset += 4;
                let rec_lsn = u64::from_le_bytes(data[offset..offset+8].try_into().unwrap());
                offset += 8;
                dpt.insert(page_id, rec_lsn);
            }

            WalRecordType::Checkpoint { att, dpt }
        }
        TAG_ALLOCATE_PAGE => {
            let page_id = u32::from_le_bytes(data[29..33].try_into().unwrap());
            let table_id = u32::from_le_bytes(data[33..37].try_into().unwrap());
            let prev_page_id = u32::from_le_bytes(data[37..41].try_into().unwrap());
            WalRecordType::AllocatePage { page_id, table_id, prev_page_id }
        }
        _ => return None,
    };

    Some(WalRecord {
        lsn,
        txn_id,
        prev_lsn,
        record_type,
    })
}

// Read all WAL records from all segments, optionally starting from a specific LSN
pub fn read_wal_records_from(wal_dir: &str, start_lsn: Option<Lsn>) -> std::io::Result<Vec<WalRecord>> {
    let mut all_records = Vec::new();
    let mut segment_files: Vec<PathBuf> = Vec::new();

    if let Ok(entries) = fs::read_dir(wal_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with("wal_") && name_str.ends_with(".log") {
                segment_files.push(entry.path());
            }
        }
    }

    // Sort segments by name (which gives us chronological order)
    segment_files.sort();

    // If start_lsn is specified, find the first segment that might contain it
    for segment_path in &segment_files {

        let records = read_segment_file(segment_path)?;

        if let Some(start) = start_lsn {
            // Check if this segment might contain relevant records
            if let Some(first_lsn) = records.first().map(|r| r.lsn) {
                if let Some(last_lsn) = records.last().map(|r| r.lsn) {
                    // Skip segment if all records are before start_lsn
                    if last_lsn < start {
                        continue;
                    }
                    // If first record is after start_lsn, we've found our starting segment
                    if first_lsn >= start {
                        all_records.extend(records);
                        continue;
                    }
                }
            }
            // Partial segment - filter records
            all_records.extend(records.into_iter().filter(|r| r.lsn >= start));
        } else {
            all_records.extend(records);
        }
    }

    Ok(all_records)
}
