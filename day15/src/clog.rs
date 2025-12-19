// CLOG (Commit Log) - Page-based Transaction Status Management
//
// Each transaction uses 2 bits for status:
//   00 = InProgress (not stored, default)
//   01 = Committed
//   10 = Aborted
//
// Page structure:
//   - Page size: 4KB (4096 bytes)
//   - Transactions per page: 4096 * 4 = 16384
//   - Page ID = txn_id / 16384
//   - Offset in page = (txn_id % 16384) / 4
//   - Bit position = (txn_id % 4) * 2
//
// File structure:
//   - Single file: clog.db
//   - Pages are stored sequentially (like heap pages)

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use indexmap::IndexSet;

use crate::transaction_manager::TxnStatus;
use crate::tuple::TxnId;

const CLOG_FILE: &str = "clog.db";
const PAGE_SIZE: usize = 4096;
const TXNS_PER_PAGE: u64 = (PAGE_SIZE * 4) as u64; // 16384
const BUFFER_SIZE: usize = 8; // Number of pages to keep in memory

// Status bit values (2 bits)
const STATUS_IN_PROGRESS: u8 = 0b00;
const STATUS_COMMITTED: u8 = 0b01;
const STATUS_ABORTED: u8 = 0b10;

struct LruReplacer {
    order: IndexSet<usize>, // Maintains insertion order, O(1) insert/remove
    pinned: Vec<bool>,
}

impl LruReplacer {
    fn new(size: usize) -> Self {
        LruReplacer {
            order: IndexSet::new(),
            pinned: vec![true; size], // Initially all frames are pinned (empty)
        }
    }

    fn victim(&self) -> Option<usize> {
        // Find first unpinned frame (oldest in order)
        self.order.iter().find(|&&id| !self.pinned[id]).copied()
    }

    fn pin(&mut self, frame_id: usize) {
        self.pinned[frame_id] = true;
        // Move to end (most recently used)
        self.order.shift_remove(&frame_id);
        self.order.insert(frame_id);
    }

    fn unpin(&mut self, frame_id: usize) {
        self.pinned[frame_id] = false;
    }
}

/// Frame for CLOG page
struct ClogFrame {
    data: [u8; PAGE_SIZE],
    page_id: Option<u64>,
    is_dirty: bool,
}

impl ClogFrame {
    fn new() -> Self {
        ClogFrame {
            data: [0u8; PAGE_SIZE],
            page_id: None,
            is_dirty: false,
        }
    }
}

/// Page-based CLOG with buffer pool
pub struct Clog {
    inner: Mutex<ClogInner>,
}

struct ClogInner {
    file_path: PathBuf,
    frames: Vec<ClogFrame>,
    page_table: std::collections::HashMap<u64, usize>, // page_id -> frame_id
    replacer: LruReplacer,
    page_count: u64, // Number of pages in file
}

impl Clog {
    pub fn new(data_dir: &str) -> std::io::Result<Self> {
        let file_path = Path::new(data_dir).join(CLOG_FILE);

        // Get current page count from file size
        let page_count = if file_path.exists() {
            let metadata = std::fs::metadata(&file_path)?;
            (metadata.len() / PAGE_SIZE as u64) as u64
        } else {
            0
        };

        let mut frames = Vec::with_capacity(BUFFER_SIZE);
        for _ in 0..BUFFER_SIZE {
            frames.push(ClogFrame::new());
        }

        Ok(Clog {
            inner: Mutex::new(ClogInner {
                file_path,
                frames,
                page_table: std::collections::HashMap::new(),
                replacer: LruReplacer::new(BUFFER_SIZE),
                page_count,
            }),
        })
    }

    /// Get transaction status
    pub fn get_status(&self, txn_id: TxnId) -> TxnStatus {
        let page_id = txn_id / TXNS_PER_PAGE;

        let mut inner = self.inner.lock().unwrap();

        // Fetch page (creates if doesn't exist)
        let frame_id = match inner.fetch_page(page_id) {
            Ok(id) => id,
            Err(_) => return TxnStatus::InProgress, // Default
        };

        let offset = ((txn_id % TXNS_PER_PAGE) / 4) as usize;
        let bit_pos = ((txn_id % 4) * 2) as u8;
        let status_bits = (inner.frames[frame_id].data[offset] >> bit_pos) & 0b11;

        // Unpin after read
        inner.unpin(frame_id);

        match status_bits {
            STATUS_COMMITTED => TxnStatus::Committed,
            STATUS_ABORTED => TxnStatus::Aborted,
            _ => TxnStatus::InProgress,
        }
    }

    /// Set transaction status
    pub fn set_status(&self, txn_id: TxnId, status: TxnStatus) {
        let page_id = txn_id / TXNS_PER_PAGE;

        let mut inner = self.inner.lock().unwrap();

        // Fetch page (creates if doesn't exist)
        let frame_id = match inner.fetch_page_mut(page_id) {
            Ok(id) => id,
            Err(_) => return,
        };

        let offset = ((txn_id % TXNS_PER_PAGE) / 4) as usize;
        let bit_pos = ((txn_id % 4) * 2) as u8;

        let status_bits = match status {
            TxnStatus::Committed => STATUS_COMMITTED,
            TxnStatus::Aborted => STATUS_ABORTED,
            TxnStatus::InProgress => STATUS_IN_PROGRESS,
        };

        // Clear the 2 bits and set new value
        inner.frames[frame_id].data[offset] &= !(0b11 << bit_pos);
        inner.frames[frame_id].data[offset] |= status_bits << bit_pos;
        inner.frames[frame_id].is_dirty = true;

        // Unpin after write
        inner.unpin(frame_id);
    }

    /// Flush all dirty pages to disk (called at checkpoint)
    pub fn flush(&self) -> std::io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.flush_all()
    }
}

impl ClogInner {
    fn fetch_page(&mut self, page_id: u64) -> std::io::Result<usize> {
        self.fetch_page_internal(page_id, false)
    }

    fn fetch_page_mut(&mut self, page_id: u64) -> std::io::Result<usize> {
        self.fetch_page_internal(page_id, true)
    }

    fn fetch_page_internal(&mut self, page_id: u64, is_dirty: bool) -> std::io::Result<usize> {
        // Check if already in buffer
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            if is_dirty {
                self.frames[frame_id].is_dirty = true;
            }
            self.replacer.pin(frame_id);
            return Ok(frame_id);
        }

        // Find a frame to use
        let frame_id = self.find_or_create_frame()?;

        // Load page from disk or create new
        if page_id < self.page_count {
            // Read from disk
            let mut file = File::open(&self.file_path)?;
            file.seek(SeekFrom::Start(page_id * PAGE_SIZE as u64))?;
            file.read_exact(&mut self.frames[frame_id].data)?;
        } else {
            // New page - zero initialize
            self.frames[frame_id].data = [0u8; PAGE_SIZE];
            // Extend page count
            self.page_count = page_id + 1;
        }

        self.frames[frame_id].page_id = Some(page_id);
        self.frames[frame_id].is_dirty = is_dirty;
        self.page_table.insert(page_id, frame_id);
        self.replacer.pin(frame_id);

        Ok(frame_id)
    }

    fn find_or_create_frame(&mut self) -> std::io::Result<usize> {
        // Find empty frame
        if let Some(frame_id) = self.frames.iter().position(|f| f.page_id.is_none()) {
            return Ok(frame_id);
        }

        // Need to evict
        let victim = self
            .replacer
            .victim()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "no victim frame"))?;
        self.evict(victim)?;
        Ok(victim)
    }

    fn evict(&mut self, frame_id: usize) -> std::io::Result<()> {
        let (old_page_id, is_dirty, data) = {
            let frame = &self.frames[frame_id];
            (frame.page_id, frame.is_dirty, frame.data)
        };

        if let Some(page_id) = old_page_id {
            if is_dirty {
                Self::write_page_to_file(&self.file_path, page_id, &data)?;
            }
            self.page_table.remove(&page_id);
        }

        self.frames[frame_id].page_id = None;
        self.frames[frame_id].is_dirty = false;
        Ok(())
    }

    fn unpin(&mut self, frame_id: usize) {
        self.replacer.unpin(frame_id);
    }

    fn flush_all(&mut self) -> std::io::Result<()> {
        // Collect dirty pages first to avoid borrow issues
        let dirty_pages: Vec<(u64, [u8; PAGE_SIZE])> = self
            .frames
            .iter()
            .filter_map(|frame| {
                if frame.is_dirty {
                    frame.page_id.map(|pid| (pid, frame.data))
                } else {
                    None
                }
            })
            .collect();

        // Write all dirty pages
        for (page_id, data) in dirty_pages {
            Self::write_page_to_file(&self.file_path, page_id, &data)?;
        }

        // Clear dirty flags
        for frame in &mut self.frames {
            frame.is_dirty = false;
        }

        Ok(())
    }

    fn write_page_to_file(file_path: &Path, page_id: u64, data: &[u8; PAGE_SIZE]) -> std::io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(file_path)?;
        file.seek(SeekFrom::Start(page_id * PAGE_SIZE as u64))?;
        file.write_all(data)?;
        file.sync_all()?;
        Ok(())
    }
}

/// Delete CLOG file (for --init)
pub fn delete_clog(data_dir: &str) -> std::io::Result<()> {
    let file_path = Path::new(data_dir).join(CLOG_FILE);
    match std::fs::remove_file(&file_path) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}
