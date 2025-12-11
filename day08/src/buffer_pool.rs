use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::{bail, Result};
use indexmap::IndexSet;

use crate::disk::DiskManager;
use crate::page::{Page, PAGE_SIZE};

const BUFFER_POOL_SIZE: usize = 100;

// Replacer trait for page replacement policies
pub trait Replacer {
    // Find a victim frame to evict
    fn victim(&self) -> Option<usize>;
    // Mark a frame as pinned (not evictable)
    fn pin(&mut self, frame_id: usize);
    // Mark a frame as unpinned (evictable)
    fn unpin(&mut self, frame_id: usize);
}

pub struct LruReplacer {
    order: IndexSet<usize>, // Maintains insertion order, O(1) insert/remove
    pinned: Vec<bool>,
}

impl LruReplacer {
    pub fn new(size: usize) -> Self {
        LruReplacer {
            order: IndexSet::new(),
            pinned: vec![true; size], // Initially all frames are considered pinned (empty)
        }
    }
}

impl Replacer for LruReplacer {
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

struct Frame {
    page: Arc<RwLock<Page>>,
    page_id: Option<u32>,
    pin_count: u32,
    is_dirty: bool,
}

impl Frame {
    fn new() -> Self {
        Frame {
            page: Arc::new(RwLock::new(Page::new(0))),
            page_id: None,
            pin_count: 0,
            is_dirty: false,
        }
    }
}

pub struct BufferPoolManager<R: Replacer = LruReplacer> {
    frames: Vec<Frame>,
    page_table: HashMap<u32, usize>, // page_id -> frame_id
    disk_manager: DiskManager,
    replacer: R,
}

impl BufferPoolManager<LruReplacer> {
    pub fn new(disk_manager: DiskManager) -> Self {
        Self::with_replacer(disk_manager, LruReplacer::new(BUFFER_POOL_SIZE))
    }
}

impl<R: Replacer> BufferPoolManager<R> {
    pub fn with_replacer(disk_manager: DiskManager, replacer: R) -> Self {
        let mut frames = Vec::with_capacity(BUFFER_POOL_SIZE);
        for _ in 0..BUFFER_POOL_SIZE {
            frames.push(Frame::new());
        }
        BufferPoolManager {
            frames,
            page_table: HashMap::new(),
            disk_manager,
            replacer,
        }
    }

    fn evict(&mut self, frame_id: usize) -> Result<()> {
        let frame = &mut self.frames[frame_id];
        if let Some(old_page_id) = frame.page_id {
            println!(
                "[BufferPool] Evicting page {old_page_id} (dirty: {})",
                frame.is_dirty
            );
            if frame.is_dirty {
                let page_guard = frame.page.read().unwrap();
                self.disk_manager.write_page(old_page_id, &page_guard.data)?;
            }
            self.page_table.remove(&old_page_id);
        }
        frame.page_id = None;
        frame.is_dirty = false;
        frame.pin_count = 0;
        Ok(())
    }

    fn find_or_create_frame(&mut self) -> Result<usize> {
        if self.page_table.len() < BUFFER_POOL_SIZE {
            // Find empty frame
            Ok(self
                .frames
                .iter()
                .position(|f| f.page_id.is_none())
                .unwrap())
        } else {
            // Need to evict
            let victim = self
                .replacer
                .victim()
                .ok_or_else(|| anyhow::anyhow!("no victim frame"))?;
            self.evict(victim)?;
            Ok(victim)
        }
    }

    fn fetch_page_internal(&mut self, page_id: u32, is_dirty: bool) -> Result<Arc<RwLock<Page>>> {
        // Check if already in buffer pool
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            self.frames[frame_id].pin_count += 1;
            if is_dirty {
                self.frames[frame_id].is_dirty = true;
            }
            self.replacer.pin(frame_id);
            return Ok(Arc::clone(&self.frames[frame_id].page));
        }

        // Find a frame to use
        let frame_id = self.find_or_create_frame()?;

        // Read page from disk
        println!("[BufferPool] Fetching page {page_id} from disk");
        let mut data = [0u8; PAGE_SIZE];
        self.disk_manager.read_page(page_id, &mut data)?;

        let frame = &mut self.frames[frame_id];
        *frame.page.write().unwrap() = Page::from_bytes(&data);
        frame.page_id = Some(page_id);
        frame.pin_count = 1;
        frame.is_dirty = is_dirty;

        self.page_table.insert(page_id, frame_id);
        self.replacer.pin(frame_id);

        Ok(Arc::clone(&self.frames[frame_id].page))
    }

    pub fn fetch_page(&mut self, page_id: u32) -> Result<Arc<RwLock<Page>>> {
        self.fetch_page_internal(page_id, false)
    }

    pub fn fetch_page_mut(&mut self, page_id: u32) -> Result<Arc<RwLock<Page>>> {
        self.fetch_page_internal(page_id, true)
    }

    pub fn new_page(&mut self) -> Result<(u32, Arc<RwLock<Page>>)> {
        let frame_id = self.find_or_create_frame()?;
        let page_id = self.disk_manager.allocate_page()?;
        println!("[BufferPool] Allocating new page {page_id}");

        let frame = &mut self.frames[frame_id];
        *frame.page.write().unwrap() = Page::new(page_id);
        frame.page_id = Some(page_id);
        frame.pin_count = 1;
        frame.is_dirty = true;

        self.page_table.insert(page_id, frame_id);
        self.replacer.pin(frame_id);

        Ok((page_id, Arc::clone(&self.frames[frame_id].page)))
    }

    pub fn unpin_page(&mut self, page_id: u32, is_dirty: bool) -> Result<()> {
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            let frame = &mut self.frames[frame_id];
            if frame.pin_count == 0 {
                bail!("page {page_id} is not pinned");
            }
            frame.pin_count -= 1;
            if frame.pin_count == 0 {
                self.replacer.unpin(frame_id);
            }
            if is_dirty {
                frame.is_dirty = true;
            }
            Ok(())
        } else {
            bail!("page {page_id} not in buffer pool");
        }
    }

    pub fn flush_all(&mut self) -> Result<()> {
        let page_ids: Vec<u32> = self.page_table.keys().cloned().collect();
        for page_id in page_ids {
            let frame_id = self.page_table[&page_id];
            let frame = &mut self.frames[frame_id];
            if frame.is_dirty {
                let page_guard = frame.page.read().unwrap();
                self.disk_manager.write_page(page_id, &page_guard.data)?;
                drop(page_guard);
                frame.is_dirty = false;
            }
        }
        Ok(())
    }

    pub fn page_count(&self) -> u32 {
        self.disk_manager.page_count()
    }
}
