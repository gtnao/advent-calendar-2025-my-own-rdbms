use std::collections::HashMap;

use anyhow::{bail, Result};

use crate::disk::DiskManager;
use crate::page::{Page, PAGE_SIZE};

const BUFFER_POOL_SIZE: usize = 3; // small for testing

struct Frame {
    page: Page,
    page_id: Option<u32>,
    pin_count: u32,
    is_dirty: bool,
}

impl Frame {
    fn new() -> Self {
        Frame {
            page: Page::new(0),
            page_id: None,
            pin_count: 0,
            is_dirty: false,
        }
    }
}

pub struct BufferPoolManager {
    frames: Vec<Frame>,
    page_table: HashMap<u32, usize>, // page_id -> frame_id
    disk_manager: DiskManager,
    // LRU: track access order (most recent at the end)
    lru_list: Vec<usize>,
}

impl BufferPoolManager {
    pub fn new(disk_manager: DiskManager) -> Self {
        let mut frames = Vec::with_capacity(BUFFER_POOL_SIZE);
        for _ in 0..BUFFER_POOL_SIZE {
            frames.push(Frame::new());
        }
        BufferPoolManager {
            frames,
            page_table: HashMap::new(),
            disk_manager,
            lru_list: Vec::new(),
        }
    }

    fn find_victim(&self) -> Option<usize> {
        // Find unpinned frame using LRU (oldest first)
        self.lru_list
            .iter()
            .find(|&&frame_id| self.frames[frame_id].pin_count == 0)
            .copied()
    }

    fn update_lru(&mut self, frame_id: usize) {
        // Move frame_id to end of LRU list (most recently used)
        self.lru_list.retain(|&id| id != frame_id);
        self.lru_list.push(frame_id);
    }

    fn evict(&mut self, frame_id: usize) -> Result<()> {
        let frame = &mut self.frames[frame_id];
        if let Some(old_page_id) = frame.page_id {
            println!("[BufferPool] Evicting page {old_page_id} (dirty: {})", frame.is_dirty);
            if frame.is_dirty {
                self.disk_manager.write_page(old_page_id, &frame.page.data)?;
            }
            self.page_table.remove(&old_page_id);
        }
        frame.page_id = None;
        frame.is_dirty = false;
        frame.pin_count = 0;
        Ok(())
    }

    pub fn fetch_page(&mut self, page_id: u32) -> Result<&mut Page> {
        // Check if already in buffer pool
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            self.frames[frame_id].pin_count += 1;
            self.update_lru(frame_id);
            return Ok(&mut self.frames[frame_id].page);
        }

        // Find a frame to use
        let frame_id = if self.page_table.len() < BUFFER_POOL_SIZE {
            // Find empty frame
            self.frames.iter().position(|f| f.page_id.is_none()).unwrap()
        } else {
            // Need to evict
            let victim = self.find_victim().ok_or_else(|| anyhow::anyhow!("no victim frame"))?;
            self.evict(victim)?;
            victim
        };

        // Read page from disk
        println!("[BufferPool] Fetching page {page_id} from disk");
        let mut data = [0u8; PAGE_SIZE];
        self.disk_manager.read_page(page_id, &mut data)?;

        let frame = &mut self.frames[frame_id];
        frame.page = Page::from_bytes(&data);
        frame.page_id = Some(page_id);
        frame.pin_count = 1;
        frame.is_dirty = false;

        self.page_table.insert(page_id, frame_id);
        self.update_lru(frame_id);

        Ok(&mut self.frames[frame_id].page)
    }

    pub fn new_page(&mut self) -> Result<(u32, &mut Page)> {
        let frame_id = if self.page_table.len() < BUFFER_POOL_SIZE {
            self.frames.iter().position(|f| f.page_id.is_none()).unwrap()
        } else {
            let victim = self.find_victim().ok_or_else(|| anyhow::anyhow!("no victim frame"))?;
            self.evict(victim)?;
            victim
        };

        let page_id = self.disk_manager.allocate_page()?;
        println!("[BufferPool] Allocating new page {page_id}");

        let frame = &mut self.frames[frame_id];
        frame.page = Page::new(page_id);
        frame.page_id = Some(page_id);
        frame.pin_count = 1;
        frame.is_dirty = true;

        self.page_table.insert(page_id, frame_id);
        self.update_lru(frame_id);

        Ok((page_id, &mut self.frames[frame_id].page))
    }

    pub fn unpin_page(&mut self, page_id: u32, is_dirty: bool) -> Result<()> {
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            let frame = &mut self.frames[frame_id];
            if frame.pin_count == 0 {
                bail!("page {page_id} is not pinned");
            }
            frame.pin_count -= 1;
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
                self.disk_manager.write_page(page_id, &frame.page.data)?;
                frame.is_dirty = false;
            }
        }
        Ok(())
    }

    pub fn page_count(&self) -> u32 {
        self.disk_manager.page_count()
    }
}
