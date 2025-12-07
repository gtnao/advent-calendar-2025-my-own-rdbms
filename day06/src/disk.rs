use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

use anyhow::Result;

use crate::page::PAGE_SIZE;

pub struct DiskManager {
    file: File,
    page_count: u32,
}

impl DiskManager {
    pub fn open(path: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;

        let metadata = file.metadata()?;
        let page_count = (metadata.len() / PAGE_SIZE as u64) as u32;

        Ok(DiskManager { file, page_count })
    }

    pub fn read_page(&mut self, page_id: u32, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        let offset = (page_id as u64) * (PAGE_SIZE as u64);
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(buf)?;
        Ok(())
    }

    pub fn write_page(&mut self, page_id: u32, data: &[u8; PAGE_SIZE]) -> Result<()> {
        let offset = (page_id as u64) * (PAGE_SIZE as u64);
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)?;
        self.file.sync_all()?;
        Ok(())
    }

    pub fn allocate_page(&mut self) -> Result<u32> {
        let page_id = self.page_count;
        self.page_count += 1;
        // Write empty page to extend file
        let empty = [0u8; PAGE_SIZE];
        self.write_page(page_id, &empty)?;
        Ok(page_id)
    }

    pub fn page_count(&self) -> u32 {
        self.page_count
    }
}
