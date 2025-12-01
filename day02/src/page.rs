use anyhow::{bail, Result};

pub const PAGE_SIZE: usize = 64; // small for testing

// Page layout:
//   Header (8 bytes):
//     - page_id: u32 (4 bytes)
//     - tuple_count: u16 (2 bytes)
//     - free_space_offset: u16 (2 bytes) - points to end of free space
//   Slot array (grows forward from offset 8):
//     - each slot: u16 offset + u16 length (4 bytes per slot)
//   Tuple data (grows backward from end of page)

const HEADER_SIZE: usize = 8;
const SLOT_SIZE: usize = 4;

pub struct Page {
    pub data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new(page_id: u32) -> Self {
        let mut page = Page {
            data: [0; PAGE_SIZE],
        };
        page.set_page_id(page_id);
        page.set_tuple_count(0);
        page.set_free_space_offset(PAGE_SIZE as u16);
        page
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut data = [0; PAGE_SIZE];
        data.copy_from_slice(bytes);
        Page { data }
    }

    pub fn page_id(&self) -> u32 {
        u32::from_ne_bytes(self.data[0..4].try_into().unwrap())
    }

    fn set_page_id(&mut self, id: u32) {
        self.data[0..4].copy_from_slice(&id.to_ne_bytes());
    }

    pub fn tuple_count(&self) -> u16 {
        u16::from_ne_bytes(self.data[4..6].try_into().unwrap())
    }

    fn set_tuple_count(&mut self, count: u16) {
        self.data[4..6].copy_from_slice(&count.to_ne_bytes());
    }

    pub fn free_space_offset(&self) -> u16 {
        u16::from_ne_bytes(self.data[6..8].try_into().unwrap())
    }

    fn set_free_space_offset(&mut self, offset: u16) {
        self.data[6..8].copy_from_slice(&offset.to_ne_bytes());
    }

    fn get_slot(&self, slot_id: u16) -> (u16, u16) {
        let slot_offset = HEADER_SIZE + (slot_id as usize) * SLOT_SIZE;
        let offset = u16::from_ne_bytes(self.data[slot_offset..slot_offset + 2].try_into().unwrap());
        let length = u16::from_ne_bytes(self.data[slot_offset + 2..slot_offset + 4].try_into().unwrap());
        (offset, length)
    }

    fn set_slot(&mut self, slot_id: u16, offset: u16, length: u16) {
        let slot_offset = HEADER_SIZE + (slot_id as usize) * SLOT_SIZE;
        self.data[slot_offset..slot_offset + 2].copy_from_slice(&offset.to_ne_bytes());
        self.data[slot_offset + 2..slot_offset + 4].copy_from_slice(&length.to_ne_bytes());
    }

    pub fn free_space(&self) -> usize {
        let slots_end = HEADER_SIZE + (self.tuple_count() as usize) * SLOT_SIZE;
        self.free_space_offset() as usize - slots_end
    }

    pub fn insert(&mut self, tuple_data: &[u8]) -> Result<u16> {
        let tuple_len = tuple_data.len();
        let required_space = tuple_len + SLOT_SIZE;

        if self.free_space() < required_space {
            bail!("not enough space in page");
        }

        let new_offset = self.free_space_offset() - tuple_len as u16;
        self.data[new_offset as usize..new_offset as usize + tuple_len].copy_from_slice(tuple_data);

        let slot_id = self.tuple_count();
        self.set_slot(slot_id, new_offset, tuple_len as u16);
        self.set_tuple_count(slot_id + 1);
        self.set_free_space_offset(new_offset);

        Ok(slot_id)
    }

    pub fn get_tuple(&self, slot_id: u16) -> Option<&[u8]> {
        if slot_id >= self.tuple_count() {
            return None;
        }
        let (offset, length) = self.get_slot(slot_id);
        Some(&self.data[offset as usize..(offset + length) as usize])
    }
}
