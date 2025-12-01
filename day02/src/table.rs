use anyhow::Result;

use crate::disk::DiskManager;
use crate::page::{Page, PAGE_SIZE};
use crate::tuple::{deserialize_tuple, serialize_tuple, Schema, Value};

pub struct Table {
    disk_manager: DiskManager,
    schema: Schema,
}

impl Table {
    pub fn new(disk_manager: DiskManager, schema: Schema) -> Self {
        Table {
            disk_manager,
            schema,
        }
    }

    pub fn insert(&mut self, values: &[Value]) -> Result<(u32, u16)> {
        let tuple_data = serialize_tuple(values);

        // Try to insert into the last page
        let page_count = self.disk_manager.page_count();
        if page_count > 0 {
            let last_page_id = page_count - 1;
            let mut buf = [0u8; PAGE_SIZE];
            self.disk_manager.read_page(last_page_id, &mut buf)?;
            let mut page = Page::from_bytes(&buf);
            if let Ok(slot_id) = page.insert(&tuple_data) {
                self.disk_manager.write_page(last_page_id, &page.data)?;
                return Ok((last_page_id, slot_id));
            }
        }

        // Allocate new page
        let page_id = self.disk_manager.allocate_page()?;
        let mut page = Page::new(page_id);
        let slot_id = page.insert(&tuple_data)?;
        self.disk_manager.write_page(page_id, &page.data)?;
        Ok((page_id, slot_id))
    }

    pub fn scan(&mut self) -> Result<Vec<Vec<Value>>> {
        let mut results = Vec::new();
        let page_count = self.disk_manager.page_count();
        for page_id in 0..page_count {
            let mut buf = [0u8; PAGE_SIZE];
            self.disk_manager.read_page(page_id, &mut buf)?;
            let page = Page::from_bytes(&buf);
            for slot_id in 0..page.tuple_count() {
                if let Some(tuple_data) = page.get_tuple(slot_id) {
                    let values = deserialize_tuple(tuple_data, &self.schema)?;
                    results.push(values);
                }
            }
        }
        Ok(results)
    }

    pub fn page_count(&self) -> u32 {
        self.disk_manager.page_count()
    }

}
