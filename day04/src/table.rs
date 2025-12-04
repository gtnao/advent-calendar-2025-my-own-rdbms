use anyhow::Result;

use crate::buffer_pool::BufferPoolManager;
use crate::tuple::{deserialize_tuple, serialize_tuple, Schema, Value};

pub struct Table {
    bpm: BufferPoolManager,
    schema: Schema,
}

impl Table {
    pub fn new(bpm: BufferPoolManager, schema: Schema) -> Self {
        Table { bpm, schema }
    }

    pub fn insert(&mut self, values: &[Value]) -> Result<(u32, u16)> {
        let tuple_data = serialize_tuple(values);

        // Try to insert into the last page
        let page_count = self.bpm.page_count();
        if page_count > 0 {
            let last_page_id = page_count - 1;
            let page = self.bpm.fetch_page(last_page_id)?;
            if let Ok(slot_id) = page.insert(&tuple_data) {
                self.bpm.unpin_page(last_page_id, true)?;
                return Ok((last_page_id, slot_id));
            }
            self.bpm.unpin_page(last_page_id, false)?;
        }

        // Allocate new page
        let (page_id, page) = self.bpm.new_page()?;
        let slot_id = page.insert(&tuple_data)?;
        self.bpm.unpin_page(page_id, true)?;
        Ok((page_id, slot_id))
    }

    pub fn scan(&mut self) -> Result<Vec<Vec<Value>>> {
        let mut results = Vec::new();
        let page_count = self.bpm.page_count();
        for page_id in 0..page_count {
            let page = self.bpm.fetch_page(page_id)?;
            for slot_id in 0..page.tuple_count() {
                if let Some(tuple_data) = page.get_tuple(slot_id) {
                    let values = deserialize_tuple(tuple_data, &self.schema)?;
                    results.push(values);
                }
            }
            self.bpm.unpin_page(page_id, false)?;
        }
        Ok(results)
    }

    // For debugging purposes.
    pub fn flush(&mut self) -> Result<()> {
        self.bpm.flush_all()
    }

    // For debugging purposes.
    pub fn page_count(&self) -> u32 {
        self.bpm.page_count()
    }
}
