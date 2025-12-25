use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::btree::{BTree, IndexKey};
use crate::buffer_pool::BufferPoolManager;
use crate::catalog::{Catalog, IndexDef};
use crate::transaction_manager::TransactionManager;
use crate::tuple::deserialize_tuple_mvcc;
use crate::visibility::{is_tuple_visible, Snapshot};

use super::{Executor, Rid, Tuple};

pub struct IndexScanExecutor<'a> {
    bpm: Arc<Mutex<BufferPoolManager>>,
    catalog: &'a Catalog,
    table_id: u32,
    index_def: IndexDef,
    start_key: Option<IndexKey>,
    end_key: Option<IndexKey>,
    snapshot: Option<Snapshot>,
    txn_manager: Option<&'a TransactionManager>,
    rids: Vec<Rid>,
    current_idx: usize,
    opened: bool,
}

impl<'a> IndexScanExecutor<'a> {
    pub fn new(
        bpm: Arc<Mutex<BufferPoolManager>>,
        catalog: &'a Catalog,
        table_id: u32,
        index_def: IndexDef,
        start_key: Option<IndexKey>,
        end_key: Option<IndexKey>,
        snapshot: Option<Snapshot>,
        txn_manager: Option<&'a TransactionManager>,
    ) -> Self {
        IndexScanExecutor {
            bpm,
            catalog,
            table_id,
            index_def,
            start_key,
            end_key,
            snapshot,
            txn_manager,
            rids: Vec::new(),
            current_idx: 0,
            opened: false,
        }
    }
}

impl Executor for IndexScanExecutor<'_> {
    fn open(&mut self) -> Result<()> {
        self.current_idx = 0;
        self.opened = true;

        // Get table schema for building B-Tree key schema
        let table = self
            .catalog
            .get_table_by_id(self.table_id)
            .ok_or_else(|| anyhow::anyhow!("table not found"))?;

        let key_schema = self
            .index_def
            .column_ids
            .iter()
            .map(|&col_idx| table.columns[col_idx].data_type.clone())
            .collect();

        // Create B-Tree and perform range scan
        let mut btree = BTree::new(Arc::clone(&self.bpm), key_schema);
        btree.set_meta_page_id(self.index_def.meta_page_id);

        let results = btree.range_scan(
            self.start_key.as_ref(),
            self.end_key.as_ref(),
        )?;

        // Extract RIDs from results
        self.rids = results.into_iter().map(|(_, rid)| rid).collect();

        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if !self.opened {
            self.open()?;
        }

        let table = self
            .catalog
            .get_table_by_id(self.table_id)
            .ok_or_else(|| anyhow::anyhow!("table not found"))?;
        let schema = table.to_schema();

        while self.current_idx < self.rids.len() {
            let rid = self.rids[self.current_idx];
            self.current_idx += 1;

            // Fetch tuple from heap
            let page_arc = self.bpm.lock().unwrap().fetch_page(rid.page_id)?;
            let page_guard = page_arc.read().unwrap();

            let tuple_data = match page_guard.get_tuple(rid.slot_id) {
                Some(data) => data.to_vec(),
                None => {
                    drop(page_guard);
                    self.bpm.lock().unwrap().unpin_page(rid.page_id, false)?;
                    continue;
                }
            };

            drop(page_guard);
            self.bpm.lock().unwrap().unpin_page(rid.page_id, false)?;

            // Deserialize and check visibility
            let (xmin, xmax, values) = deserialize_tuple_mvcc(&tuple_data, &schema)?;

            // Check MVCC visibility
            let visible = if let (Some(snapshot), Some(txn_manager)) =
                (&self.snapshot, self.txn_manager)
            {
                is_tuple_visible(xmin, xmax, snapshot, txn_manager)
            } else {
                xmax == 0
            };

            if visible {
                return Ok(Some(Tuple {
                    values,
                    rid: Some(rid),
                }));
            }
        }

        Ok(None)
    }
}
