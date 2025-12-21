use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::buffer_pool::BufferPoolManager;
use crate::catalog::Catalog;
use crate::lock_manager::LockManager;
use crate::page::NO_NEXT_PAGE;
use crate::transaction::Transaction;
use crate::transaction_manager::TransactionManager;
use crate::tuple::deserialize_tuple_mvcc;
use crate::visibility::{is_tuple_visible, Snapshot};

use super::{Executor, Rid, Tuple};

pub struct SeqScanExecutor<'a> {
    bpm: Arc<Mutex<BufferPoolManager>>,
    catalog: &'a Catalog,
    table_id: u32,
    current_page_id: u32,
    current_slot_id: u16,
    #[allow(dead_code)]
    txn: Option<&'a mut Transaction>,
    #[allow(dead_code)]
    lock_manager: Option<&'a LockManager>,
    snapshot: Option<Snapshot>,
    txn_manager: Option<&'a TransactionManager>,
}

impl<'a> SeqScanExecutor<'a> {
    pub fn new(
        bpm: Arc<Mutex<BufferPoolManager>>,
        catalog: &'a Catalog,
        table_id: u32,
        txn: Option<&'a mut Transaction>,
        lock_manager: Option<&'a LockManager>,
        snapshot: Option<Snapshot>,
        txn_manager: Option<&'a TransactionManager>,
    ) -> Self {
        SeqScanExecutor {
            bpm,
            catalog,
            table_id,
            current_page_id: 0,
            current_slot_id: 0,
            txn,
            lock_manager,
            snapshot,
            txn_manager,
        }
    }
}

impl Executor for SeqScanExecutor<'_> {
    fn open(&mut self) -> Result<()> {
        let table = self
            .catalog
            .get_table_by_id(self.table_id)
            .ok_or_else(|| anyhow::anyhow!("table not found"))?;
        self.current_page_id = table.first_page_id;
        self.current_slot_id = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        let table = self
            .catalog
            .get_table_by_id(self.table_id)
            .ok_or_else(|| anyhow::anyhow!("table not found"))?;
        let schema = table.to_schema();

        loop {
            if self.current_page_id == NO_NEXT_PAGE {
                return Ok(None);
            }

            let page_arc = self.bpm.lock().unwrap().fetch_page(self.current_page_id)?;
            let (tuple_count, next_page_id) = {
                let page_guard = page_arc.read().unwrap();
                (page_guard.tuple_count(), page_guard.next_page_id())
            };
            self.bpm
                .lock()
                .unwrap()
                .unpin_page(self.current_page_id, false)?;

            if self.current_slot_id >= tuple_count {
                self.current_page_id = next_page_id;
                self.current_slot_id = 0;
                continue;
            }

            let rid = Rid {
                page_id: self.current_page_id,
                slot_id: self.current_slot_id,
            };

            let page_arc = self.bpm.lock().unwrap().fetch_page(self.current_page_id)?;
            let result = {
                let page_guard = page_arc.read().unwrap();
                if let Some(tuple_data) = page_guard.get_tuple(self.current_slot_id) {
                    let (xmin, xmax, values) = deserialize_tuple_mvcc(tuple_data, &schema)?;

                    let is_visible = match (&self.snapshot, &self.txn_manager) {
                        (Some(snapshot), Some(txn_manager)) => {
                            is_tuple_visible(xmin, xmax, snapshot, txn_manager)
                        }
                        _ => true,
                    };

                    if is_visible {
                        Some(Tuple::with_rid(values, rid))
                    } else {
                        None
                    }
                } else {
                    None
                }
            };
            self.bpm
                .lock()
                .unwrap()
                .unpin_page(self.current_page_id, false)?;

            self.current_slot_id += 1;

            if let Some(tuple) = result {
                return Ok(Some(tuple));
            }
        }
    }
}
