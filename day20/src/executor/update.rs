use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::analyzer::AnalyzedAssignment;
use crate::btree::{BTree, IndexKey};
use crate::buffer_pool::BufferPoolManager;
use crate::catalog::Catalog;
use crate::lock_manager::{LockManager, LockMode};
use crate::page::NO_NEXT_PAGE;
use crate::transaction::{Transaction, UndoLogEntry};
use crate::tuple::{serialize_tuple_mvcc, TxnId, Value, INVALID_TXN_ID};
use crate::wal::{WalManager, WalRecordType};

use super::{evaluate_expr, Executor, Rid, Tuple};

pub struct UpdateExecutor<'a> {
    bpm: Arc<Mutex<BufferPoolManager>>,
    catalog: &'a Catalog,
    table_id: u32,
    child: Box<dyn Executor + 'a>,
    assignments: Vec<AnalyzedAssignment>,
    txn: Option<&'a mut Transaction>,
    lock_manager: Option<&'a LockManager>,
    wal_manager: Option<Arc<WalManager>>,
    executed: bool,
    updated_count: i32,
}

impl<'a> UpdateExecutor<'a> {
    pub fn new(
        bpm: Arc<Mutex<BufferPoolManager>>,
        catalog: &'a Catalog,
        table_id: u32,
        child: Box<dyn Executor + 'a>,
        assignments: Vec<AnalyzedAssignment>,
        txn: Option<&'a mut Transaction>,
        lock_manager: Option<&'a LockManager>,
        wal_manager: Option<Arc<WalManager>>,
    ) -> Self {
        UpdateExecutor {
            bpm,
            catalog,
            table_id,
            child,
            assignments,
            txn,
            lock_manager,
            wal_manager,
            executed: false,
            updated_count: 0,
        }
    }
}

impl Executor for UpdateExecutor<'_> {
    fn open(&mut self) -> Result<()> {
        self.executed = false;
        self.updated_count = 0;
        self.child.open()
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;

        let mut updates = Vec::new();
        while let Some(tuple) = self.child.next()? {
            if let Some(rid) = tuple.rid {
                let mut new_values = tuple.values.clone();
                for AnalyzedAssignment {
                    column_index,
                    value,
                } in &self.assignments
                {
                    let new_value = evaluate_expr(value, &tuple)?;
                    new_values[*column_index] = new_value;
                }
                updates.push((rid, new_values));
            }
        }

        if let Some(ref mut txn) = self.txn {
            if txn.is_active() {
                if let Some(lock_manager) = self.lock_manager {
                    for (old_rid, _) in &updates {
                        lock_manager
                            .lock(txn.id, *old_rid, LockMode::Exclusive)
                            .map_err(|e| anyhow::anyhow!("{e}"))?;
                        txn.add_lock(*old_rid);
                    }
                }
            }
        }

        let xmax = self
            .txn
            .as_ref()
            .map(|t| t.id)
            .unwrap_or(INVALID_TXN_ID);

        self.updated_count = updates.len() as i32;
        for (old_rid, new_values) in updates {
            let page_arc = self.bpm.lock().unwrap().fetch_page_mut(old_rid.page_id)?;
            let mut page_guard = page_arc.write().unwrap();
            page_guard.set_tuple_xmax(old_rid.slot_id, xmax)?;

            let delete_lsn;
            let delete_prev_lsn;
            if let Some(ref mut txn) = self.txn {
                if txn.is_active() {
                    if let Some(ref wal_manager) = self.wal_manager {
                        delete_prev_lsn = txn.last_lsn;
                        delete_lsn = wal_manager.append(
                            txn.id,
                            delete_prev_lsn,
                            WalRecordType::Delete {
                                rid: old_rid,
                                xmax,
                            },
                        );
                        txn.set_last_lsn(delete_lsn);
                        page_guard.page_lsn = delete_lsn;
                    } else {
                        delete_lsn = 0;
                        delete_prev_lsn = 0;
                    }
                } else {
                    delete_lsn = 0;
                    delete_prev_lsn = 0;
                }
            } else {
                delete_lsn = 0;
                delete_prev_lsn = 0;
            }

            drop(page_guard);
            self.bpm.lock().unwrap().unpin_page(old_rid.page_id, true)?;

            let new_xmin = xmax;
            let new_xmax: TxnId = INVALID_TXN_ID;
            let tuple_data = serialize_tuple_mvcc(new_xmin, new_xmax, &new_values);

            // Get the table definition to find the first page
            let table = self
                .catalog
                .get_table_by_id(self.table_id)
                .ok_or_else(|| anyhow::anyhow!("table not found"))?;

            // Find a page with space or create new one (following the linked list)
            let new_rid = {
                let mut current_page_id = table.first_page_id;
                let mut last_page_id = current_page_id;
                let mut result_rid: Option<Rid> = None;

                while current_page_id != NO_NEXT_PAGE {
                    let page_arc = self.bpm.lock().unwrap().fetch_page_mut(current_page_id)?;
                    let mut page_guard = page_arc.write().unwrap();

                    if let Ok(slot_id) = page_guard.insert(&tuple_data) {
                        drop(page_guard);
                        self.bpm.lock().unwrap().unpin_page(current_page_id, true)?;
                        result_rid = Some(Rid {
                            page_id: current_page_id,
                            slot_id,
                        });
                        break;
                    }

                    last_page_id = current_page_id;
                    current_page_id = page_guard.next_page_id();
                    drop(page_guard);
                    self.bpm.lock().unwrap().unpin_page(last_page_id, false)?;
                }

                match result_rid {
                    Some(rid) => rid,
                    None => {
                        // No space in existing pages, create new page
                        let (new_page_id, new_page_arc) = self.bpm.lock().unwrap().new_page()?;
                        let mut new_page = new_page_arc.write().unwrap();
                        new_page.set_next_page_id(NO_NEXT_PAGE);
                        let slot_id = new_page.insert(&tuple_data)?;
                        drop(new_page);
                        self.bpm.lock().unwrap().unpin_page(new_page_id, true)?;

                        // Link the new page
                        let page_arc = self.bpm.lock().unwrap().fetch_page_mut(last_page_id)?;
                        let mut page = page_arc.write().unwrap();
                        page.set_next_page_id(new_page_id);
                        drop(page);
                        self.bpm.lock().unwrap().unpin_page(last_page_id, true)?;

                        Rid {
                            page_id: new_page_id,
                            slot_id,
                        }
                    }
                }
            };

            if let Some(ref mut txn) = self.txn {
                if txn.is_active() {
                    if let Some(ref wal_manager) = self.wal_manager {
                        let insert_prev_lsn = txn.last_lsn;
                        let insert_lsn = wal_manager.append(
                            txn.id,
                            insert_prev_lsn,
                            WalRecordType::Insert {
                                rid: new_rid,
                                data: tuple_data.clone(),
                            },
                        );
                        txn.set_last_lsn(insert_lsn);

                        let page_arc = self.bpm.lock().unwrap().fetch_page_mut(new_rid.page_id)?;
                        let mut page_guard = page_arc.write().unwrap();
                        page_guard.page_lsn = insert_lsn;
                        drop(page_guard);
                        self.bpm.lock().unwrap().unpin_page(new_rid.page_id, true)?;

                        txn.add_undo_entry(UndoLogEntry::Delete {
                            lsn: delete_lsn,
                            prev_lsn: delete_prev_lsn,
                            rid: old_rid,
                            old_xmax: INVALID_TXN_ID,
                        });
                        txn.add_undo_entry(UndoLogEntry::Insert {
                            lsn: insert_lsn,
                            prev_lsn: insert_prev_lsn,
                            rid: new_rid,
                            data: tuple_data,
                        });
                    }

                    if let Some(lock_manager) = self.lock_manager {
                        lock_manager
                            .lock(txn.id, new_rid, LockMode::Exclusive)
                            .map_err(|e| anyhow::anyhow!("{e}"))?;
                        txn.add_lock(new_rid);
                    }
                }
            }

            // Maintain indexes - insert new entry for the new tuple
            if let Some(table) = self.catalog.get_table_by_id(self.table_id) {
                let indexes = self.catalog.get_indexes_for_table(self.table_id);
                for index_def in indexes {
                    let key_schema: Vec<_> = index_def
                        .column_ids
                        .iter()
                        .map(|&col_idx| table.columns[col_idx].data_type.clone())
                        .collect();

                    let mut btree = BTree::new(Arc::clone(&self.bpm), key_schema);
                    btree.set_meta_page_id(index_def.meta_page_id);

                    let key_values: Vec<Value> = index_def
                        .column_ids
                        .iter()
                        .map(|&col_idx| new_values[col_idx].clone())
                        .collect();
                    let key = IndexKey::new(key_values);
                    btree.insert(&key, new_rid)?;
                }
            }
        }

        Ok(Some(Tuple::new(vec![Value::Int(self.updated_count)])))
    }
}
