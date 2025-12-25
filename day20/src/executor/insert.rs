use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::analyzer::{AnalyzedExpr, AnalyzedInsertStatement};
use crate::btree::{BTree, IndexKey};
use crate::buffer_pool::BufferPoolManager;
use crate::catalog::Catalog;
use crate::lock_manager::{LockManager, LockMode};
use crate::page::NO_NEXT_PAGE;
use crate::transaction::{Transaction, UndoLogEntry};
use crate::tuple::{serialize_tuple_mvcc, TxnId, Value, INVALID_TXN_ID};
use crate::wal::{WalManager, WalRecordType};

use super::{literal_to_value, Executor, Rid, Tuple};

pub struct InsertExecutor<'a> {
    bpm: Arc<Mutex<BufferPoolManager>>,
    catalog: &'a Catalog,
    table_id: u32,
    values: Vec<Value>,
    txn: Option<&'a mut Transaction>,
    lock_manager: Option<&'a LockManager>,
    wal_manager: Option<Arc<WalManager>>,
    executed: bool,
}

impl<'a> InsertExecutor<'a> {
    pub fn new(
        bpm: Arc<Mutex<BufferPoolManager>>,
        catalog: &'a Catalog,
        stmt: &AnalyzedInsertStatement,
        txn: Option<&'a mut Transaction>,
        lock_manager: Option<&'a LockManager>,
        wal_manager: Option<Arc<WalManager>>,
    ) -> Result<Self> {
        let values: Vec<Value> = stmt
            .values
            .iter()
            .map(|expr| match expr {
                AnalyzedExpr::Literal(lit) => Ok(literal_to_value(lit)),
                _ => anyhow::bail!("only literals supported in INSERT"),
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(InsertExecutor {
            bpm,
            catalog,
            table_id: stmt.table_id,
            values,
            txn,
            lock_manager,
            wal_manager,
            executed: false,
        })
    }
}

impl Executor for InsertExecutor<'_> {
    fn open(&mut self) -> Result<()> {
        self.executed = false;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;

        let table = self
            .catalog
            .get_table_by_id(self.table_id)
            .ok_or_else(|| anyhow::anyhow!("table not found"))?;

        let xmin: TxnId = self.txn.as_ref().map(|t| t.id).unwrap_or(INVALID_TXN_ID);
        let xmax: TxnId = INVALID_TXN_ID;

        let tuple_data = serialize_tuple_mvcc(xmin, xmax, &self.values);

        let rid = {
            let mut current_page_id = table.first_page_id;
            let mut last_page_id = current_page_id;
            let mut result_rid: Option<Rid> = None;

            while current_page_id != NO_NEXT_PAGE {
                let page_arc = self.bpm.lock().unwrap().fetch_page_mut(current_page_id)?;
                let mut page_guard = page_arc.write().unwrap();

                if let Ok(slot_id) = page_guard.insert(&tuple_data) {
                    drop(page_guard);
                    self.bpm
                        .lock()
                        .unwrap()
                        .unpin_page(current_page_id, true)?;
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
                    let (new_page_id, new_page_arc) = self.bpm.lock().unwrap().new_page()?;
                    let mut new_page = new_page_arc.write().unwrap();
                    new_page.set_next_page_id(NO_NEXT_PAGE);
                    let slot_id = new_page.insert(&tuple_data)?;
                    drop(new_page);
                    self.bpm.lock().unwrap().unpin_page(new_page_id, true)?;

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
                    let prev_lsn = txn.last_lsn;
                    let lsn = wal_manager.append(
                        txn.id,
                        prev_lsn,
                        WalRecordType::Insert {
                            rid,
                            data: tuple_data.clone(),
                        },
                    );
                    txn.set_last_lsn(lsn);

                    let page_arc = self.bpm.lock().unwrap().fetch_page_mut(rid.page_id)?;
                    let mut page_guard = page_arc.write().unwrap();
                    page_guard.page_lsn = lsn;
                    drop(page_guard);
                    self.bpm.lock().unwrap().unpin_page(rid.page_id, true)?;

                    txn.add_undo_entry(UndoLogEntry::Insert {
                        lsn,
                        prev_lsn,
                        rid,
                        data: tuple_data,
                    });
                }

                if let Some(lock_manager) = self.lock_manager {
                    lock_manager
                        .lock(txn.id, rid, LockMode::Exclusive)
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                    txn.add_lock(rid);
                }
            }
        }

        // Maintain indexes
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
                .map(|&col_idx| self.values[col_idx].clone())
                .collect();
            let key = IndexKey::new(key_values);
            btree.insert(&key, rid)?;
        }

        Ok(Some(Tuple::new(vec![Value::Int(1)])))
    }
}
