use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::analyzer::AnalyzedAssignment;
use crate::buffer_pool::BufferPoolManager;
use crate::lock_manager::{LockManager, LockMode};
use crate::transaction::{Transaction, UndoLogEntry};
use crate::tuple::{serialize_tuple_mvcc, TxnId, Value, INVALID_TXN_ID};
use crate::wal::{WalManager, WalRecordType};

use super::{evaluate_expr, Executor, Rid, Tuple};

pub struct UpdateExecutor<'a> {
    bpm: Arc<Mutex<BufferPoolManager>>,
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
        child: Box<dyn Executor + 'a>,
        assignments: Vec<AnalyzedAssignment>,
        txn: Option<&'a mut Transaction>,
        lock_manager: Option<&'a LockManager>,
        wal_manager: Option<Arc<WalManager>>,
    ) -> Self {
        UpdateExecutor {
            bpm,
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
            let new_rid = loop {
                let page_count = self.bpm.lock().unwrap().page_count();
                if page_count > 0 {
                    let last_page_id = page_count - 1;
                    let page_arc = self.bpm.lock().unwrap().fetch_page_mut(last_page_id)?;
                    let mut page_guard = page_arc.write().unwrap();
                    if let Ok(slot_id) = page_guard.insert(&tuple_data) {
                        drop(page_guard);
                        self.bpm.lock().unwrap().unpin_page(last_page_id, true)?;
                        break Rid {
                            page_id: last_page_id,
                            slot_id,
                        };
                    }
                    drop(page_guard);
                    self.bpm.lock().unwrap().unpin_page(last_page_id, false)?;
                }

                let (page_id, page_arc) = self.bpm.lock().unwrap().new_page()?;
                let mut page_guard = page_arc.write().unwrap();
                if let Ok(slot_id) = page_guard.insert(&tuple_data) {
                    drop(page_guard);
                    self.bpm.lock().unwrap().unpin_page(page_id, true)?;
                    break Rid { page_id, slot_id };
                }
                drop(page_guard);
                self.bpm.lock().unwrap().unpin_page(page_id, false)?;
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
        }

        Ok(Some(Tuple::new(vec![Value::Int(self.updated_count)])))
    }
}
