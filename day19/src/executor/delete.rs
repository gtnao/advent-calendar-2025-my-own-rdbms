use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::buffer_pool::BufferPoolManager;
use crate::lock_manager::{LockManager, LockMode};
use crate::transaction::{Transaction, UndoLogEntry};
use crate::tuple::{TxnId, Value, INVALID_TXN_ID};
use crate::wal::{WalManager, WalRecordType};

use super::{Executor, Tuple};

pub struct DeleteExecutor<'a> {
    bpm: Arc<Mutex<BufferPoolManager>>,
    child: Box<dyn Executor + 'a>,
    txn: Option<&'a mut Transaction>,
    lock_manager: Option<&'a LockManager>,
    wal_manager: Option<Arc<WalManager>>,
    executed: bool,
    deleted_count: i32,
}

impl<'a> DeleteExecutor<'a> {
    pub fn new(
        bpm: Arc<Mutex<BufferPoolManager>>,
        child: Box<dyn Executor + 'a>,
        txn: Option<&'a mut Transaction>,
        lock_manager: Option<&'a LockManager>,
        wal_manager: Option<Arc<WalManager>>,
    ) -> Self {
        DeleteExecutor {
            bpm,
            child,
            txn,
            lock_manager,
            wal_manager,
            executed: false,
            deleted_count: 0,
        }
    }
}

impl Executor for DeleteExecutor<'_> {
    fn open(&mut self) -> Result<()> {
        self.executed = false;
        self.deleted_count = 0;
        self.child.open()
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;

        let xmax: TxnId = self.txn.as_ref().map(|t| t.id).unwrap_or(INVALID_TXN_ID);

        let mut targets = Vec::new();
        while let Some(tuple) = self.child.next()? {
            if let Some(rid) = tuple.rid {
                targets.push(rid);
            }
        }

        if let Some(ref mut txn) = self.txn {
            if txn.is_active() {
                if let Some(lock_manager) = self.lock_manager {
                    for rid in &targets {
                        lock_manager
                            .lock(txn.id, *rid, LockMode::Exclusive)
                            .map_err(|e| anyhow::anyhow!("{e}"))?;
                        txn.add_lock(*rid);
                    }
                }
            }
        }

        self.deleted_count = targets.len() as i32;
        for rid in targets {
            let page_arc = self.bpm.lock().unwrap().fetch_page_mut(rid.page_id)?;
            let mut page_guard = page_arc.write().unwrap();

            page_guard.set_tuple_xmax(rid.slot_id, xmax)?;

            if let Some(ref mut txn) = self.txn {
                if txn.is_active() {
                    if let Some(ref wal_manager) = self.wal_manager {
                        let prev_lsn = txn.last_lsn;
                        let lsn = wal_manager.append(
                            txn.id,
                            prev_lsn,
                            WalRecordType::Delete { rid, xmax },
                        );
                        txn.set_last_lsn(lsn);
                        page_guard.page_lsn = lsn;

                        txn.add_undo_entry(UndoLogEntry::Delete {
                            lsn,
                            prev_lsn,
                            rid,
                            old_xmax: INVALID_TXN_ID,
                        });
                    }
                }
            }

            drop(page_guard);
            self.bpm.lock().unwrap().unpin_page(rid.page_id, true)?;
        }

        Ok(Some(Tuple::new(vec![Value::Int(self.deleted_count)])))
    }
}
