use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::analyzer::AnalyzedCreateTableStatement;
use crate::bootstrap::{DATA_TYPE_BOOL, DATA_TYPE_INT, DATA_TYPE_VARCHAR, PG_ATTRIBUTE_TABLE_ID, PG_CLASS_TABLE_ID};
use crate::buffer_pool::BufferPoolManager;
use crate::catalog::Catalog;
use crate::lock_manager::LockManager;
use crate::page::NO_NEXT_PAGE;
use crate::transaction::{Transaction, UndoLogEntry};
use crate::tuple::{serialize_tuple_mvcc, DataType, TxnId, Value, INVALID_TXN_ID};
use crate::wal::{WalManager, WalRecordType};

use super::{Executor, Rid, Tuple};

pub struct CreateTableExecutor<'a> {
    bpm: Arc<Mutex<BufferPoolManager>>,
    catalog: &'a Catalog,
    stmt: AnalyzedCreateTableStatement,
    txn: Option<&'a mut Transaction>,
    #[allow(dead_code)]
    lock_manager: Option<&'a LockManager>,
    wal_manager: Option<Arc<WalManager>>,
    executed: bool,
}

impl<'a> CreateTableExecutor<'a> {
    pub fn new(
        bpm: Arc<Mutex<BufferPoolManager>>,
        catalog: &'a Catalog,
        stmt: &AnalyzedCreateTableStatement,
        txn: Option<&'a mut Transaction>,
        lock_manager: Option<&'a LockManager>,
        wal_manager: Option<Arc<WalManager>>,
    ) -> Self {
        CreateTableExecutor {
            bpm,
            catalog,
            stmt: stmt.clone(),
            txn,
            lock_manager,
            wal_manager,
            executed: false,
        }
    }

    fn data_type_to_int(dt: &DataType) -> i32 {
        match dt {
            DataType::Int => DATA_TYPE_INT,
            DataType::Varchar => DATA_TYPE_VARCHAR,
            DataType::Bool => DATA_TYPE_BOOL,
        }
    }

    fn insert_into_table(
        &self,
        table_id: u32,
        tuple_data: &[u8],
    ) -> Result<(Rid, Option<(u32, u32)>)> {
        let table = self
            .catalog
            .get_table_by_id(table_id)
            .ok_or_else(|| anyhow::anyhow!("table not found: {}", table_id))?;

        let mut current_page_id = table.first_page_id;
        let mut last_page_id = current_page_id;

        while current_page_id != NO_NEXT_PAGE {
            let page_arc = self.bpm.lock().unwrap().fetch_page_mut(current_page_id)?;
            let mut page_guard = page_arc.write().unwrap();

            if let Ok(slot_id) = page_guard.insert(tuple_data) {
                drop(page_guard);
                self.bpm
                    .lock()
                    .unwrap()
                    .unpin_page(current_page_id, true)?;

                return Ok((
                    Rid {
                        page_id: current_page_id,
                        slot_id,
                    },
                    None,
                ));
            }

            last_page_id = current_page_id;
            current_page_id = page_guard.next_page_id();
            drop(page_guard);
            self.bpm.lock().unwrap().unpin_page(last_page_id, false)?;
        }

        let (new_page_id, new_page_arc) = self.bpm.lock().unwrap().new_page()?;
        {
            let mut new_page = new_page_arc.write().unwrap();
            new_page.set_next_page_id(NO_NEXT_PAGE);
            let _slot_id = new_page.insert(tuple_data)?;
        }
        self.bpm.lock().unwrap().unpin_page(new_page_id, true)?;

        let page_arc = self.bpm.lock().unwrap().fetch_page_mut(last_page_id)?;
        {
            let mut page = page_arc.write().unwrap();
            page.set_next_page_id(new_page_id);
        }
        self.bpm.lock().unwrap().unpin_page(last_page_id, true)?;

        Ok((
            Rid {
                page_id: new_page_id,
                slot_id: 0,
            },
            Some((new_page_id, last_page_id)),
        ))
    }
}

impl Executor for CreateTableExecutor<'_> {
    fn open(&mut self) -> Result<()> {
        self.executed = false;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;

        let xmin: TxnId = self.txn.as_ref().map(|t| t.id).unwrap_or(INVALID_TXN_ID);
        let xmax: TxnId = INVALID_TXN_ID;

        let new_table_id = self.catalog.next_table_id()?;

        let (new_page_id, new_page_arc) = self.bpm.lock().unwrap().new_page()?;
        {
            let mut new_page = new_page_arc.write().unwrap();
            new_page.set_next_page_id(NO_NEXT_PAGE);

            if let Some(ref wal_manager) = self.wal_manager {
                if let Some(ref mut txn) = self.txn {
                    let prev_lsn = txn.last_lsn;
                    let lsn = wal_manager.append(
                        txn.id,
                        prev_lsn,
                        WalRecordType::AllocatePage {
                            page_id: new_page_id,
                            table_id: new_table_id,
                            prev_page_id: NO_NEXT_PAGE,
                        },
                    );
                    txn.set_last_lsn(lsn);
                    new_page.page_lsn = lsn;
                }
            }
        }
        self.bpm.lock().unwrap().unpin_page(new_page_id, true)?;

        let pg_class_tuple = serialize_tuple_mvcc(
            xmin,
            xmax,
            &[
                Value::Int(new_table_id as i32),
                Value::Varchar(self.stmt.table_name.clone()),
                Value::Int(new_page_id as i32),
            ],
        );
        let (pg_class_rid, _new_page_info) =
            self.insert_into_table(PG_CLASS_TABLE_ID, &pg_class_tuple)?;

        if let Some(ref wal_manager) = self.wal_manager {
            if let Some(ref mut txn) = self.txn {
                let prev_lsn = txn.last_lsn;
                let lsn = wal_manager.append(
                    txn.id,
                    prev_lsn,
                    WalRecordType::Insert {
                        rid: pg_class_rid,
                        data: pg_class_tuple.clone(),
                    },
                );
                txn.set_last_lsn(lsn);

                let page_arc = self
                    .bpm
                    .lock()
                    .unwrap()
                    .fetch_page_mut(pg_class_rid.page_id)?;
                {
                    let mut page_guard = page_arc.write().unwrap();
                    page_guard.page_lsn = lsn;
                }
                self.bpm
                    .lock()
                    .unwrap()
                    .unpin_page(pg_class_rid.page_id, true)?;

                txn.add_undo_entry(UndoLogEntry::Insert {
                    lsn,
                    prev_lsn,
                    rid: pg_class_rid,
                    data: pg_class_tuple,
                });
            }
        }

        for (ordinal, col) in self.stmt.columns.iter().enumerate() {
            let pg_attribute_tuple = serialize_tuple_mvcc(
                xmin,
                xmax,
                &[
                    Value::Int(new_table_id as i32),
                    Value::Varchar(col.name.clone()),
                    Value::Int(Self::data_type_to_int(&col.data_type)),
                    Value::Bool(false),
                    Value::Int(ordinal as i32),
                ],
            );
            let (pg_attribute_rid, _new_page_info) =
                self.insert_into_table(PG_ATTRIBUTE_TABLE_ID, &pg_attribute_tuple)?;

            if let Some(ref wal_manager) = self.wal_manager {
                if let Some(ref mut txn) = self.txn {
                    let prev_lsn = txn.last_lsn;
                    let lsn = wal_manager.append(
                        txn.id,
                        prev_lsn,
                        WalRecordType::Insert {
                            rid: pg_attribute_rid,
                            data: pg_attribute_tuple.clone(),
                        },
                    );
                    txn.set_last_lsn(lsn);

                    let page_arc = self
                        .bpm
                        .lock()
                        .unwrap()
                        .fetch_page_mut(pg_attribute_rid.page_id)?;
                    {
                        let mut page_guard = page_arc.write().unwrap();
                        page_guard.page_lsn = lsn;
                    }
                    self.bpm
                        .lock()
                        .unwrap()
                        .unpin_page(pg_attribute_rid.page_id, true)?;

                    txn.add_undo_entry(UndoLogEntry::Insert {
                        lsn,
                        prev_lsn,
                        rid: pg_attribute_rid,
                        data: pg_attribute_tuple,
                    });
                }
            }
        }

        Ok(Some(Tuple::new(vec![Value::Int(1)])))
    }
}
