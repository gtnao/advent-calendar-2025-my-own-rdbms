use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::analyzer::AnalyzedCreateIndexStatement;
use crate::bootstrap::PG_INDEX_TABLE_ID;
use crate::btree::{BTree, IndexKey};
use crate::buffer_pool::BufferPoolManager;
use crate::catalog::Catalog;
use crate::lock_manager::LockManager;
use crate::page::NO_NEXT_PAGE;
use crate::transaction::{Transaction, UndoLogEntry};
use crate::tuple::{deserialize_tuple_mvcc, serialize_tuple_mvcc, TxnId, Value, INVALID_TXN_ID};
use crate::wal::{WalManager, WalRecordType};

use super::{Executor, Rid, Tuple};

pub struct CreateIndexExecutor<'a> {
    bpm: Arc<Mutex<BufferPoolManager>>,
    catalog: &'a Catalog,
    stmt: AnalyzedCreateIndexStatement,
    txn: Option<&'a mut Transaction>,
    #[allow(dead_code)]
    lock_manager: Option<&'a LockManager>,
    wal_manager: Option<Arc<WalManager>>,
    executed: bool,
}

impl<'a> CreateIndexExecutor<'a> {
    pub fn new(
        bpm: Arc<Mutex<BufferPoolManager>>,
        catalog: &'a Catalog,
        stmt: &AnalyzedCreateIndexStatement,
        txn: Option<&'a mut Transaction>,
        lock_manager: Option<&'a LockManager>,
        wal_manager: Option<Arc<WalManager>>,
    ) -> Self {
        CreateIndexExecutor {
            bpm,
            catalog,
            stmt: stmt.clone(),
            txn,
            lock_manager,
            wal_manager,
            executed: false,
        }
    }

    fn insert_into_pg_index(&self, tuple_data: &[u8]) -> Result<Rid> {
        let table = self
            .catalog
            .get_table_by_id(PG_INDEX_TABLE_ID)
            .ok_or_else(|| anyhow::anyhow!("pg_index table not found"))?;

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

                return Ok(Rid {
                    page_id: current_page_id,
                    slot_id,
                });
            }

            last_page_id = current_page_id;
            current_page_id = page_guard.next_page_id();
            drop(page_guard);
            self.bpm.lock().unwrap().unpin_page(last_page_id, false)?;
        }

        // Need a new page
        let (new_page_id, new_page_arc) = self.bpm.lock().unwrap().new_page()?;
        {
            let mut new_page = new_page_arc.write().unwrap();
            new_page.set_next_page_id(NO_NEXT_PAGE);
            let _slot_id = new_page.insert(tuple_data)?;
        }
        self.bpm.lock().unwrap().unpin_page(new_page_id, true)?;

        // Link the new page
        let page_arc = self.bpm.lock().unwrap().fetch_page_mut(last_page_id)?;
        {
            let mut page = page_arc.write().unwrap();
            page.set_next_page_id(new_page_id);
        }
        self.bpm.lock().unwrap().unpin_page(last_page_id, true)?;

        Ok(Rid {
            page_id: new_page_id,
            slot_id: 0,
        })
    }
}

impl Executor for CreateIndexExecutor<'_> {
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

        // Get next index ID
        let new_index_id = self.catalog.next_index_id()?;

        // Get table info
        let table = self
            .catalog
            .get_table_by_id(self.stmt.table_id)
            .ok_or_else(|| anyhow::anyhow!("table not found"))?;

        // Determine key schema from column IDs
        let key_schema: Vec<_> = self
            .stmt
            .column_ids
            .iter()
            .map(|&col_idx| table.columns[col_idx].data_type.clone())
            .collect();

        // Create B-Tree (creates meta page + initial root leaf page)
        let mut btree = BTree::new(Arc::clone(&self.bpm), key_schema);
        btree.create_empty()?;
        let meta_page_id = btree.meta_page_id().unwrap();

        // Log page allocation for meta page
        if let Some(ref wal_manager) = self.wal_manager {
            if let Some(ref mut txn) = self.txn {
                let prev_lsn = txn.last_lsn;
                let lsn = wal_manager.append(
                    txn.id,
                    prev_lsn,
                    WalRecordType::AllocatePage {
                        page_id: meta_page_id,
                        table_id: new_index_id, // Use index_id as table_id for index pages
                        prev_page_id: NO_NEXT_PAGE,
                    },
                );
                txn.set_last_lsn(lsn);
            }
        }

        // Scan existing table data and populate the index
        // Index ALL tuples regardless of visibility - visibility is checked at query time
        let schema = table.to_schema();
        let mut current_page_id = table.first_page_id;

        while current_page_id != NO_NEXT_PAGE {
            let page_arc = self.bpm.lock().unwrap().fetch_page(current_page_id)?;
            let page = page_arc.read().unwrap();

            let tuple_count = page.tuple_count();
            for slot_id in 0..tuple_count {
                if let Some(tuple_data) = page.get_tuple(slot_id) {
                    let (_xmin, _xmax, values) = deserialize_tuple_mvcc(tuple_data, &schema)?;

                    // Build index key from specified columns
                    let key_values: Vec<Value> = self
                        .stmt
                        .column_ids
                        .iter()
                        .map(|&col_idx| values[col_idx].clone())
                        .collect();
                    let key = IndexKey::new(key_values);
                    let rid = Rid {
                        page_id: current_page_id,
                        slot_id,
                    };
                    btree.insert(&key, rid)?;
                }
            }

            let next_page_id = page.next_page_id();
            drop(page);
            self.bpm.lock().unwrap().unpin_page(current_page_id, false)?;
            current_page_id = next_page_id;
        }

        // Insert pg_index record
        // pg_index schema: (index_id, index_name, table_id, column_ids, meta_page_id)
        let column_ids_str = self
            .stmt
            .column_ids
            .iter()
            .map(|&id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let pg_index_tuple = serialize_tuple_mvcc(
            xmin,
            xmax,
            &[
                Value::Int(new_index_id as i32),
                Value::Varchar(self.stmt.index_name.clone()),
                Value::Int(self.stmt.table_id as i32),
                Value::Varchar(column_ids_str),
                Value::Int(meta_page_id as i32),
            ],
        );
        let pg_index_rid = self.insert_into_pg_index(&pg_index_tuple)?;

        // Log the insert
        if let Some(ref wal_manager) = self.wal_manager {
            if let Some(ref mut txn) = self.txn {
                let prev_lsn = txn.last_lsn;
                let lsn = wal_manager.append(
                    txn.id,
                    prev_lsn,
                    WalRecordType::Insert {
                        rid: pg_index_rid,
                        data: pg_index_tuple.clone(),
                    },
                );
                txn.set_last_lsn(lsn);

                let page_arc = self.bpm.lock().unwrap().fetch_page_mut(pg_index_rid.page_id)?;
                {
                    let mut page_guard = page_arc.write().unwrap();
                    page_guard.page_lsn = lsn;
                }
                self.bpm
                    .lock()
                    .unwrap()
                    .unpin_page(pg_index_rid.page_id, true)?;

                txn.add_undo_entry(UndoLogEntry::Insert {
                    lsn,
                    prev_lsn,
                    rid: pg_index_rid,
                    data: pg_index_tuple,
                });
            }
        }

        Ok(Some(Tuple::new(vec![Value::Int(1)])))
    }
}
