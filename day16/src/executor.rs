use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::analyzer::{
    AnalyzedAssignment, AnalyzedCreateTableStatement, AnalyzedExpr, AnalyzedInsertStatement,
    AnalyzedLiteral, AnalyzedSelectStatement, AnalyzedStatement, LiteralValue, TableSource,
};
use crate::ast::{BinaryOperator, UnaryOperator};
use crate::bootstrap::{
    DATA_TYPE_BOOL, DATA_TYPE_INT, DATA_TYPE_VARCHAR, PG_ATTRIBUTE_TABLE_ID, PG_CLASS_TABLE_ID,
};
use crate::buffer_pool::BufferPoolManager;
use crate::catalog::Catalog;
use crate::lock_manager::{LockManager, LockMode};
use crate::page::NO_NEXT_PAGE;
use crate::transaction::{Transaction, UndoLogEntry};
use crate::transaction_manager::TransactionManager;
use crate::tuple::{
    deserialize_tuple_mvcc, serialize_tuple_mvcc,
    DataType, Value, TxnId, INVALID_TXN_ID,
};
use crate::visibility::{Snapshot, is_tuple_visible};
use crate::wal::{CLRRedo, Lsn, WalManager, WalRecordType};

// Row ID: page_id + slot_id
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rid {
    pub page_id: u32,
    pub slot_id: u16,
}

impl std::hash::Hash for Rid {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.page_id.hash(state);
        self.slot_id.hash(state);
    }
}

// Runtime representation of a row during query execution.
// (tuple.rs handles serialization/deserialization for storage)
#[derive(Debug, Clone)]
pub struct Tuple {
    pub values: Vec<Value>,
    pub rid: Option<Rid>,
}

impl Tuple {
    pub fn new(values: Vec<Value>) -> Self {
        Tuple { values, rid: None }
    }

    pub fn with_rid(values: Vec<Value>, rid: Rid) -> Self {
        Tuple {
            values,
            rid: Some(rid),
        }
    }
}

// Volcano model: open/next/close interface
pub trait Executor {
    fn open(&mut self) -> Result<()>;
    fn next(&mut self) -> Result<Option<Tuple>>;
}

// Sequential scan executor
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
    // MVCC: snapshot for visibility check
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
        // Start from table's first page
        let table = self
            .catalog
            .get_table_by_id(self.table_id)
            .ok_or_else(|| anyhow::anyhow!("table not found"))?;
        self.current_page_id = table.first_page_id;
        self.current_slot_id = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        use crate::page::NO_NEXT_PAGE;

        let table = self
            .catalog
            .get_table_by_id(self.table_id)
            .ok_or_else(|| anyhow::anyhow!("table not found"))?;
        let schema = table.to_schema();

        loop {
            // End of table pages
            if self.current_page_id == NO_NEXT_PAGE {
                return Ok(None);
            }

            // Get tuple count and next_page_id for current page
            let page_arc = self.bpm.lock().unwrap().fetch_page(self.current_page_id)?;
            let (tuple_count, next_page_id) = {
                let page_guard = page_arc.read().unwrap();
                (page_guard.tuple_count(), page_guard.next_page_id())
            };
            self.bpm.lock().unwrap().unpin_page(self.current_page_id, false)?;

            if self.current_slot_id >= tuple_count {
                // Move to next page in the chain
                self.current_page_id = next_page_id;
                self.current_slot_id = 0;
                continue;
            }

            let rid = Rid {
                page_id: self.current_page_id,
                slot_id: self.current_slot_id,
            };

            // MVCC: No shared lock needed for reads!
            // Instead, we use visibility check based on snapshot.

            // Fetch page and check if tuple exists
            let page_arc = self.bpm.lock().unwrap().fetch_page(self.current_page_id)?;
            let result = {
                let page_guard = page_arc.read().unwrap();
                if let Some(tuple_data) = page_guard.get_tuple(self.current_slot_id) {
                    // Deserialize with MVCC header (xmin/xmax)
                    let (xmin, xmax, values) = deserialize_tuple_mvcc(tuple_data, &schema)?;

                    // Check visibility using snapshot
                    let is_visible = match (&self.snapshot, &self.txn_manager) {
                        (Some(snapshot), Some(txn_manager)) => {
                            is_tuple_visible(xmin, xmax, snapshot, txn_manager)
                        }
                        _ => true, // No snapshot = see everything (autocommit mode)
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
            self.bpm.lock().unwrap().unpin_page(self.current_page_id, false)?;

            self.current_slot_id += 1;

            if let Some(tuple) = result {
                return Ok(Some(tuple));
            }
            // Tuple was deleted, continue to next slot
        }
    }
}

// Filter executor (WHERE clause)
pub struct FilterExecutor<'a> {
    child: Box<dyn Executor + 'a>,
    predicate: AnalyzedExpr,
}

impl<'a> FilterExecutor<'a> {
    pub fn new(child: Box<dyn Executor + 'a>, predicate: AnalyzedExpr) -> Self {
        FilterExecutor { child, predicate }
    }
}

impl Executor for FilterExecutor<'_> {
    fn open(&mut self) -> Result<()> {
        self.child.open()
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        while let Some(tuple) = self.child.next()? {
            if evaluate_predicate(&self.predicate, &tuple)? {
                // Preserve RID from child
                return Ok(Some(tuple));
            }
        }
        Ok(None)
    }
}

// Projection executor (SELECT expressions)
pub struct ProjectionExecutor<'a> {
    child: Box<dyn Executor + 'a>,
    exprs: Vec<AnalyzedExpr>,
}

impl<'a> ProjectionExecutor<'a> {
    pub fn new(child: Box<dyn Executor + 'a>, exprs: Vec<AnalyzedExpr>) -> Self {
        ProjectionExecutor { child, exprs }
    }
}

impl Executor for ProjectionExecutor<'_> {
    fn open(&mut self) -> Result<()> {
        self.child.open()
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if let Some(tuple) = self.child.next()? {
            let projected: Vec<Value> = self
                .exprs
                .iter()
                .map(|expr| evaluate_expr(expr, &tuple))
                .collect::<Result<Vec<_>>>()?;
            return Ok(Some(Tuple::new(projected)));
        }
        Ok(None)
    }
}

// Insert executor
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
        use crate::page::NO_NEXT_PAGE;

        if self.executed {
            return Ok(None);
        }
        self.executed = true;

        // Get table info
        let table = self.catalog
            .get_table_by_id(self.table_id)
            .ok_or_else(|| anyhow::anyhow!("table not found"))?;

        // Get xmin from transaction (or 0 if no transaction)
        let xmin: TxnId = self.txn.as_ref().map(|t| t.id).unwrap_or(INVALID_TXN_ID);
        let xmax: TxnId = INVALID_TXN_ID; // Not deleted

        let tuple_data = serialize_tuple_mvcc(xmin, xmax, &self.values);

        // Find a page with space in the table's page chain
        let rid = {
            let mut current_page_id = table.first_page_id;
            let mut last_page_id = current_page_id;
            let mut result_rid: Option<Rid> = None;

            // Traverse to find a page with space
            while current_page_id != NO_NEXT_PAGE {
                let page_arc = self.bpm.lock().unwrap().fetch_page_mut(current_page_id)?;
                let mut page_guard = page_arc.write().unwrap();

                // Try to insert into this page
                if let Ok(slot_id) = page_guard.insert(&tuple_data) {
                    drop(page_guard);
                    self.bpm.lock().unwrap().unpin_page(current_page_id, true)?;
                    result_rid = Some(Rid {
                        page_id: current_page_id,
                        slot_id,
                    });
                    break;
                }

                // Page is full, move to next
                last_page_id = current_page_id;
                current_page_id = page_guard.next_page_id();
                drop(page_guard);
                self.bpm.lock().unwrap().unpin_page(last_page_id, false)?;
            }

            match result_rid {
                Some(rid) => rid,
                None => {
                    // All pages are full, allocate a new page and link it
                    let (new_page_id, new_page_arc) = self.bpm.lock().unwrap().new_page()?;
                    let mut new_page = new_page_arc.write().unwrap();
                    new_page.set_next_page_id(NO_NEXT_PAGE);
                    let slot_id = new_page.insert(&tuple_data)?;
                    drop(new_page);
                    self.bpm.lock().unwrap().unpin_page(new_page_id, true)?;

                    // Link the new page to the last page
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

        // Write WAL record, acquire lock, record undo log if in transaction
        if let Some(ref mut txn) = self.txn {
            if txn.is_active() {
                // Write WAL record and update page_lsn
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

                    // Update page_lsn
                    let page_arc = self.bpm.lock().unwrap().fetch_page_mut(rid.page_id)?;
                    let mut page_guard = page_arc.write().unwrap();
                    page_guard.page_lsn = lsn;
                    drop(page_guard);
                    self.bpm.lock().unwrap().unpin_page(rid.page_id, true)?;

                    // Record undo log with LSN info
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

        Ok(Some(Tuple::new(vec![Value::Int(1)])))
    }
}

// Delete executor - with child executor (SeqScan + Filter)
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

        // Get xmax from transaction (or 0 if no transaction)
        let xmax: TxnId = self.txn.as_ref().map(|t| t.id).unwrap_or(INVALID_TXN_ID);

        // Must collect all targets first to avoid "read-while-write" problem
        let mut targets = Vec::new();
        while let Some(tuple) = self.child.next()? {
            if let Some(rid) = tuple.rid {
                targets.push(rid);
            }
        }

        // Acquire exclusive locks before deleting (if in transaction)
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

        // MVCC logical delete: set xmax instead of physical delete
        self.deleted_count = targets.len() as i32;
        for rid in targets {
            let page_arc = self.bpm.lock().unwrap().fetch_page_mut(rid.page_id)?;
            let mut page_guard = page_arc.write().unwrap();

            // MVCC: Set xmax to mark as deleted (logical delete)
            page_guard.set_tuple_xmax(rid.slot_id, xmax)?;

            // Write WAL record and update page_lsn if in transaction
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

                        // Record undo log (reset xmax to 0 on rollback)
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

// Update executor - with child executor (SeqScan + Filter)
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

        // Must collect all updates first to avoid "read-while-write" problem
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

        // Acquire exclusive locks on old RIDs before updating (if in transaction)
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

        // Apply updates and record WAL + undo log
        // MVCC: Use xmax for logical delete of old tuple
        let xmax = self
            .txn
            .as_ref()
            .map(|t| t.id)
            .unwrap_or(INVALID_TXN_ID);

        self.updated_count = updates.len() as i32;
        for (old_rid, new_values) in updates {
            // MVCC: Logical delete old tuple (set xmax)
            let page_arc = self.bpm.lock().unwrap().fetch_page_mut(old_rid.page_id)?;
            let mut page_guard = page_arc.write().unwrap();
            page_guard.set_tuple_xmax(old_rid.slot_id, xmax)?;

            // Write WAL Delete record and update page_lsn
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

            // Insert new tuple with MVCC header
            // xmin = current transaction ID, xmax = INVALID (not deleted)
            let new_xmin = xmax; // Same transaction that's deleting the old tuple
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

            // Write WAL Insert record, acquire lock on new RID, record undo log
            if let Some(ref mut txn) = self.txn {
                if txn.is_active() {
                    // Write WAL Insert record and update page_lsn
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

                        // Record undo log entries with LSN info
                        // Undo delete: reset xmax to 0 (was not deleted)
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

// CREATE TABLE executor
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

    // Convert DataType to data_type constant for pg_attribute
    fn data_type_to_int(dt: &DataType) -> i32 {
        match dt {
            DataType::Int => DATA_TYPE_INT,
            DataType::Varchar => DATA_TYPE_VARCHAR,
            DataType::Bool => DATA_TYPE_BOOL,
        }
    }

    // Insert a tuple into a specific table's page chain
    // Returns (Rid, Option<(new_page_id, prev_page_id)>) if a new page was allocated
    fn insert_into_table(
        &self,
        table_id: u32,
        tuple_data: &[u8],
    ) -> Result<(Rid, Option<(u32, u32)>)> {
        let table = self.catalog
            .get_table_by_id(table_id)
            .ok_or_else(|| anyhow::anyhow!("table not found: {}", table_id))?;

        let mut current_page_id = table.first_page_id;
        let mut last_page_id = current_page_id;

        // Traverse to find a page with space
        while current_page_id != NO_NEXT_PAGE {
            let page_arc = self.bpm.lock().unwrap().fetch_page_mut(current_page_id)?;
            let mut page_guard = page_arc.write().unwrap();

            // Try to insert into this page
            if let Ok(slot_id) = page_guard.insert(tuple_data) {
                drop(page_guard);
                self.bpm.lock().unwrap().unpin_page(current_page_id, true)?;

                return Ok((
                    Rid {
                        page_id: current_page_id,
                        slot_id,
                    },
                    None, // No new page allocated
                ));
            }

            // Page is full, move to next
            last_page_id = current_page_id;
            current_page_id = page_guard.next_page_id();
            drop(page_guard);
            self.bpm.lock().unwrap().unpin_page(last_page_id, false)?;
        }

        // All pages are full, allocate a new page and link it
        let (new_page_id, new_page_arc) = self.bpm.lock().unwrap().new_page()?;
        {
            let mut new_page = new_page_arc.write().unwrap();
            new_page.set_next_page_id(NO_NEXT_PAGE);
            let _slot_id = new_page.insert(tuple_data)?;
        }
        self.bpm.lock().unwrap().unpin_page(new_page_id, true)?;

        // Link the new page to the last page
        let page_arc = self.bpm.lock().unwrap().fetch_page_mut(last_page_id)?;
        {
            let mut page = page_arc.write().unwrap();
            page.set_next_page_id(new_page_id);
        }
        self.bpm.lock().unwrap().unpin_page(last_page_id, true)?;

        // Return the RID of the inserted tuple (it's at slot 0 in the new page)
        Ok((
            Rid {
                page_id: new_page_id,
                slot_id: 0,
            },
            Some((new_page_id, last_page_id)), // New page was allocated
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

        // Get xmin from transaction (or 0 if no transaction)
        let xmin: TxnId = self.txn.as_ref().map(|t| t.id).unwrap_or(INVALID_TXN_ID);
        let xmax: TxnId = INVALID_TXN_ID; // Not deleted

        // 1. Get the next available table ID
        let new_table_id = self.catalog.next_table_id()?;

        // 2. Allocate a new page for the table
        let (new_page_id, new_page_arc) = self.bpm.lock().unwrap().new_page()?;
        {
            let mut new_page = new_page_arc.write().unwrap();
            new_page.set_next_page_id(NO_NEXT_PAGE);

            // Write AllocatePage WAL record if we have wal_manager
            if let Some(ref wal_manager) = self.wal_manager {
                if let Some(ref mut txn) = self.txn {
                    let prev_lsn = txn.last_lsn;
                    let lsn = wal_manager.append(
                        txn.id,
                        prev_lsn,
                        WalRecordType::AllocatePage {
                            page_id: new_page_id,
                            table_id: new_table_id,
                            prev_page_id: NO_NEXT_PAGE, // No previous page for this table
                        },
                    );
                    txn.set_last_lsn(lsn);
                    new_page.page_lsn = lsn;
                }
            }
        }
        self.bpm.lock().unwrap().unpin_page(new_page_id, true)?;

        // 3. Insert a row into pg_class (table_id, name, first_page_id)
        let pg_class_tuple = serialize_tuple_mvcc(
            xmin,
            xmax,
            &[
                Value::Int(new_table_id as i32),
                Value::Varchar(self.stmt.table_name.clone()),
                Value::Int(new_page_id as i32),
            ],
        );
        let (pg_class_rid, _new_page_info) = self.insert_into_table(PG_CLASS_TABLE_ID, &pg_class_tuple)?;

        // Write Insert WAL record for pg_class
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

                // Update page_lsn
                let page_arc = self.bpm.lock().unwrap().fetch_page_mut(pg_class_rid.page_id)?;
                {
                    let mut page_guard = page_arc.write().unwrap();
                    page_guard.page_lsn = lsn;
                }
                self.bpm.lock().unwrap().unpin_page(pg_class_rid.page_id, true)?;

                // Record undo log
                txn.add_undo_entry(UndoLogEntry::Insert {
                    lsn,
                    prev_lsn,
                    rid: pg_class_rid,
                    data: pg_class_tuple,
                });
            }
        }

        // 4. Insert rows into pg_attribute for each column
        for (ordinal, col) in self.stmt.columns.iter().enumerate() {
            let pg_attribute_tuple = serialize_tuple_mvcc(
                xmin,
                xmax,
                &[
                    Value::Int(new_table_id as i32),
                    Value::Varchar(col.name.clone()),
                    Value::Int(Self::data_type_to_int(&col.data_type)),
                    Value::Bool(false), // nullable = false for now
                    Value::Int(ordinal as i32),
                ],
            );
            let (pg_attribute_rid, _new_page_info) = self.insert_into_table(PG_ATTRIBUTE_TABLE_ID, &pg_attribute_tuple)?;

            // Write Insert WAL record for pg_attribute
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

                    // Update page_lsn
                    let page_arc = self.bpm.lock().unwrap().fetch_page_mut(pg_attribute_rid.page_id)?;
                    {
                        let mut page_guard = page_arc.write().unwrap();
                        page_guard.page_lsn = lsn;
                    }
                    self.bpm.lock().unwrap().unpin_page(pg_attribute_rid.page_id, true)?;

                    // Record undo log
                    txn.add_undo_entry(UndoLogEntry::Insert {
                        lsn,
                        prev_lsn,
                        rid: pg_attribute_rid,
                        data: pg_attribute_tuple,
                    });
                }
            }
        }

        // Return success (1 table created)
        Ok(Some(Tuple::new(vec![Value::Int(1)])))
    }
}

// Expression evaluation
pub fn evaluate_expr(expr: &AnalyzedExpr, tuple: &Tuple) -> Result<Value> {
    match expr {
        AnalyzedExpr::Literal(lit) => Ok(literal_to_value(lit)),
        AnalyzedExpr::ColumnRef(col_ref) => Ok(tuple.values[col_ref.column_index].clone()),
        AnalyzedExpr::BinaryOp {
            left,
            op,
            right,
            ..
        } => {
            let left_val = evaluate_expr(left, tuple)?;
            let right_val = evaluate_expr(right, tuple)?;
            evaluate_binary_op(op, &left_val, &right_val)
        }
        AnalyzedExpr::UnaryOp { op, expr, .. } => {
            let val = evaluate_expr(expr, tuple)?;
            evaluate_unary_op(op, &val)
        }
    }
}

fn literal_to_value(lit: &AnalyzedLiteral) -> Value {
    match &lit.value {
        LiteralValue::Integer(n) => Value::Int(*n as i32),
        LiteralValue::String(s) => Value::Varchar(s.clone()),
        LiteralValue::Boolean(b) => Value::Bool(*b),
        LiteralValue::Null => Value::Null,
    }
}

fn evaluate_binary_op(op: &BinaryOperator, left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Int(l), Value::Int(r)) => {
            let result = match op {
                BinaryOperator::Eq => Value::Bool(*l == *r),
                BinaryOperator::Ne => Value::Bool(*l != *r),
                BinaryOperator::Lt => Value::Bool(*l < *r),
                BinaryOperator::Le => Value::Bool(*l <= *r),
                BinaryOperator::Gt => Value::Bool(*l > *r),
                BinaryOperator::Ge => Value::Bool(*l >= *r),
                BinaryOperator::Add => Value::Int(l + r),
                BinaryOperator::Sub => Value::Int(l - r),
                BinaryOperator::Mul => Value::Int(l * r),
                BinaryOperator::Div => {
                    if *r == 0 {
                        anyhow::bail!("division by zero");
                    }
                    Value::Int(l / r)
                }
                BinaryOperator::And | BinaryOperator::Or => {
                    anyhow::bail!("AND/OR not supported for Int")
                }
            };
            Ok(result)
        }
        (Value::Bool(l), Value::Bool(r)) => {
            let result = match op {
                BinaryOperator::And => Value::Bool(*l && *r),
                BinaryOperator::Or => Value::Bool(*l || *r),
                BinaryOperator::Eq => Value::Bool(*l == *r),
                BinaryOperator::Ne => Value::Bool(*l != *r),
                _ => anyhow::bail!("unsupported operation for Bool"),
            };
            Ok(result)
        }
        (Value::Varchar(l), Value::Varchar(r)) => {
            let result = match op {
                BinaryOperator::Eq => Value::Bool(l == r),
                BinaryOperator::Ne => Value::Bool(l != r),
                BinaryOperator::Lt => Value::Bool(l < r),
                BinaryOperator::Le => Value::Bool(l <= r),
                BinaryOperator::Gt => Value::Bool(l > r),
                BinaryOperator::Ge => Value::Bool(l >= r),
                _ => anyhow::bail!("unsupported operation for VARCHAR"),
            };
            Ok(result)
        }
        _ => anyhow::bail!("type mismatch in binary operation"),
    }
}

fn evaluate_unary_op(op: &UnaryOperator, val: &Value) -> Result<Value> {
    match (op, val) {
        (UnaryOperator::Not, Value::Bool(b)) => Ok(Value::Bool(!b)),
        (UnaryOperator::Neg, Value::Int(v)) => Ok(Value::Int(-v)),
        _ => anyhow::bail!("unsupported unary operation"),
    }
}

fn evaluate_predicate(expr: &AnalyzedExpr, tuple: &Tuple) -> Result<bool> {
    let val = evaluate_expr(expr, tuple)?;
    match val {
        Value::Bool(b) => Ok(b),
        _ => anyhow::bail!("predicate must evaluate to boolean"),
    }
}

// Execution Engine - orchestrates query execution
pub struct ExecutionEngine<'a> {
    executor: Box<dyn Executor + 'a>,
}

impl<'a> ExecutionEngine<'a> {
    pub fn new(
        bpm: Arc<Mutex<BufferPoolManager>>,
        catalog: &'a Catalog,
        stmt: &AnalyzedStatement,
        txn: Option<&'a mut Transaction>,
        lock_manager: Option<&'a LockManager>,
        wal_manager: Option<Arc<WalManager>>,
        txn_manager: Option<&'a TransactionManager>,
    ) -> Result<Self> {
        let executor = Self::build_executor(bpm, catalog, stmt, txn, lock_manager, wal_manager, txn_manager)?;
        Ok(ExecutionEngine { executor })
    }

    pub fn execute(&mut self) -> Result<Vec<Tuple>> {
        self.executor.open()?;
        let mut results = Vec::new();
        while let Some(tuple) = self.executor.next()? {
            results.push(tuple);
        }
        Ok(results)
    }

    // Perform rollback by applying undo log in reverse order, writing CLRs
    pub fn perform_rollback(
        bpm: &Arc<Mutex<BufferPoolManager>>,
        wal_manager: &Arc<WalManager>,
        txn_id: u64,
        undo_log: Vec<UndoLogEntry>,
        mut last_lsn: Lsn,
    ) -> Result<Lsn> {
        for entry in undo_log.into_iter().rev() {
            match entry {
                UndoLogEntry::Insert { prev_lsn, rid, .. } => {
                    // Undo INSERT by deleting the tuple
                    let page_arc = bpm.lock().unwrap().fetch_page_mut(rid.page_id)?;
                    let mut page_guard = page_arc.write().unwrap();
                    page_guard.delete(rid.slot_id)?;

                    // Write CLR
                    let clr_lsn = wal_manager.append(
                        txn_id,
                        last_lsn,
                        WalRecordType::CLR {
                            undo_next_lsn: prev_lsn,
                            redo: CLRRedo::UndoInsert { rid },
                        },
                    );
                    page_guard.page_lsn = clr_lsn;
                    last_lsn = clr_lsn;

                    drop(page_guard);
                    bpm.lock().unwrap().unpin_page(rid.page_id, true)?;
                }
                UndoLogEntry::Delete { prev_lsn, rid, old_xmax, .. } => {
                    // Undo DELETE by restoring old xmax value
                    let page_arc = bpm.lock().unwrap().fetch_page_mut(rid.page_id)?;
                    let mut page_guard = page_arc.write().unwrap();
                    page_guard.set_tuple_xmax(rid.slot_id, old_xmax)?;

                    // Write CLR
                    let clr_lsn = wal_manager.append(
                        txn_id,
                        last_lsn,
                        WalRecordType::CLR {
                            undo_next_lsn: prev_lsn,
                            redo: CLRRedo::UndoDelete { rid, old_xmax },
                        },
                    );
                    page_guard.page_lsn = clr_lsn;
                    last_lsn = clr_lsn;

                    drop(page_guard);
                    bpm.lock().unwrap().unpin_page(rid.page_id, true)?;
                }
            }
        }
        Ok(last_lsn)
    }

    fn build_executor(
        bpm: Arc<Mutex<BufferPoolManager>>,
        catalog: &'a Catalog,
        stmt: &AnalyzedStatement,
        txn: Option<&'a mut Transaction>,
        lock_manager: Option<&'a LockManager>,
        wal_manager: Option<Arc<WalManager>>,
        txn_manager: Option<&'a TransactionManager>,
    ) -> Result<Box<dyn Executor + 'a>> {
        match stmt {
            AnalyzedStatement::Select(s) => Ok(Self::build_select_executor(bpm, catalog, s, txn, lock_manager, txn_manager)),
            AnalyzedStatement::Insert(s) => {
                Ok(Box::new(InsertExecutor::new(bpm, catalog, s, txn, lock_manager, wal_manager)?))
            }
            AnalyzedStatement::CreateTable(s) => {
                Ok(Box::new(CreateTableExecutor::new(bpm, catalog, s, txn, lock_manager, wal_manager)))
            }
            AnalyzedStatement::Delete(s) => {
                Ok(Self::build_delete_executor(bpm, catalog, s, txn, lock_manager, wal_manager, txn_manager))
            }
            AnalyzedStatement::Update(s) => {
                Ok(Self::build_update_executor(bpm, catalog, s, txn, lock_manager, wal_manager, txn_manager))
            }
        }
    }

    fn build_select_executor(
        bpm: Arc<Mutex<BufferPoolManager>>,
        catalog: &'a Catalog,
        stmt: &AnalyzedSelectStatement,
        txn: Option<&'a mut Transaction>,
        lock_manager: Option<&'a LockManager>,
        txn_manager: Option<&'a TransactionManager>,
    ) -> Box<dyn Executor + 'a> {
        // Get table_id from RTE
        let rte = &stmt.range_table[stmt.from_rte_index];
        let table_id = match &rte.source {
            TableSource::BaseTable { table_id, .. } => *table_id,
        };

        // Get snapshot for MVCC visibility check (REPEATABLE READ)
        // Use transaction's snapshot if available (taken at BEGIN)
        // For autocommit SELECT, create a new snapshot
        let snapshot = match &txn {
            Some(t) if t.snapshot.is_some() => t.snapshot.clone(),
            _ => txn_manager.map(|tm| {
                let txn_id = txn.as_ref().map(|t| t.id).unwrap_or(0);
                tm.get_snapshot(txn_id)
            }),
        };

        // SeqScan with MVCC visibility check
        let scan: Box<dyn Executor> = Box::new(SeqScanExecutor::new(
            bpm,
            catalog,
            table_id,
            txn,
            lock_manager,
            snapshot,
            txn_manager,
        ));

        // Filter (if WHERE clause exists)
        let filtered: Box<dyn Executor> = if let Some(pred) = &stmt.where_clause {
            Box::new(FilterExecutor::new(scan, pred.clone()))
        } else {
            scan
        };

        // Projection - evaluate expressions from select_items
        let exprs: Vec<AnalyzedExpr> = stmt
            .select_items
            .iter()
            .map(|item| item.expr.clone())
            .collect();

        Box::new(ProjectionExecutor::new(filtered, exprs))
    }

    fn build_delete_executor(
        bpm: Arc<Mutex<BufferPoolManager>>,
        catalog: &'a Catalog,
        stmt: &crate::analyzer::AnalyzedDeleteStatement,
        txn: Option<&'a mut Transaction>,
        lock_manager: Option<&'a LockManager>,
        wal_manager: Option<Arc<WalManager>>,
        txn_manager: Option<&'a TransactionManager>,
    ) -> Box<dyn Executor + 'a> {
        let table_id = Self::get_table_id_from_range_table(&stmt.range_table);

        // Get snapshot for MVCC visibility check (REPEATABLE READ)
        // Use transaction's snapshot if available (taken at BEGIN)
        let snapshot = match &txn {
            Some(t) if t.snapshot.is_some() => t.snapshot.clone(),
            _ => txn_manager.map(|tm| {
                let txn_id = txn.as_ref().map(|t| t.id).unwrap_or(0);
                tm.get_snapshot(txn_id)
            }),
        };

        // SeqScan with MVCC visibility (no lock - lock acquired in DeleteExecutor)
        let scan: Box<dyn Executor> =
            Box::new(SeqScanExecutor::new(Arc::clone(&bpm), catalog, table_id, None, None, snapshot, txn_manager));

        // Filter (if WHERE clause exists)
        let filtered: Box<dyn Executor> = if let Some(pred) = &stmt.where_clause {
            Box::new(FilterExecutor::new(scan, pred.clone()))
        } else {
            scan
        };

        Box::new(DeleteExecutor::new(bpm, filtered, txn, lock_manager, wal_manager))
    }

    fn build_update_executor(
        bpm: Arc<Mutex<BufferPoolManager>>,
        catalog: &'a Catalog,
        stmt: &crate::analyzer::AnalyzedUpdateStatement,
        txn: Option<&'a mut Transaction>,
        lock_manager: Option<&'a LockManager>,
        wal_manager: Option<Arc<WalManager>>,
        txn_manager: Option<&'a TransactionManager>,
    ) -> Box<dyn Executor + 'a> {
        let table_id = Self::get_table_id_from_range_table(&stmt.range_table);

        // Get snapshot for MVCC visibility check (REPEATABLE READ)
        // Use transaction's snapshot if available (taken at BEGIN)
        // For autocommit UPDATE, create a new snapshot
        let snapshot = match &txn {
            Some(t) if t.snapshot.is_some() => t.snapshot.clone(),
            _ => txn_manager.map(|tm| {
                let txn_id = txn.as_ref().map(|t| t.id).unwrap_or(0);
                tm.get_snapshot(txn_id)
            }),
        };

        // SeqScan with MVCC visibility (no lock - lock acquired in UpdateExecutor)
        let scan: Box<dyn Executor> =
            Box::new(SeqScanExecutor::new(Arc::clone(&bpm), catalog, table_id, None, None, snapshot, txn_manager));

        // Filter (if WHERE clause exists)
        let filtered: Box<dyn Executor> = if let Some(pred) = &stmt.where_clause {
            Box::new(FilterExecutor::new(scan, pred.clone()))
        } else {
            scan
        };

        Box::new(UpdateExecutor::new(
            bpm,
            filtered,
            stmt.assignments.clone(),
            txn,
            lock_manager,
            wal_manager,
        ))
    }

    fn get_table_id_from_range_table(
        range_table: &[crate::analyzer::RangeTableEntry],
    ) -> u32 {
        let rte = &range_table[0];
        match &rte.source {
            TableSource::BaseTable { table_id, .. } => *table_id,
        }
    }
}
