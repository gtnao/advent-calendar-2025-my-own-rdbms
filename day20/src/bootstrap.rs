use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::buffer_pool::BufferPoolManager;
use crate::page::NO_NEXT_PAGE;
use crate::transaction_manager::{TransactionManager, TxnStatus};
use crate::tuple::{serialize_tuple_mvcc, Value};

// System table IDs
pub const PG_CLASS_TABLE_ID: u32 = 0;
pub const PG_ATTRIBUTE_TABLE_ID: u32 = 1;
pub const PG_INDEX_TABLE_ID: u32 = 2;

// System table page IDs
pub const PG_CLASS_PAGE_ID: u32 = 0;
pub const PG_ATTRIBUTE_PAGE_ID: u32 = 1;
pub const PG_INDEX_PAGE_ID: u32 = 2;

// Data type constants for pg_attribute
pub const DATA_TYPE_INT: i32 = 0;
pub const DATA_TYPE_VARCHAR: i32 = 1;
pub const DATA_TYPE_BOOL: i32 = 2;

/// System transaction ID used for bootstrap (reserved)
pub const SYSTEM_TXN_ID: u64 = 1;

/// Bootstrap the database by creating system catalog tables.
/// This should only be called on a fresh database (init=true).
pub fn bootstrap(bpm: &Arc<Mutex<BufferPoolManager>>, txn_manager: &TransactionManager) -> Result<()> {
    println!("[Bootstrap] Initializing system catalog...");

    let mut bpm_guard = bpm.lock().unwrap();

    // Create page 0 for pg_class
    let (pg_class_page_id, pg_class_page_arc) = bpm_guard.new_page()?;
    assert_eq!(pg_class_page_id, PG_CLASS_PAGE_ID);
    {
        let mut page = pg_class_page_arc.write().unwrap();
        page.set_next_page_id(NO_NEXT_PAGE);

        // Insert pg_class entries
        // pg_class schema: (table_id: Int, name: Varchar, first_page_id: Int)
        // Using xmin=SYSTEM_TXN_ID, xmax=0 (system transaction, not deleted)

        // Entry for pg_class itself
        let pg_class_tuple = serialize_tuple_mvcc(
            SYSTEM_TXN_ID, // xmin
            0,             // xmax
            &[
                Value::Int(PG_CLASS_TABLE_ID as i32),
                Value::Varchar("pg_class".to_string()),
                Value::Int(PG_CLASS_PAGE_ID as i32),
            ],
        );
        page.insert(&pg_class_tuple)?;

        // Entry for pg_attribute
        let pg_attribute_tuple = serialize_tuple_mvcc(
            SYSTEM_TXN_ID, // xmin
            0,             // xmax
            &[
                Value::Int(PG_ATTRIBUTE_TABLE_ID as i32),
                Value::Varchar("pg_attribute".to_string()),
                Value::Int(PG_ATTRIBUTE_PAGE_ID as i32),
            ],
        );
        page.insert(&pg_attribute_tuple)?;

        // Entry for pg_index
        let pg_index_tuple = serialize_tuple_mvcc(
            SYSTEM_TXN_ID, // xmin
            0,             // xmax
            &[
                Value::Int(PG_INDEX_TABLE_ID as i32),
                Value::Varchar("pg_index".to_string()),
                Value::Int(PG_INDEX_PAGE_ID as i32),
            ],
        );
        page.insert(&pg_index_tuple)?;
    }
    bpm_guard.unpin_page(pg_class_page_id, true)?;

    // Create page 1 for pg_attribute
    let (pg_attribute_page_id, pg_attribute_page_arc) = bpm_guard.new_page()?;
    assert_eq!(pg_attribute_page_id, PG_ATTRIBUTE_PAGE_ID);
    {
        let mut page = pg_attribute_page_arc.write().unwrap();
        page.set_next_page_id(NO_NEXT_PAGE);

        // Insert pg_attribute entries
        // pg_attribute schema: (table_id: Int, column_name: Varchar, data_type: Int, nullable: Bool, ordinal_position: Int)

        // Columns for pg_class (table_id=0)
        let columns = [
            (PG_CLASS_TABLE_ID, "table_id", DATA_TYPE_INT, false, 0),
            (PG_CLASS_TABLE_ID, "name", DATA_TYPE_VARCHAR, false, 1),
            (PG_CLASS_TABLE_ID, "first_page_id", DATA_TYPE_INT, false, 2),
            // Columns for pg_attribute (table_id=1)
            (PG_ATTRIBUTE_TABLE_ID, "table_id", DATA_TYPE_INT, false, 0),
            (PG_ATTRIBUTE_TABLE_ID, "column_name", DATA_TYPE_VARCHAR, false, 1),
            (PG_ATTRIBUTE_TABLE_ID, "data_type", DATA_TYPE_INT, false, 2),
            (PG_ATTRIBUTE_TABLE_ID, "nullable", DATA_TYPE_BOOL, false, 3),
            (PG_ATTRIBUTE_TABLE_ID, "ordinal_position", DATA_TYPE_INT, false, 4),
            // Columns for pg_index (table_id=2)
            // pg_index schema: (index_id, index_name, table_id, column_ids, root_page_id)
            (PG_INDEX_TABLE_ID, "index_id", DATA_TYPE_INT, false, 0),
            (PG_INDEX_TABLE_ID, "index_name", DATA_TYPE_VARCHAR, false, 1),
            (PG_INDEX_TABLE_ID, "table_id", DATA_TYPE_INT, false, 2),
            (PG_INDEX_TABLE_ID, "column_ids", DATA_TYPE_VARCHAR, false, 3), // comma-separated
            (PG_INDEX_TABLE_ID, "root_page_id", DATA_TYPE_INT, false, 4),
        ];

        for (table_id, col_name, data_type, nullable, ordinal) in columns {
            let tuple = serialize_tuple_mvcc(
                SYSTEM_TXN_ID, // xmin
                0,             // xmax
                &[
                    Value::Int(table_id as i32),
                    Value::Varchar(col_name.to_string()),
                    Value::Int(data_type),
                    Value::Bool(nullable),
                    Value::Int(ordinal),
                ],
            );
            page.insert(&tuple)?;
        }
    }
    bpm_guard.unpin_page(pg_attribute_page_id, true)?;

    // Create page 2 for pg_index (empty initially)
    let (pg_index_page_id, pg_index_page_arc) = bpm_guard.new_page()?;
    assert_eq!(pg_index_page_id, PG_INDEX_PAGE_ID);
    {
        let mut page = pg_index_page_arc.write().unwrap();
        page.set_next_page_id(NO_NEXT_PAGE);
        // pg_index is empty initially - indexes are created via CREATE INDEX
    }
    bpm_guard.unpin_page(pg_index_page_id, true)?;

    // Flush to disk
    bpm_guard.flush_all()?;

    // Mark system transaction as committed in CLOG
    txn_manager.set_txn_status(SYSTEM_TXN_ID, TxnStatus::Committed);

    // Set next transaction ID to 2 (1 is reserved for system)
    txn_manager.set_next_txn_id(SYSTEM_TXN_ID + 1);

    println!("[Bootstrap] System catalog initialized.");
    println!("[Bootstrap]   - pg_class (table_id=0, page_id=0)");
    println!("[Bootstrap]   - pg_attribute (table_id=1, page_id=1)");
    println!("[Bootstrap]   - pg_index (table_id=2, page_id=2)");

    Ok(())
}
