mod buffer_pool;
mod disk;
mod page;
mod table;
mod tuple;

use anyhow::Result;

use buffer_pool::BufferPoolManager;
use disk::DiskManager;
use table::Table;
use tuple::{Column, DataType, Schema, Value};

const DATA_FILE: &str = "table.db";

fn main() -> Result<()> {
    let _ = std::fs::remove_file(DATA_FILE);

    let schema = Schema {
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: DataType::Int,
            },
            Column {
                name: "name".to_string(),
                data_type: DataType::Varchar,
            },
        ],
    };

    let disk_manager = DiskManager::open(DATA_FILE)?;
    let bpm = BufferPoolManager::new(disk_manager);
    let mut table = Table::new(bpm, schema);

    // Insert tuples to test buffer pool eviction
    for i in 1..=15 {
        let name = format!("User{i}");
        let loc = table.insert(&[Value::Int(i), Value::Varchar(name)])?;
        println!("Inserted at: {loc:?}");
    }

    println!("\nTotal pages: {}", table.page_count());

    // Flush all dirty pages
    table.flush()?;

    let tuples = table.scan()?;
    println!("\nScanned {} tuples:", tuples.len());
    for values in tuples {
        println!("  {values:?}");
    }

    Ok(())
}
