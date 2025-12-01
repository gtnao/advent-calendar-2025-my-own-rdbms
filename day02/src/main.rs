mod disk;
mod page;
mod table;
mod tuple;

use anyhow::Result;

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
    let mut table = Table::new(disk_manager, schema);

    let loc1 = table.insert(&[Value::Int(1), Value::Varchar("Alice".to_string())])?;
    let loc2 = table.insert(&[Value::Int(2), Value::Varchar("Bob".to_string())])?;
    let loc3 = table.insert(&[Value::Int(3), Value::Varchar("Charlie".to_string())])?;
    let loc4 = table.insert(&[Value::Int(4), Value::Varchar("Dave".to_string())])?;
    let loc5 = table.insert(&[Value::Int(5), Value::Varchar("Eve".to_string())])?;

    println!("Inserted at: {loc1:?}, {loc2:?}, {loc3:?}, {loc4:?}, {loc5:?}");
    println!("Total pages: {}", table.page_count());

    let tuples = table.scan()?;
    println!("Scanned {} tuples:", tuples.len());
    for values in tuples {
        println!("  {values:?}");
    }

    // Show page info (debug)
    let mut debug_disk = DiskManager::open(DATA_FILE)?;
    for page_id in 0..table.page_count() {
        let mut buf = [0u8; page::PAGE_SIZE];
        debug_disk.read_page(page_id, &mut buf)?;
        let page = page::Page::from_bytes(&buf);
        println!(
            "Page {}: tuple_count={}, free_space_offset={}, free_space={}",
            page.page_id(),
            page.tuple_count(),
            page.free_space_offset(),
            page.free_space()
        );
    }

    Ok(())
}
