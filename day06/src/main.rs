#![allow(dead_code)]

mod analyzer;
mod ast;
mod buffer_pool;
mod catalog;
mod disk;
mod executor;
mod lexer;
mod page;
mod parser;
mod table;
mod tuple;

use anyhow::Result;

use analyzer::analyze;
use buffer_pool::BufferPoolManager;
use catalog::Catalog;
use disk::DiskManager;
use executor::ExecutionEngine;
use parser::parse;

const DATA_FILE: &str = "table.db";

fn execute_sql(sql: &str, catalog: &Catalog, bpm: &mut BufferPoolManager) -> Result<()> {
    let stmt = parse(sql)?;
    let analyzed = analyze(catalog, &stmt)?;
    let mut engine = ExecutionEngine::new(bpm, catalog, &analyzed)?;
    let results = engine.execute()?;
    for row in &results {
        println!("  {row:?}");
    }
    Ok(())
}

fn main() -> Result<()> {
    let catalog = Catalog::new();

    // Create fresh database file
    let _ = std::fs::remove_file(DATA_FILE);
    let disk_manager = DiskManager::open(DATA_FILE)?;
    let mut bpm = BufferPoolManager::new(disk_manager);

    println!("=== Volcano Executor Demo ===\n");

    // Insert some data
    println!("Inserting data...");
    execute_sql("INSERT INTO users VALUES (1, 'Alice')", &catalog, &mut bpm)?;
    execute_sql("INSERT INTO users VALUES (2, 'Bob')", &catalog, &mut bpm)?;
    execute_sql("INSERT INTO users VALUES (3, 'Charlie')", &catalog, &mut bpm)?;
    execute_sql("INSERT INTO users VALUES (10, 'Dave')", &catalog, &mut bpm)?;
    execute_sql("INSERT INTO users VALUES (20, 'Eve')", &catalog, &mut bpm)?;
    println!("Inserted 5 rows.\n");

    // SELECT *
    println!("SQL: SELECT * FROM users");
    execute_sql("SELECT * FROM users", &catalog, &mut bpm)?;
    println!();

    // SELECT with specific columns
    println!("SQL: SELECT name FROM users");
    execute_sql("SELECT name FROM users", &catalog, &mut bpm)?;
    println!();

    // SELECT with WHERE
    println!("SQL: SELECT * FROM users WHERE id > 5");
    execute_sql("SELECT * FROM users WHERE id > 5", &catalog, &mut bpm)?;
    println!();

    // SELECT with complex WHERE
    println!("SQL: SELECT id, name FROM users WHERE id >= 2 AND id <= 10");
    execute_sql(
        "SELECT id, name FROM users WHERE id >= 2 AND id <= 10",
        &catalog,
        &mut bpm,
    )?;
    println!();

    // SELECT with expression
    println!("SQL: SELECT id + 1 FROM users");
    execute_sql("SELECT id + 1 FROM users", &catalog, &mut bpm)?;
    println!();

    // Flush to disk
    bpm.flush_all()?;
    println!("Data flushed to disk.");

    Ok(())
}
