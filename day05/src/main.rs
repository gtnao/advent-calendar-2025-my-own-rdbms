#![allow(dead_code)]

mod analyzer;
mod ast;
mod buffer_pool;
mod catalog;
mod disk;
mod lexer;
mod page;
mod parser;
mod table;
mod tuple;

use anyhow::Result;

use analyzer::analyze;
use catalog::Catalog;
use parser::parse;

fn main() -> Result<()> {
    let catalog = Catalog::new();

    println!("=== Parser + Analyzer Demo ===\n");

    // Valid queries against 'users' table
    let valid_sqls = vec![
        "SELECT * FROM users",
        "SELECT id, name FROM users",
        "SELECT id FROM users WHERE id > 10",
        "SELECT id FROM users WHERE id = 1 AND name = 'Alice'",
        "SELECT id + 1 FROM users",
        "SELECT 1 + 2 FROM users",
        "INSERT INTO users VALUES (1, 'Alice')",
        "INSERT INTO users VALUES (2, NULL)",
    ];

    for sql in valid_sqls {
        println!("SQL: {sql}");
        match parse(sql) {
            Ok(stmt) => {
                println!("AST: {stmt:#?}");
                match analyze(&catalog, &stmt) {
                    Ok(analyzed) => println!("Analyzed: {analyzed:#?}\n"),
                    Err(e) => println!("Analyze Error: {e}\n"),
                }
            }
            Err(e) => println!("Parse Error: {e}\n"),
        }
    }

    // Error cases
    println!("=== Error Cases ===\n");

    let error_sqls = vec![
        ("SELECT * FROM unknown_table", "table not found"),
        ("SELECT unknown_col FROM users", "column not found"),
        ("INSERT INTO users VALUES (1)", "wrong number of values"),
        ("INSERT INTO users VALUES ('Alice', 1)", "type mismatch"),
        ("INSERT INTO users VALUES (NULL, 'Alice')", "not nullable"),
        ("CREATE TABLE users (id INT)", "table already exists"),
    ];

    for (sql, expected_error) in error_sqls {
        println!("SQL: {sql}");
        println!("Expected: {expected_error}");
        match parse(sql) {
            Ok(stmt) => match analyze(&catalog, &stmt) {
                Ok(analyzed) => println!("Unexpected success: {analyzed:#?}\n"),
                Err(e) => println!("Got: {e}\n"),
            },
            Err(e) => println!("Parse Error: {e}\n"),
        }
    }

    Ok(())
}
