#![allow(dead_code)]

mod ast;
mod buffer_pool;
mod disk;
mod lexer;
mod page;
mod parser;
mod table;
mod tuple;

use anyhow::Result;

use parser::parse;

fn main() -> Result<()> {
    let sqls = vec![
        "SELECT * FROM users",
        "SELECT id, name FROM users WHERE id > 10",
        "SELECT id FROM users WHERE id = 1 AND name = 'Alice'",
        "INSERT INTO users VALUES (1, 'Alice')",
        "CREATE TABLE users (id INT, name VARCHAR)",
        "SELECT a + b * c FROM t",
        "SELECT * FROM t WHERE NOT (x = 1 OR y = 2)",
        "SELECT * FROM t WHERE active = TRUE",
        "SELECT * FROM t WHERE deleted = FALSE",
        "SELECT * FROM t WHERE a <> b",
    ];

    for sql in sqls {
        println!("SQL: {sql}");
        match parse(sql) {
            Ok(stmt) => println!("AST: {stmt:#?}\n"),
            Err(e) => println!("Error: {e}\n"),
        }
    }

    // Test error case
    println!("SQL: SELECT FROM");
    match parse("SELECT FROM") {
        Ok(stmt) => println!("AST: {stmt:#?}\n"),
        Err(e) => println!("Error: {e}\n"),
    }

    Ok(())
}
