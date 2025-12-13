mod analyzer;
mod ast;
mod buffer_pool;
mod catalog;
mod disk;
mod executor;
mod instance;
mod lexer;
mod lock_manager;
mod page;
mod parser;
mod protocol;
mod transaction;
mod tuple;
mod wal;

use std::env;

use anyhow::Result;

use instance::Instance;

fn main() -> Result<()> {
    let init = env::args().any(|arg| arg == "--init");
    let instance = Instance::new(init)?;
    instance.start()
}
