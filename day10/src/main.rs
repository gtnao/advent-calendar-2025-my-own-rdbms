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

use anyhow::Result;

use instance::Instance;

fn main() -> Result<()> {
    let instance = Instance::new()?;
    instance.start()
}
