mod analyzer;
mod ast;
mod buffer_pool;
mod catalog;
mod disk;
mod executor;
mod instance;
mod lexer;
mod page;
mod parser;
mod protocol;
mod tuple;

use anyhow::Result;

use instance::Instance;

fn main() -> Result<()> {
    let mut instance = Instance::new()?;
    instance.start()
}
