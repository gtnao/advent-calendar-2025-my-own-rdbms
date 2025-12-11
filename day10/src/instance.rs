use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;

use anyhow::Result;

use crate::analyzer::{analyze, AnalyzedExpr, AnalyzedSelectItem, AnalyzedStatement};
use crate::ast::Statement;
use crate::buffer_pool::BufferPoolManager;
use crate::catalog::Catalog;
use crate::disk::DiskManager;
use crate::executor::{ExecutionEngine, Tuple};
use crate::lock_manager::LockManager;
use crate::parser::parse;
use crate::protocol::{ColumnDesc, Connection, FrontendMessage};
use crate::transaction::Transaction;
use crate::tuple::{DataType, Value};

const DATA_FILE: &str = "table.db";
const PORT: u16 = 5433;

pub struct Instance {
    catalog: Arc<Catalog>,
    bpm: Arc<Mutex<BufferPoolManager>>,
    lock_manager: Arc<LockManager>,
}

struct QueryResult {
    columns: Vec<ColumnDesc>,
    rows: Vec<Vec<Option<String>>>,
    command_tag: String,
}

// Result of executing a statement
enum ExecuteResult {
    Query(QueryResult),
    Begin,
    Commit,
    Rollback,
}

impl Instance {
    pub fn new() -> Result<Self> {
        let catalog = Arc::new(Catalog::new());

        // Create fresh database file
        let _ = std::fs::remove_file(DATA_FILE);
        let disk_manager = DiskManager::open(DATA_FILE)?;
        let bpm = Arc::new(Mutex::new(BufferPoolManager::new(disk_manager)));
        let lock_manager = Arc::new(LockManager::new());

        Ok(Instance {
            catalog,
            bpm,
            lock_manager,
        })
    }

    pub fn start(&self) -> Result<()> {
        let listener = TcpListener::bind(format!("127.0.0.1:{PORT}"))?;
        println!("=== PostgreSQL Wire Protocol Server (Multi-threaded) ===");
        println!("Listening on port {PORT}");
        println!("Connect with: psql -h localhost -p {PORT}");
        println!();

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    println!("[Server] New connection from {:?}", stream.peer_addr());
                    let conn = Connection::new(stream);

                    // Clone Arc references for the new thread
                    let catalog = Arc::clone(&self.catalog);
                    let bpm = Arc::clone(&self.bpm);
                    let lock_manager = Arc::clone(&self.lock_manager);

                    // Spawn a new thread for each connection
                    thread::spawn(move || {
                        if let Err(e) = Self::handle_client(conn, catalog, bpm, lock_manager) {
                            println!("[Server] Error handling client: {e}");
                        }
                    });
                }
                Err(e) => {
                    println!("[Server] Accept error: {e}");
                }
            }
        }

        Ok(())
    }

    fn handle_client(
        mut conn: Connection,
        catalog: Arc<Catalog>,
        bpm: Arc<Mutex<BufferPoolManager>>,
        lock_manager: Arc<LockManager>,
    ) -> Result<()> {
        // Read startup message
        let startup = conn.read_startup()?;
        println!(
            "[Server] Startup: {:?} (thread: {:?})",
            startup.params,
            thread::current().id()
        );

        // Send authentication OK
        conn.send_auth_ok()?;

        // Send some parameter status messages
        conn.send_parameter_status("server_version", "0.0.1")?;
        conn.send_parameter_status("client_encoding", "UTF8")?;

        // Send backend key data
        conn.send_backend_key_data(1, 12345)?;

        // Ready for query
        conn.send_ready_for_query()?;

        // Transaction state for this connection
        let mut txn = Transaction::new();

        // Main query loop
        loop {
            match conn.read_message()? {
                Some(FrontendMessage::Query(sql)) => {
                    println!(
                        "[Server] Query: {sql} (thread: {:?})",
                        thread::current().id()
                    );

                    if sql.trim().is_empty() {
                        conn.send_empty_query()?;
                        conn.send_ready_for_query()?;
                        continue;
                    }

                    match Self::execute_sql(&sql, &catalog, &bpm, &lock_manager, &mut txn) {
                        Ok(ExecuteResult::Query(result)) => {
                            if !result.columns.is_empty() {
                                conn.send_row_description(&result.columns)?;
                                for row in &result.rows {
                                    conn.send_data_row(row)?;
                                }
                            }
                            conn.send_command_complete(&result.command_tag)?;
                        }
                        Ok(ExecuteResult::Begin) => {
                            conn.send_command_complete("BEGIN")?;
                        }
                        Ok(ExecuteResult::Commit) => {
                            conn.send_command_complete("COMMIT")?;
                        }
                        Ok(ExecuteResult::Rollback) => {
                            conn.send_command_complete("ROLLBACK")?;
                        }
                        Err(e) => {
                            println!("[Server] Error: {e}");
                            conn.send_error(&e.to_string())?;
                        }
                    }
                    conn.send_ready_for_query()?;
                }
                Some(FrontendMessage::Terminate) => {
                    println!(
                        "[Server] Client disconnected (thread: {:?})",
                        thread::current().id()
                    );
                    break;
                }
                Some(FrontendMessage::Unknown(t)) => {
                    println!("[Server] Unknown message type: {t}");
                }
                None => {
                    println!(
                        "[Server] Connection closed (thread: {:?})",
                        thread::current().id()
                    );
                    break;
                }
            }
        }

        // If transaction is still active, rollback on disconnect
        if txn.is_active() {
            let undo_log = txn.take_undo_log();
            let _ = ExecutionEngine::perform_rollback(&bpm, undo_log);
            // Release all locks
            let held_locks = txn.take_held_locks();
            lock_manager.unlock_all(txn.id, &held_locks);
        }

        // Flush data to disk before closing
        bpm.lock().unwrap().flush_all()?;
        Ok(())
    }

    fn execute_sql(
        sql: &str,
        catalog: &Catalog,
        bpm: &Arc<Mutex<BufferPoolManager>>,
        lock_manager: &Arc<LockManager>,
        txn: &mut Transaction,
    ) -> Result<ExecuteResult> {
        let sql = sql.trim();
        if sql.is_empty() {
            return Ok(ExecuteResult::Query(QueryResult {
                columns: vec![],
                rows: vec![],
                command_tag: String::new(),
            }));
        }

        let stmt = parse(sql)?;

        // Handle transaction control statements before analyze
        match &stmt {
            Statement::Begin => {
                if txn.is_active() {
                    anyhow::bail!("there is already a transaction in progress");
                }
                txn.begin();
                return Ok(ExecuteResult::Begin);
            }
            Statement::Commit => {
                if !txn.is_active() {
                    anyhow::bail!("there is no transaction in progress");
                }
                // Release all locks before commit
                let held_locks = txn.take_held_locks();
                lock_manager.unlock_all(txn.id, &held_locks);
                txn.commit();
                return Ok(ExecuteResult::Commit);
            }
            Statement::Rollback => {
                if !txn.is_active() {
                    anyhow::bail!("there is no transaction in progress");
                }
                let undo_log = txn.take_undo_log();
                ExecutionEngine::perform_rollback(bpm, undo_log)?;
                // Release all locks after rollback
                let held_locks = txn.take_held_locks();
                lock_manager.unlock_all(txn.id, &held_locks);
                return Ok(ExecuteResult::Rollback);
            }
            _ => {}
        }

        let analyzed = analyze(catalog, &stmt)?;

        // Pass transaction and lock_manager to engine
        let txn_opt = if txn.is_active() {
            Some(txn)
        } else {
            None
        };

        match &analyzed {
            AnalyzedStatement::Select(select_stmt) => {
                let columns: Vec<ColumnDesc> = select_stmt
                    .select_items
                    .iter()
                    .map(Self::get_column_desc)
                    .collect();

                let mut engine =
                    ExecutionEngine::new(Arc::clone(bpm), catalog, &analyzed, txn_opt, Some(lock_manager))?;
                let results = engine.execute()?;
                let rows: Vec<Vec<Option<String>>> =
                    results.iter().map(Self::tuple_to_row).collect();

                let row_count = rows.len();
                Ok(ExecuteResult::Query(QueryResult {
                    columns,
                    rows,
                    command_tag: format!("SELECT {row_count}"),
                }))
            }
            AnalyzedStatement::Insert(_) => {
                let mut engine =
                    ExecutionEngine::new(Arc::clone(bpm), catalog, &analyzed, txn_opt, Some(lock_manager))?;
                let results = engine.execute()?;
                let count = results.len();
                Ok(ExecuteResult::Query(QueryResult {
                    columns: vec![],
                    rows: vec![],
                    command_tag: format!("INSERT 0 {count}"),
                }))
            }
            AnalyzedStatement::CreateTable(_) => {
                anyhow::bail!("CREATE TABLE not implemented");
            }
            AnalyzedStatement::Delete(_) => {
                let mut engine =
                    ExecutionEngine::new(Arc::clone(bpm), catalog, &analyzed, txn_opt, Some(lock_manager))?;
                let results = engine.execute()?;
                let count = results.len() as i32;
                Ok(ExecuteResult::Query(QueryResult {
                    columns: vec![],
                    rows: vec![],
                    command_tag: format!("DELETE {count}"),
                }))
            }
            AnalyzedStatement::Update(_) => {
                let mut engine =
                    ExecutionEngine::new(Arc::clone(bpm), catalog, &analyzed, txn_opt, Some(lock_manager))?;
                let results = engine.execute()?;
                let count = results.len() as i32;
                Ok(ExecuteResult::Query(QueryResult {
                    columns: vec![],
                    rows: vec![],
                    command_tag: format!("UPDATE {count}"),
                }))
            }
        }
    }

    fn get_column_desc(item: &AnalyzedSelectItem) -> ColumnDesc {
        let data_type = item.expr.data_type();
        let name = item
            .alias
            .clone()
            .unwrap_or_else(|| Self::format_expr_name(&item.expr));

        match data_type {
            DataType::Int => ColumnDesc::new_int(&name),
            DataType::Varchar => ColumnDesc::new_varchar(&name),
            DataType::Bool => ColumnDesc::new_bool(&name),
        }
    }

    fn format_expr_name(expr: &AnalyzedExpr) -> String {
        match expr {
            AnalyzedExpr::ColumnRef(col_ref) => col_ref.column_name.clone(),
            AnalyzedExpr::Literal(lit) => format!("{:?}", lit.value),
            AnalyzedExpr::BinaryOp { .. } => "?column?".to_string(),
            AnalyzedExpr::UnaryOp { .. } => "?column?".to_string(),
        }
    }

    fn tuple_to_row(tuple: &Tuple) -> Vec<Option<String>> {
        tuple
            .values
            .iter()
            .map(|v| match v {
                Value::Int(n) => Some(n.to_string()),
                Value::Varchar(s) => Some(s.clone()),
                Value::Bool(b) => Some(if *b { "t" } else { "f" }.to_string()),
                Value::Null => None,
            })
            .collect()
    }
}
