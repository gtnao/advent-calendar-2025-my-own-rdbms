use std::net::TcpListener;

use anyhow::Result;

use crate::analyzer::{analyze, AnalyzedExpr, AnalyzedSelectItem, AnalyzedStatement};
use crate::buffer_pool::BufferPoolManager;
use crate::catalog::Catalog;
use crate::disk::DiskManager;
use crate::executor::{ExecutionEngine, Tuple};
use crate::parser::parse;
use crate::protocol::{ColumnDesc, Connection, FrontendMessage};
use crate::tuple::{DataType, Value};

const DATA_FILE: &str = "table.db";
const PORT: u16 = 5433;

pub struct Instance {
    catalog: Catalog,
    bpm: BufferPoolManager,
}

struct QueryResult {
    columns: Vec<ColumnDesc>,
    rows: Vec<Vec<Option<String>>>,
    command_tag: String,
}

impl Instance {
    pub fn new() -> Result<Self> {
        let catalog = Catalog::new();

        // Create fresh database file
        let _ = std::fs::remove_file(DATA_FILE);
        let disk_manager = DiskManager::open(DATA_FILE)?;
        let bpm = BufferPoolManager::new(disk_manager);

        Ok(Instance { catalog, bpm })
    }

    pub fn start(&mut self) -> Result<()> {
        let listener = TcpListener::bind(format!("127.0.0.1:{PORT}"))?;
        println!("=== PostgreSQL Wire Protocol Server ===");
        println!("Listening on port {PORT}");
        println!("Connect with: psql -h localhost -p {PORT}");
        println!();

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    println!("[Server] New connection from {:?}", stream.peer_addr());
                    let conn = Connection::new(stream);
                    if let Err(e) = self.handle_client(conn) {
                        println!("[Server] Error handling client: {e}");
                    }
                }
                Err(e) => {
                    println!("[Server] Accept error: {e}");
                }
            }
        }

        Ok(())
    }

    fn handle_client(&mut self, mut conn: Connection) -> Result<()> {
        // Read startup message
        let startup = conn.read_startup()?;
        println!("[Server] Startup: {:?}", startup.params);

        // Send authentication OK
        conn.send_auth_ok()?;

        // Send some parameter status messages
        conn.send_parameter_status("server_version", "0.0.1")?;
        conn.send_parameter_status("client_encoding", "UTF8")?;

        // Send backend key data
        conn.send_backend_key_data(1, 12345)?;

        // Ready for query
        conn.send_ready_for_query()?;

        // Main query loop
        loop {
            match conn.read_message()? {
                Some(FrontendMessage::Query(sql)) => {
                    println!("[Server] Query: {sql}");

                    if sql.trim().is_empty() {
                        conn.send_empty_query()?;
                        conn.send_ready_for_query()?;
                        continue;
                    }

                    match self.execute_sql(&sql) {
                        Ok(result) => {
                            if !result.columns.is_empty() {
                                conn.send_row_description(&result.columns)?;
                                for row in &result.rows {
                                    conn.send_data_row(row)?;
                                }
                            }
                            conn.send_command_complete(&result.command_tag)?;
                        }
                        Err(e) => {
                            println!("[Server] Error: {e}");
                            conn.send_error(&e.to_string())?;
                        }
                    }
                    conn.send_ready_for_query()?;
                }
                Some(FrontendMessage::Terminate) => {
                    println!("[Server] Client disconnected");
                    break;
                }
                Some(FrontendMessage::Unknown(t)) => {
                    println!("[Server] Unknown message type: {t}");
                }
                None => {
                    println!("[Server] Connection closed");
                    break;
                }
            }
        }

        // Flush data to disk before closing
        self.bpm.flush_all()?;
        Ok(())
    }

    fn execute_sql(&mut self, sql: &str) -> Result<QueryResult> {
        let sql = sql.trim();
        if sql.is_empty() {
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                command_tag: String::new(),
            });
        }

        let stmt = parse(sql)?;
        let analyzed = analyze(&self.catalog, &stmt)?;

        match &analyzed {
            AnalyzedStatement::Select(select_stmt) => {
                let columns: Vec<ColumnDesc> = select_stmt
                    .select_items
                    .iter()
                    .map(Self::get_column_desc)
                    .collect();

                let mut engine = ExecutionEngine::new(&mut self.bpm, &self.catalog, &analyzed)?;
                let results = engine.execute()?;
                let rows: Vec<Vec<Option<String>>> =
                    results.iter().map(Self::tuple_to_row).collect();

                let row_count = rows.len();
                Ok(QueryResult {
                    columns,
                    rows,
                    command_tag: format!("SELECT {row_count}"),
                })
            }
            AnalyzedStatement::Insert(_) => {
                let mut engine = ExecutionEngine::new(&mut self.bpm, &self.catalog, &analyzed)?;
                let results = engine.execute()?;
                let count = results.len();
                Ok(QueryResult {
                    columns: vec![],
                    rows: vec![],
                    command_tag: format!("INSERT 0 {count}"),
                })
            }
            AnalyzedStatement::CreateTable(_) => {
                anyhow::bail!("CREATE TABLE not implemented");
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
