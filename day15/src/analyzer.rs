use anyhow::{bail, Result};

use crate::ast::{
    Assignment, BinaryOperator, CreateTableStatement, DeleteStatement, Expr, InsertStatement,
    Literal, SelectColumn, SelectStatement, Statement, UnaryOperator, UpdateStatement,
};
use crate::catalog::Catalog;
use crate::tuple::DataType;

// Range Table Entry (RTE) - represents table-like objects in a query
// Base tables, subqueries, joins, etc. can all be represented as RTEs

#[derive(Debug, Clone)]
pub enum TableSource {
    BaseTable {
        table_id: usize,
        #[allow(dead_code)]
        table_name: String,
    },
}

// Output schema for an RTE
#[derive(Debug, Clone)]
pub struct OutputColumn {
    pub name: String,
    pub data_type: DataType,
    #[allow(dead_code)]
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct RangeTableEntry {
    #[allow(dead_code)]
    pub rte_index: usize,
    pub source: TableSource,
    pub output_columns: Vec<OutputColumn>,
}

impl RangeTableEntry {
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.output_columns.iter().position(|c| c.name == name)
    }
}

// Analyzed AST nodes with resolved type information

#[derive(Debug, Clone)]
pub enum AnalyzedStatement {
    Select(AnalyzedSelectStatement),
    Insert(AnalyzedInsertStatement),
    #[allow(dead_code)]
    CreateTable(AnalyzedCreateTableStatement),
    Delete(AnalyzedDeleteStatement),
    Update(AnalyzedUpdateStatement),
}

#[derive(Debug, Clone)]
pub struct AnalyzedSelectStatement {
    pub range_table: Vec<RangeTableEntry>,
    pub select_items: Vec<AnalyzedSelectItem>,
    pub from_rte_index: usize,
    pub where_clause: Option<AnalyzedExpr>,
}

#[derive(Debug, Clone)]
pub struct AnalyzedSelectItem {
    pub expr: AnalyzedExpr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AnalyzedColumnRef {
    #[allow(dead_code)]
    pub rte_index: usize,
    pub column_index: usize,
    pub column_name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone)]
pub struct AnalyzedInsertStatement {
    #[allow(dead_code)]
    pub table_id: usize,
    #[allow(dead_code)]
    pub table_name: String,
    pub values: Vec<AnalyzedExpr>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct AnalyzedCreateTableStatement {
    pub table_name: String,
    pub columns: Vec<AnalyzedColumnDef>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct AnalyzedColumnDef {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone)]
pub struct AnalyzedDeleteStatement {
    #[allow(dead_code)]
    pub table_id: usize,
    pub range_table: Vec<RangeTableEntry>,
    pub where_clause: Option<AnalyzedExpr>,
}

#[derive(Debug, Clone)]
pub struct AnalyzedUpdateStatement {
    #[allow(dead_code)]
    pub table_id: usize,
    pub range_table: Vec<RangeTableEntry>,
    pub assignments: Vec<AnalyzedAssignment>,
    pub where_clause: Option<AnalyzedExpr>,
}

#[derive(Debug, Clone)]
pub struct AnalyzedAssignment {
    pub column_index: usize,
    pub value: AnalyzedExpr,
}

#[derive(Debug, Clone)]
pub enum AnalyzedExpr {
    Literal(AnalyzedLiteral),
    ColumnRef(AnalyzedColumnRef),
    BinaryOp {
        left: Box<AnalyzedExpr>,
        op: BinaryOperator,
        right: Box<AnalyzedExpr>,
        result_type: DataType,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<AnalyzedExpr>,
        result_type: DataType,
    },
}

#[derive(Debug, Clone)]
pub struct AnalyzedLiteral {
    pub value: LiteralValue,
    pub data_type: DataType,
}

#[derive(Debug, Clone)]
pub enum LiteralValue {
    Integer(i64),
    String(String),
    Boolean(bool),
    Null,
}

impl AnalyzedExpr {
    pub fn data_type(&self) -> &DataType {
        match self {
            AnalyzedExpr::Literal(lit) => &lit.data_type,
            AnalyzedExpr::ColumnRef(col) => &col.data_type,
            AnalyzedExpr::BinaryOp { result_type, .. } => result_type,
            AnalyzedExpr::UnaryOp { result_type, .. } => result_type,
        }
    }
}

#[derive(Debug, Clone)]
struct ScopeEntry {
    #[allow(dead_code)]
    name: String,
    rte_index: usize,
}

#[derive(Debug, Clone)]
struct Scope {
    entries: Vec<ScopeEntry>,
}

impl Scope {
    fn new() -> Self {
        Scope {
            entries: Vec::new(),
        }
    }

    fn add_rte(&mut self, name: String, rte_index: usize) {
        self.entries.push(ScopeEntry { name, rte_index });
    }
}

pub struct Analyzer<'a> {
    catalog: &'a Catalog,
    range_table: Vec<RangeTableEntry>,
    scopes: Vec<Scope>,
}

impl<'a> Analyzer<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Analyzer {
            catalog,
            range_table: Vec::new(),
            scopes: Vec::new(),
        }
    }

    fn add_rte(&mut self, source: TableSource, output_columns: Vec<OutputColumn>) -> usize {
        let rte_index = self.range_table.len();
        self.range_table.push(RangeTableEntry {
            rte_index,
            source,
            output_columns,
        });
        rte_index
    }

    fn push_scope(&mut self) {
        self.scopes.push(Scope::new());
    }

    fn pop_scope(&mut self) {
        self.scopes.pop();
    }

    fn current_scope(&mut self) -> &mut Scope {
        self.scopes.last_mut().expect("no scope")
    }

    pub fn analyze(&mut self, stmt: &Statement) -> Result<AnalyzedStatement> {
        match stmt {
            Statement::Select(s) => self.analyze_select(s),
            Statement::Insert(s) => self.analyze_insert(s),
            Statement::CreateTable(s) => self.analyze_create_table(s),
            Statement::Delete(s) => self.analyze_delete(s),
            Statement::Update(s) => self.analyze_update(s),
            // Transaction control and utility statements are handled before analyze
            Statement::Begin | Statement::Commit | Statement::Rollback | Statement::Checkpoint => {
                anyhow::bail!("transaction control statements should be handled before analyze")
            }
        }
    }

    fn analyze_select(&mut self, stmt: &SelectStatement) -> Result<AnalyzedStatement> {
        self.range_table.clear();

        // Resolve table
        let table_ref = &stmt.from;
        let table = self
            .catalog
            .get_table(&table_ref.name)
            .ok_or_else(|| anyhow::anyhow!("table '{}' not found", table_ref.name))?;
        let table_id = self.catalog.get_table_id(&table_ref.name).unwrap();

        let output_columns: Vec<OutputColumn> = table
            .columns
            .iter()
            .map(|c| OutputColumn {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                nullable: c.nullable,
            })
            .collect();

        let rte_index = self.add_rte(
            TableSource::BaseTable {
                table_id,
                table_name: table_ref.name.clone(),
            },
            output_columns,
        );

        self.push_scope();
        let scope_name = table_ref.alias.clone().unwrap_or(table_ref.name.clone());
        self.current_scope().add_rte(scope_name, rte_index);

        // Resolve columns
        let mut select_items = Vec::new();
        for col in &stmt.columns {
            match col {
                SelectColumn::Asterisk => {
                    // Expand * to all columns
                    let rte = &self.range_table[rte_index];
                    for (col_idx, col_def) in rte.output_columns.iter().enumerate() {
                        select_items.push(AnalyzedSelectItem {
                            expr: AnalyzedExpr::ColumnRef(AnalyzedColumnRef {
                                rte_index,
                                column_index: col_idx,
                                column_name: col_def.name.clone(),
                                data_type: col_def.data_type.clone(),
                            }),
                            alias: None,
                        });
                    }
                }
                SelectColumn::Expr(expr) => {
                    let analyzed_expr = self.analyze_expr(expr)?;
                    select_items.push(AnalyzedSelectItem {
                        expr: analyzed_expr,
                        alias: None,
                    });
                }
            }
        }

        // Resolve WHERE clause
        let where_clause = if let Some(expr) = &stmt.where_clause {
            Some(self.analyze_expr(expr)?)
        } else {
            None
        };

        self.pop_scope();

        Ok(AnalyzedStatement::Select(AnalyzedSelectStatement {
            range_table: self.range_table.clone(),
            select_items,
            from_rte_index: rte_index,
            where_clause,
        }))
    }

    fn analyze_insert(&mut self, stmt: &InsertStatement) -> Result<AnalyzedStatement> {
        let table = self
            .catalog
            .get_table(&stmt.table)
            .ok_or_else(|| anyhow::anyhow!("table '{}' not found", stmt.table))?;
        let table_id = self.catalog.get_table_id(&stmt.table).unwrap();

        // Check value count matches column count
        if stmt.values.len() != table.columns.len() {
            bail!(
                "INSERT has {} values but table has {} columns",
                stmt.values.len(),
                table.columns.len()
            );
        }

        // Analyze and type-check values
        let mut analyzed_values = Vec::new();
        for (i, value) in stmt.values.iter().enumerate() {
            let analyzed_expr = self.analyze_expr(value)?;
            let expected_type = &table.columns[i].data_type;
            let actual_type = analyzed_expr.data_type();

            // Type check (allow NULL for nullable columns)
            if let AnalyzedExpr::Literal(AnalyzedLiteral {
                value: LiteralValue::Null,
                ..
            }) = &analyzed_expr
            {
                if !table.columns[i].nullable {
                    bail!("column '{}' is not nullable", table.columns[i].name);
                }
            } else if actual_type != expected_type {
                bail!(
                    "type mismatch for column '{}': expected {:?}, got {:?}",
                    table.columns[i].name,
                    expected_type,
                    actual_type
                );
            }

            analyzed_values.push(analyzed_expr);
        }

        Ok(AnalyzedStatement::Insert(AnalyzedInsertStatement {
            table_id,
            table_name: stmt.table.clone(),
            values: analyzed_values,
        }))
    }

    fn analyze_create_table(&mut self, stmt: &CreateTableStatement) -> Result<AnalyzedStatement> {
        // Check table doesn't exist
        if self.catalog.get_table(&stmt.table).is_some() {
            bail!("table '{}' already exists", stmt.table);
        }

        let analyzed_columns: Vec<AnalyzedColumnDef> = stmt
            .columns
            .iter()
            .map(|c| AnalyzedColumnDef {
                name: c.name.clone(),
                data_type: convert_ast_data_type(&c.data_type),
            })
            .collect();

        Ok(AnalyzedStatement::CreateTable(AnalyzedCreateTableStatement {
            table_name: stmt.table.clone(),
            columns: analyzed_columns,
        }))
    }

    fn analyze_delete(&mut self, stmt: &DeleteStatement) -> Result<AnalyzedStatement> {
        self.range_table.clear();

        let table = self
            .catalog
            .get_table(&stmt.table)
            .ok_or_else(|| anyhow::anyhow!("table '{}' not found", stmt.table))?;
        let table_id = self.catalog.get_table_id(&stmt.table).unwrap();

        let output_columns: Vec<OutputColumn> = table
            .columns
            .iter()
            .map(|c| OutputColumn {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                nullable: c.nullable,
            })
            .collect();

        let rte_index = self.add_rte(
            TableSource::BaseTable {
                table_id,
                table_name: stmt.table.clone(),
            },
            output_columns,
        );

        self.push_scope();
        self.current_scope().add_rte(stmt.table.clone(), rte_index);

        let where_clause = if let Some(expr) = &stmt.where_clause {
            Some(self.analyze_expr(expr)?)
        } else {
            None
        };

        self.pop_scope();

        Ok(AnalyzedStatement::Delete(AnalyzedDeleteStatement {
            table_id,
            range_table: self.range_table.clone(),
            where_clause,
        }))
    }

    fn analyze_update(&mut self, stmt: &UpdateStatement) -> Result<AnalyzedStatement> {
        self.range_table.clear();

        let table = self
            .catalog
            .get_table(&stmt.table)
            .ok_or_else(|| anyhow::anyhow!("table '{}' not found", stmt.table))?;
        let table_id = self.catalog.get_table_id(&stmt.table).unwrap();

        let output_columns: Vec<OutputColumn> = table
            .columns
            .iter()
            .map(|c| OutputColumn {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                nullable: c.nullable,
            })
            .collect();

        let rte_index = self.add_rte(
            TableSource::BaseTable {
                table_id,
                table_name: stmt.table.clone(),
            },
            output_columns,
        );

        self.push_scope();
        self.current_scope().add_rte(stmt.table.clone(), rte_index);

        // Analyze assignments
        let mut analyzed_assignments = Vec::new();
        for Assignment { column, value } in &stmt.assignments {
            let column_index = table
                .columns
                .iter()
                .position(|c| c.name == *column)
                .ok_or_else(|| anyhow::anyhow!("column '{column}' not found"))?;

            let analyzed_value = self.analyze_expr(value)?;

            // Type check
            let expected_type = &table.columns[column_index].data_type;
            let actual_type = analyzed_value.data_type();
            if actual_type != expected_type {
                bail!(
                    "type mismatch for column '{column}': expected {expected_type:?}, got {actual_type:?}"
                );
            }

            analyzed_assignments.push(AnalyzedAssignment {
                column_index,
                value: analyzed_value,
            });
        }

        let where_clause = if let Some(expr) = &stmt.where_clause {
            Some(self.analyze_expr(expr)?)
        } else {
            None
        };

        self.pop_scope();

        Ok(AnalyzedStatement::Update(AnalyzedUpdateStatement {
            table_id,
            range_table: self.range_table.clone(),
            assignments: analyzed_assignments,
            where_clause,
        }))
    }

    fn analyze_expr(&mut self, expr: &Expr) -> Result<AnalyzedExpr> {
        match expr {
            Expr::Literal(lit) => self.analyze_literal(lit),
            Expr::Column(name) => self.analyze_column(name),
            Expr::BinaryOp { left, op, right } => {
                let analyzed_left = self.analyze_expr(left)?;
                let analyzed_right = self.analyze_expr(right)?;
                let result_type = self.infer_binary_op_type(op, &analyzed_left, &analyzed_right)?;

                Ok(AnalyzedExpr::BinaryOp {
                    left: Box::new(analyzed_left),
                    op: op.clone(),
                    right: Box::new(analyzed_right),
                    result_type,
                })
            }
            Expr::UnaryOp { op, expr } => {
                let analyzed_expr = self.analyze_expr(expr)?;
                let result_type = self.infer_unary_op_type(op, &analyzed_expr)?;

                Ok(AnalyzedExpr::UnaryOp {
                    op: op.clone(),
                    expr: Box::new(analyzed_expr),
                    result_type,
                })
            }
        }
    }

    fn analyze_literal(&self, lit: &Literal) -> Result<AnalyzedExpr> {
        let (value, data_type) = match lit {
            Literal::Integer(n) => (LiteralValue::Integer(*n), DataType::Int),
            Literal::String(s) => (LiteralValue::String(s.clone()), DataType::Varchar),
            Literal::Boolean(b) => (LiteralValue::Boolean(*b), DataType::Bool),
            Literal::Null => (LiteralValue::Null, DataType::Int), // NULL type is context-dependent
        };
        Ok(AnalyzedExpr::Literal(AnalyzedLiteral { value, data_type }))
    }

    fn analyze_column(&self, name: &str) -> Result<AnalyzedExpr> {
        // Search through scopes from innermost to outermost
        for scope in self.scopes.iter().rev() {
            for entry in &scope.entries {
                let rte = &self.range_table[entry.rte_index];
                if let Some(col_idx) = rte.get_column_index(name) {
                    let col = &rte.output_columns[col_idx];
                    return Ok(AnalyzedExpr::ColumnRef(AnalyzedColumnRef {
                        rte_index: entry.rte_index,
                        column_index: col_idx,
                        column_name: name.to_string(),
                        data_type: col.data_type.clone(),
                    }));
                }
            }
        }
        bail!("column '{name}' not found")
    }

    fn infer_binary_op_type(
        &self,
        op: &BinaryOperator,
        _left: &AnalyzedExpr,
        _right: &AnalyzedExpr,
    ) -> Result<DataType> {
        match op {
            BinaryOperator::Eq
            | BinaryOperator::Ne
            | BinaryOperator::Lt
            | BinaryOperator::Le
            | BinaryOperator::Gt
            | BinaryOperator::Ge
            | BinaryOperator::And
            | BinaryOperator::Or => Ok(DataType::Bool),
            BinaryOperator::Add
            | BinaryOperator::Sub
            | BinaryOperator::Mul
            | BinaryOperator::Div => Ok(DataType::Int),
        }
    }

    fn infer_unary_op_type(&self, op: &UnaryOperator, _expr: &AnalyzedExpr) -> Result<DataType> {
        match op {
            UnaryOperator::Not => Ok(DataType::Bool),
            UnaryOperator::Neg => Ok(DataType::Int),
        }
    }
}

fn convert_ast_data_type(dt: &crate::ast::DataType) -> DataType {
    match dt {
        crate::ast::DataType::Int => DataType::Int,
        crate::ast::DataType::Varchar => DataType::Varchar,
    }
}

pub fn analyze(catalog: &Catalog, stmt: &Statement) -> Result<AnalyzedStatement> {
    let mut analyzer = Analyzer::new(catalog);
    analyzer.analyze(stmt)
}
