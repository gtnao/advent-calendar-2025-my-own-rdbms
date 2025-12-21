mod aggregate;
mod create_table;
mod delete;
mod filter;
mod insert;
mod nested_loop_join;
mod projection;
mod seq_scan;
mod update;

use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::analyzer::{
    AnalyzedColumnRef, AnalyzedExpr, AnalyzedLiteral, AnalyzedSelectItem, AnalyzedSelectStatement,
    AnalyzedStatement, LiteralValue, RangeTableEntry, TableSource,
};
use crate::ast::{BinaryOperator, UnaryOperator};
use crate::buffer_pool::BufferPoolManager;
use crate::catalog::Catalog;
use crate::lock_manager::LockManager;
use crate::transaction::{Transaction, UndoLogEntry};
use crate::transaction_manager::TransactionManager;
use crate::tuple::Value;
use crate::wal::{CLRRedo, Lsn, WalManager, WalRecordType};

pub use aggregate::AggregateExecutor;
pub use create_table::CreateTableExecutor;
pub use delete::DeleteExecutor;
pub use filter::{FilterExecutor, FilterWithOffsetsExecutor};
pub use insert::InsertExecutor;
pub use nested_loop_join::NestedLoopJoinExecutor;
pub use projection::{ProjectionExecutor, ProjectionWithOffsetsExecutor};
pub use seq_scan::SeqScanExecutor;
pub use update::UpdateExecutor;

// Row ID: page_id + slot_id
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rid {
    pub page_id: u32,
    pub slot_id: u16,
}

impl std::hash::Hash for Rid {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.page_id.hash(state);
        self.slot_id.hash(state);
    }
}

// Runtime representation of a row during query execution.
#[derive(Debug, Clone)]
pub struct Tuple {
    pub values: Vec<Value>,
    pub rid: Option<Rid>,
}

impl Tuple {
    pub fn new(values: Vec<Value>) -> Self {
        Tuple { values, rid: None }
    }

    pub fn with_rid(values: Vec<Value>, rid: Rid) -> Self {
        Tuple {
            values,
            rid: Some(rid),
        }
    }
}

// Volcano model: open/next interface
pub trait Executor {
    fn open(&mut self) -> Result<()>;
    fn next(&mut self) -> Result<Option<Tuple>>;
}

// Expression evaluation for single-table queries
pub fn evaluate_expr(expr: &AnalyzedExpr, tuple: &Tuple) -> Result<Value> {
    match expr {
        AnalyzedExpr::Literal(lit) => Ok(literal_to_value(lit)),
        AnalyzedExpr::ColumnRef(col_ref) => Ok(tuple.values[col_ref.column_index].clone()),
        AnalyzedExpr::BinaryOp {
            left, op, right, ..
        } => {
            let left_val = evaluate_expr(left, tuple)?;
            let right_val = evaluate_expr(right, tuple)?;
            evaluate_binary_op(op, &left_val, &right_val)
        }
        AnalyzedExpr::UnaryOp { op, expr, .. } => {
            let val = evaluate_expr(expr, tuple)?;
            evaluate_unary_op(op, &val)
        }
        AnalyzedExpr::Aggregate(_) => {
            anyhow::bail!("aggregate functions should be evaluated by AggregateExecutor")
        }
    }
}

// Expression evaluation with RTE offsets (for JOIN queries)
pub fn evaluate_expr_with_offsets(
    expr: &AnalyzedExpr,
    tuple: &Tuple,
    rte_offsets: &[usize],
) -> Result<Value> {
    match expr {
        AnalyzedExpr::Literal(lit) => Ok(literal_to_value(lit)),
        AnalyzedExpr::ColumnRef(col_ref) => {
            let offset = rte_offsets.get(col_ref.rte_index).copied().unwrap_or(0);
            let actual_index = offset + col_ref.column_index;
            Ok(tuple.values[actual_index].clone())
        }
        AnalyzedExpr::BinaryOp {
            left, op, right, ..
        } => {
            let left_val = evaluate_expr_with_offsets(left, tuple, rte_offsets)?;
            let right_val = evaluate_expr_with_offsets(right, tuple, rte_offsets)?;
            evaluate_binary_op(op, &left_val, &right_val)
        }
        AnalyzedExpr::UnaryOp { op, expr, .. } => {
            let val = evaluate_expr_with_offsets(expr, tuple, rte_offsets)?;
            evaluate_unary_op(op, &val)
        }
        AnalyzedExpr::Aggregate(_) => {
            anyhow::bail!("aggregate functions should be evaluated by AggregateExecutor")
        }
    }
}

pub fn evaluate_predicate_with_offsets(
    expr: &AnalyzedExpr,
    tuple: &Tuple,
    rte_offsets: &[usize],
) -> Result<bool> {
    let result = evaluate_expr_with_offsets(expr, tuple, rte_offsets)?;
    match result {
        Value::Bool(b) => Ok(b),
        Value::Null => Ok(false),
        _ => anyhow::bail!("predicate must evaluate to boolean"),
    }
}

pub fn literal_to_value(lit: &AnalyzedLiteral) -> Value {
    match &lit.value {
        LiteralValue::Integer(n) => Value::Int(*n as i32),
        LiteralValue::String(s) => Value::Varchar(s.clone()),
        LiteralValue::Boolean(b) => Value::Bool(*b),
        LiteralValue::Null => Value::Null,
    }
}

pub fn evaluate_binary_op(op: &BinaryOperator, left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Int(l), Value::Int(r)) => {
            let result = match op {
                BinaryOperator::Eq => Value::Bool(*l == *r),
                BinaryOperator::Ne => Value::Bool(*l != *r),
                BinaryOperator::Lt => Value::Bool(*l < *r),
                BinaryOperator::Le => Value::Bool(*l <= *r),
                BinaryOperator::Gt => Value::Bool(*l > *r),
                BinaryOperator::Ge => Value::Bool(*l >= *r),
                BinaryOperator::Add => Value::Int(*l + *r),
                BinaryOperator::Sub => Value::Int(*l - *r),
                BinaryOperator::Mul => Value::Int(*l * *r),
                BinaryOperator::Div => {
                    if *r == 0 {
                        Value::Null
                    } else {
                        Value::Int(*l / *r)
                    }
                }
                BinaryOperator::And | BinaryOperator::Or => {
                    anyhow::bail!("AND/OR not supported for integers")
                }
            };
            Ok(result)
        }
        (Value::Varchar(l), Value::Varchar(r)) => {
            let result = match op {
                BinaryOperator::Eq => Value::Bool(l == r),
                BinaryOperator::Ne => Value::Bool(l != r),
                BinaryOperator::Lt => Value::Bool(l < r),
                BinaryOperator::Le => Value::Bool(l <= r),
                BinaryOperator::Gt => Value::Bool(l > r),
                BinaryOperator::Ge => Value::Bool(l >= r),
                _ => anyhow::bail!("unsupported binary op for varchar"),
            };
            Ok(result)
        }
        (Value::Bool(l), Value::Bool(r)) => {
            let result = match op {
                BinaryOperator::Eq => Value::Bool(*l == *r),
                BinaryOperator::Ne => Value::Bool(*l != *r),
                BinaryOperator::And => Value::Bool(*l && *r),
                BinaryOperator::Or => Value::Bool(*l || *r),
                _ => anyhow::bail!("unsupported binary op for bool"),
            };
            Ok(result)
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => anyhow::bail!("type mismatch in binary operation"),
    }
}

pub fn evaluate_unary_op(op: &UnaryOperator, val: &Value) -> Result<Value> {
    match (op, val) {
        (UnaryOperator::Not, Value::Bool(b)) => Ok(Value::Bool(!*b)),
        (UnaryOperator::Neg, Value::Int(n)) => Ok(Value::Int(-*n)),
        (_, Value::Null) => Ok(Value::Null),
        _ => anyhow::bail!("type mismatch in unary operation"),
    }
}

fn evaluate_predicate(expr: &AnalyzedExpr, tuple: &Tuple) -> Result<bool> {
    let result = evaluate_expr(expr, tuple)?;
    match result {
        Value::Bool(b) => Ok(b),
        Value::Null => Ok(false),
        _ => anyhow::bail!("predicate must evaluate to boolean"),
    }
}

// Execution Engine - orchestrates query execution
pub struct ExecutionEngine<'a> {
    executor: Box<dyn Executor + 'a>,
}

impl<'a> ExecutionEngine<'a> {
    pub fn new(
        bpm: Arc<Mutex<BufferPoolManager>>,
        catalog: &'a Catalog,
        stmt: &AnalyzedStatement,
        txn: Option<&'a mut Transaction>,
        lock_manager: Option<&'a LockManager>,
        wal_manager: Option<Arc<WalManager>>,
        txn_manager: Option<&'a TransactionManager>,
    ) -> Result<Self> {
        let executor =
            Self::build_executor(bpm, catalog, stmt, txn, lock_manager, wal_manager, txn_manager)?;
        Ok(ExecutionEngine { executor })
    }

    pub fn execute(&mut self) -> Result<Vec<Tuple>> {
        self.executor.open()?;
        let mut results = Vec::new();
        while let Some(tuple) = self.executor.next()? {
            results.push(tuple);
        }
        Ok(results)
    }

    pub fn perform_rollback(
        bpm: &Arc<Mutex<BufferPoolManager>>,
        wal_manager: &Arc<WalManager>,
        txn_id: u64,
        undo_log: Vec<UndoLogEntry>,
        mut last_lsn: Lsn,
    ) -> Result<Lsn> {
        for entry in undo_log.into_iter().rev() {
            match entry {
                UndoLogEntry::Insert { prev_lsn, rid, .. } => {
                    let page_arc = bpm.lock().unwrap().fetch_page_mut(rid.page_id)?;
                    let mut page_guard = page_arc.write().unwrap();
                    page_guard.delete(rid.slot_id)?;

                    let clr_lsn = wal_manager.append(
                        txn_id,
                        last_lsn,
                        WalRecordType::CLR {
                            undo_next_lsn: prev_lsn,
                            redo: CLRRedo::UndoInsert { rid },
                        },
                    );
                    page_guard.page_lsn = clr_lsn;
                    last_lsn = clr_lsn;

                    drop(page_guard);
                    bpm.lock().unwrap().unpin_page(rid.page_id, true)?;
                }
                UndoLogEntry::Delete {
                    prev_lsn,
                    rid,
                    old_xmax,
                    ..
                } => {
                    let page_arc = bpm.lock().unwrap().fetch_page_mut(rid.page_id)?;
                    let mut page_guard = page_arc.write().unwrap();
                    page_guard.set_tuple_xmax(rid.slot_id, old_xmax)?;

                    let clr_lsn = wal_manager.append(
                        txn_id,
                        last_lsn,
                        WalRecordType::CLR {
                            undo_next_lsn: prev_lsn,
                            redo: CLRRedo::UndoDelete { rid, old_xmax },
                        },
                    );
                    page_guard.page_lsn = clr_lsn;
                    last_lsn = clr_lsn;

                    drop(page_guard);
                    bpm.lock().unwrap().unpin_page(rid.page_id, true)?;
                }
            }
        }
        Ok(last_lsn)
    }

    fn build_executor(
        bpm: Arc<Mutex<BufferPoolManager>>,
        catalog: &'a Catalog,
        stmt: &AnalyzedStatement,
        txn: Option<&'a mut Transaction>,
        lock_manager: Option<&'a LockManager>,
        wal_manager: Option<Arc<WalManager>>,
        txn_manager: Option<&'a TransactionManager>,
    ) -> Result<Box<dyn Executor + 'a>> {
        match stmt {
            AnalyzedStatement::Select(s) => Ok(Self::build_select_executor(
                bpm,
                catalog,
                s,
                txn,
                lock_manager,
                txn_manager,
            )),
            AnalyzedStatement::Insert(s) => Ok(Box::new(InsertExecutor::new(
                bpm,
                catalog,
                s,
                txn,
                lock_manager,
                wal_manager,
            )?)),
            AnalyzedStatement::CreateTable(s) => Ok(Box::new(CreateTableExecutor::new(
                bpm,
                catalog,
                s,
                txn,
                lock_manager,
                wal_manager,
            ))),
            AnalyzedStatement::Delete(s) => Ok(Self::build_delete_executor(
                bpm,
                catalog,
                s,
                txn,
                lock_manager,
                wal_manager,
                txn_manager,
            )),
            AnalyzedStatement::Update(s) => Ok(Self::build_update_executor(
                bpm,
                catalog,
                s,
                txn,
                lock_manager,
                wal_manager,
                txn_manager,
            )),
        }
    }

    fn build_select_executor(
        bpm: Arc<Mutex<BufferPoolManager>>,
        catalog: &'a Catalog,
        stmt: &AnalyzedSelectStatement,
        txn: Option<&'a mut Transaction>,
        lock_manager: Option<&'a LockManager>,
        txn_manager: Option<&'a TransactionManager>,
    ) -> Box<dyn Executor + 'a> {
        let snapshot = match &txn {
            Some(t) if t.snapshot.is_some() => t.snapshot.clone(),
            _ => txn_manager.map(|tm| {
                let txn_id = txn.as_ref().map(|t| t.id).unwrap_or(0);
                tm.get_snapshot(txn_id)
            }),
        };

        let mut rte_offsets = Vec::new();
        let mut offset = 0;
        for rte in &stmt.range_table {
            rte_offsets.push(offset);
            offset += rte.output_columns.len();
        }

        let scan: Box<dyn Executor> = if stmt.join_conditions.is_empty() {
            let rte = &stmt.range_table[stmt.from_rte_index];
            let table_id = match &rte.source {
                TableSource::BaseTable { table_id, .. } => *table_id,
            };

            Box::new(SeqScanExecutor::new(
                bpm,
                catalog,
                table_id,
                txn,
                lock_manager,
                snapshot,
                txn_manager,
            ))
        } else {
            let first_rte = &stmt.range_table[0];
            let first_table_id = match &first_rte.source {
                TableSource::BaseTable { table_id, .. } => *table_id,
            };

            let mut current: Box<dyn Executor> = Box::new(SeqScanExecutor::new(
                Arc::clone(&bpm),
                catalog,
                first_table_id,
                None,
                None,
                snapshot.clone(),
                txn_manager,
            ));

            for join_cond in &stmt.join_conditions {
                let right_rte = &stmt.range_table[join_cond.right_rte_index];
                let right_table_id = match &right_rte.source {
                    TableSource::BaseTable { table_id, .. } => *table_id,
                };
                let right_col_count = right_rte.output_columns.len();

                let right_scan: Box<dyn Executor> = Box::new(SeqScanExecutor::new(
                    Arc::clone(&bpm),
                    catalog,
                    right_table_id,
                    None,
                    None,
                    snapshot.clone(),
                    txn_manager,
                ));

                current = Box::new(NestedLoopJoinExecutor::new(
                    current,
                    right_scan,
                    join_cond.condition.clone(),
                    join_cond.join_type.clone(),
                    rte_offsets.clone(),
                    right_col_count,
                ));
            }

            current
        };

        let filtered: Box<dyn Executor> = if let Some(pred) = &stmt.where_clause {
            if stmt.join_conditions.is_empty() {
                Box::new(FilterExecutor::new(scan, pred.clone()))
            } else {
                Box::new(FilterWithOffsetsExecutor::new(
                    scan,
                    pred.clone(),
                    rte_offsets.clone(),
                ))
            }
        } else {
            scan
        };

        // Check if we need aggregation (GROUP BY or aggregate functions)
        let needs_aggregate = !stmt.group_by.is_empty()
            || stmt
                .select_items
                .iter()
                .any(|item| Self::contains_aggregate(&item.expr))
            || stmt
                .having
                .as_ref()
                .map(|h| Self::contains_aggregate(h))
                .unwrap_or(false);

        if needs_aggregate {
            let agg_exec: Box<dyn Executor + 'a> = Box::new(AggregateExecutor::new(
                filtered,
                stmt.group_by.clone(),
                stmt.select_items.clone(),
            ));
            // Apply HAVING as a FilterExecutor on top of AggregateExecutor
            if let Some(having) = &stmt.having {
                // Transform HAVING expression to reference output columns
                let transformed_having =
                    Self::transform_having_expr(having, &stmt.select_items, &stmt.group_by);
                Box::new(FilterExecutor::new(agg_exec, transformed_having))
            } else {
                agg_exec
            }
        } else {
            let exprs: Vec<AnalyzedExpr> = stmt
                .select_items
                .iter()
                .map(|item| item.expr.clone())
                .collect();

            if stmt.join_conditions.is_empty() {
                Box::new(ProjectionExecutor::new(filtered, exprs))
            } else {
                Box::new(ProjectionWithOffsetsExecutor::new(
                    filtered,
                    exprs,
                    rte_offsets,
                ))
            }
        }
    }

    fn contains_aggregate(expr: &AnalyzedExpr) -> bool {
        match expr {
            AnalyzedExpr::Aggregate(_) => true,
            AnalyzedExpr::BinaryOp { left, right, .. } => {
                Self::contains_aggregate(left) || Self::contains_aggregate(right)
            }
            AnalyzedExpr::UnaryOp { expr, .. } => Self::contains_aggregate(expr),
            AnalyzedExpr::Literal(_) | AnalyzedExpr::ColumnRef(_) => false,
        }
    }

    /// Transform HAVING expression to reference output columns from AggregateExecutor.
    /// Aggregates and GROUP BY columns are replaced with ColumnRefs to output indices.
    fn transform_having_expr(
        expr: &AnalyzedExpr,
        select_items: &[AnalyzedSelectItem],
        group_by: &[AnalyzedExpr],
    ) -> AnalyzedExpr {
        match expr {
            AnalyzedExpr::Aggregate(agg) => {
                // Find matching aggregate in select_items
                for (i, item) in select_items.iter().enumerate() {
                    if Self::exprs_match(&item.expr, expr) {
                        return AnalyzedExpr::ColumnRef(AnalyzedColumnRef {
                            rte_index: 0,
                            column_index: i,
                            column_name: format!("agg_{}", i),
                            data_type: agg.result_type.clone(),
                        });
                    }
                }
                // If not found in select_items, keep as-is (will fail at runtime)
                expr.clone()
            }
            AnalyzedExpr::ColumnRef(col_ref) => {
                // Find matching GROUP BY column in output
                for (i, item) in select_items.iter().enumerate() {
                    if let AnalyzedExpr::ColumnRef(item_col) = &item.expr {
                        if item_col.column_index == col_ref.column_index {
                            return AnalyzedExpr::ColumnRef(AnalyzedColumnRef {
                                rte_index: 0,
                                column_index: i,
                                column_name: col_ref.column_name.clone(),
                                data_type: col_ref.data_type.clone(),
                            });
                        }
                    }
                }
                expr.clone()
            }
            AnalyzedExpr::BinaryOp {
                left,
                op,
                right,
                result_type,
            } => AnalyzedExpr::BinaryOp {
                left: Box::new(Self::transform_having_expr(left, select_items, group_by)),
                op: op.clone(),
                right: Box::new(Self::transform_having_expr(right, select_items, group_by)),
                result_type: result_type.clone(),
            },
            AnalyzedExpr::UnaryOp {
                op,
                expr: inner,
                result_type,
            } => AnalyzedExpr::UnaryOp {
                op: op.clone(),
                expr: Box::new(Self::transform_having_expr(inner, select_items, group_by)),
                result_type: result_type.clone(),
            },
            AnalyzedExpr::Literal(_) => expr.clone(),
        }
    }

    /// Check if two expressions match (for finding aggregates in select_items)
    fn exprs_match(a: &AnalyzedExpr, b: &AnalyzedExpr) -> bool {
        match (a, b) {
            (AnalyzedExpr::Aggregate(agg1), AnalyzedExpr::Aggregate(agg2)) => {
                if agg1.func != agg2.func {
                    return false;
                }
                match (&agg1.arg, &agg2.arg) {
                    (
                        crate::analyzer::AnalyzedAggregateArg::Asterisk,
                        crate::analyzer::AnalyzedAggregateArg::Asterisk,
                    ) => true,
                    (
                        crate::analyzer::AnalyzedAggregateArg::Expr(e1),
                        crate::analyzer::AnalyzedAggregateArg::Expr(e2),
                    ) => Self::exprs_match(e1, e2),
                    _ => false,
                }
            }
            (AnalyzedExpr::ColumnRef(c1), AnalyzedExpr::ColumnRef(c2)) => {
                c1.column_index == c2.column_index
            }
            (AnalyzedExpr::Literal(l1), AnalyzedExpr::Literal(l2)) => {
                literal_to_value(l1) == literal_to_value(l2)
            }
            _ => false,
        }
    }

    fn build_delete_executor(
        bpm: Arc<Mutex<BufferPoolManager>>,
        catalog: &'a Catalog,
        stmt: &crate::analyzer::AnalyzedDeleteStatement,
        txn: Option<&'a mut Transaction>,
        lock_manager: Option<&'a LockManager>,
        wal_manager: Option<Arc<WalManager>>,
        txn_manager: Option<&'a TransactionManager>,
    ) -> Box<dyn Executor + 'a> {
        let table_id = Self::get_table_id_from_range_table(&stmt.range_table);

        let snapshot = match &txn {
            Some(t) if t.snapshot.is_some() => t.snapshot.clone(),
            _ => txn_manager.map(|tm| {
                let txn_id = txn.as_ref().map(|t| t.id).unwrap_or(0);
                tm.get_snapshot(txn_id)
            }),
        };

        let scan: Box<dyn Executor> = Box::new(SeqScanExecutor::new(
            Arc::clone(&bpm),
            catalog,
            table_id,
            None,
            None,
            snapshot,
            txn_manager,
        ));

        let filtered: Box<dyn Executor> = if let Some(pred) = &stmt.where_clause {
            Box::new(FilterExecutor::new(scan, pred.clone()))
        } else {
            scan
        };

        Box::new(DeleteExecutor::new(
            bpm,
            filtered,
            txn,
            lock_manager,
            wal_manager,
        ))
    }

    fn build_update_executor(
        bpm: Arc<Mutex<BufferPoolManager>>,
        catalog: &'a Catalog,
        stmt: &crate::analyzer::AnalyzedUpdateStatement,
        txn: Option<&'a mut Transaction>,
        lock_manager: Option<&'a LockManager>,
        wal_manager: Option<Arc<WalManager>>,
        txn_manager: Option<&'a TransactionManager>,
    ) -> Box<dyn Executor + 'a> {
        let table_id = Self::get_table_id_from_range_table(&stmt.range_table);

        let snapshot = match &txn {
            Some(t) if t.snapshot.is_some() => t.snapshot.clone(),
            _ => txn_manager.map(|tm| {
                let txn_id = txn.as_ref().map(|t| t.id).unwrap_or(0);
                tm.get_snapshot(txn_id)
            }),
        };

        let scan: Box<dyn Executor> = Box::new(SeqScanExecutor::new(
            Arc::clone(&bpm),
            catalog,
            table_id,
            None,
            None,
            snapshot,
            txn_manager,
        ));

        let filtered: Box<dyn Executor> = if let Some(pred) = &stmt.where_clause {
            Box::new(FilterExecutor::new(scan, pred.clone()))
        } else {
            scan
        };

        Box::new(UpdateExecutor::new(
            bpm,
            filtered,
            stmt.assignments.clone(),
            txn,
            lock_manager,
            wal_manager,
        ))
    }

    fn get_table_id_from_range_table(range_table: &[RangeTableEntry]) -> u32 {
        let rte = &range_table[0];
        match &rte.source {
            TableSource::BaseTable { table_id, .. } => *table_id,
        }
    }
}
