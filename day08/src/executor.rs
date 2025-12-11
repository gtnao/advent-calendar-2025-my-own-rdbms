use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::analyzer::{
    AnalyzedAssignment, AnalyzedExpr, AnalyzedInsertStatement, AnalyzedLiteral,
    AnalyzedSelectStatement, AnalyzedStatement, LiteralValue, TableSource,
};
use crate::ast::{BinaryOperator, UnaryOperator};
use crate::buffer_pool::BufferPoolManager;
use crate::catalog::Catalog;
use crate::tuple::{deserialize_tuple, serialize_tuple, Value};

// Row ID: page_id + slot_id
#[derive(Debug, Clone, Copy)]
pub struct Rid {
    pub page_id: u32,
    pub slot_id: u16,
}

// Runtime representation of a row during query execution.
// (tuple.rs handles serialization/deserialization for storage)
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

// Volcano model: open/next/close interface
pub trait Executor {
    fn open(&mut self) -> Result<()>;
    fn next(&mut self) -> Result<Option<Tuple>>;
}

// Sequential scan executor
pub struct SeqScanExecutor<'a> {
    bpm: Arc<Mutex<BufferPoolManager>>,
    catalog: &'a Catalog,
    table_id: usize,
    current_page_id: u32,
    current_slot_id: u16,
}

impl<'a> SeqScanExecutor<'a> {
    pub fn new(
        bpm: Arc<Mutex<BufferPoolManager>>,
        catalog: &'a Catalog,
        table_id: usize,
    ) -> Self {
        SeqScanExecutor {
            bpm,
            catalog,
            table_id,
            current_page_id: 0,
            current_slot_id: 0,
        }
    }
}

impl Executor for SeqScanExecutor<'_> {
    fn open(&mut self) -> Result<()> {
        self.current_page_id = 0;
        self.current_slot_id = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        let table = self
            .catalog
            .get_table_by_id(self.table_id)
            .ok_or_else(|| anyhow::anyhow!("table not found"))?;
        let schema = table.to_schema();

        // Get page count (short lock)
        let page_count = self.bpm.lock().unwrap().page_count();

        while self.current_page_id < page_count {
            // Fetch page (short lock on BPM)
            let page_arc = self.bpm.lock().unwrap().fetch_page(self.current_page_id)?;

            // Read from page (RwLock on Page, BPM lock released)
            let page_guard = page_arc.read().unwrap();
            let tuple_count = page_guard.tuple_count();

            while self.current_slot_id < tuple_count {
                if let Some(tuple_data) = page_guard.get_tuple(self.current_slot_id) {
                    let values = deserialize_tuple(tuple_data, &schema)?;
                    let rid = Rid {
                        page_id: self.current_page_id,
                        slot_id: self.current_slot_id,
                    };
                    self.current_slot_id += 1;
                    drop(page_guard);
                    self.bpm.lock().unwrap().unpin_page(self.current_page_id, false)?;
                    return Ok(Some(Tuple::with_rid(values, rid)));
                }
                self.current_slot_id += 1;
            }

            drop(page_guard);
            self.bpm.lock().unwrap().unpin_page(self.current_page_id, false)?;
            self.current_page_id += 1;
            self.current_slot_id = 0;
        }

        Ok(None)
    }
}

// Filter executor (WHERE clause)
pub struct FilterExecutor<'a> {
    child: Box<dyn Executor + 'a>,
    predicate: AnalyzedExpr,
}

impl<'a> FilterExecutor<'a> {
    pub fn new(child: Box<dyn Executor + 'a>, predicate: AnalyzedExpr) -> Self {
        FilterExecutor { child, predicate }
    }
}

impl Executor for FilterExecutor<'_> {
    fn open(&mut self) -> Result<()> {
        self.child.open()
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        while let Some(tuple) = self.child.next()? {
            if evaluate_predicate(&self.predicate, &tuple)? {
                // Preserve RID from child
                return Ok(Some(tuple));
            }
        }
        Ok(None)
    }
}

// Projection executor (SELECT expressions)
pub struct ProjectionExecutor<'a> {
    child: Box<dyn Executor + 'a>,
    exprs: Vec<AnalyzedExpr>,
}

impl<'a> ProjectionExecutor<'a> {
    pub fn new(child: Box<dyn Executor + 'a>, exprs: Vec<AnalyzedExpr>) -> Self {
        ProjectionExecutor { child, exprs }
    }
}

impl Executor for ProjectionExecutor<'_> {
    fn open(&mut self) -> Result<()> {
        self.child.open()
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if let Some(tuple) = self.child.next()? {
            let projected: Vec<Value> = self
                .exprs
                .iter()
                .map(|expr| evaluate_expr(expr, &tuple))
                .collect::<Result<Vec<_>>>()?;
            return Ok(Some(Tuple::new(projected)));
        }
        Ok(None)
    }
}

// Insert executor
pub struct InsertExecutor {
    bpm: Arc<Mutex<BufferPoolManager>>,
    values: Vec<Value>,
    executed: bool,
}

impl InsertExecutor {
    pub fn new(
        bpm: Arc<Mutex<BufferPoolManager>>,
        stmt: &AnalyzedInsertStatement,
    ) -> Result<Self> {
        let values: Vec<Value> = stmt
            .values
            .iter()
            .map(|expr| match expr {
                AnalyzedExpr::Literal(lit) => Ok(literal_to_value(lit)),
                _ => anyhow::bail!("only literals supported in INSERT"),
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(InsertExecutor {
            bpm,
            values,
            executed: false,
        })
    }
}

impl Executor for InsertExecutor {
    fn open(&mut self) -> Result<()> {
        self.executed = false;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;

        let tuple_data = serialize_tuple(&self.values);

        // Retry loop: if insert fails due to concurrent access, try again
        loop {
            // Try to insert into the last page
            let page_count = self.bpm.lock().unwrap().page_count();
            if page_count > 0 {
                let last_page_id = page_count - 1;
                let page_arc = self.bpm.lock().unwrap().fetch_page_mut(last_page_id)?;
                let mut page_guard = page_arc.write().unwrap();
                if page_guard.insert(&tuple_data).is_ok() {
                    drop(page_guard);
                    self.bpm.lock().unwrap().unpin_page(last_page_id, true)?;
                    return Ok(Some(Tuple::new(vec![Value::Int(1)])));
                }
                drop(page_guard);
                self.bpm.lock().unwrap().unpin_page(last_page_id, false)?;
            }

            // Allocate new page and try to insert
            let (page_id, page_arc) = self.bpm.lock().unwrap().new_page()?;
            let mut page_guard = page_arc.write().unwrap();
            if page_guard.insert(&tuple_data).is_ok() {
                drop(page_guard);
                self.bpm.lock().unwrap().unpin_page(page_id, true)?;
                return Ok(Some(Tuple::new(vec![Value::Int(1)])));
            }
            // Insert failed on new page (another thread filled it), retry from the beginning
            drop(page_guard);
            self.bpm.lock().unwrap().unpin_page(page_id, false)?;
        }
    }
}

// Delete executor - with child executor (SeqScan + Filter)
pub struct DeleteExecutor<'a> {
    bpm: Arc<Mutex<BufferPoolManager>>,
    child: Box<dyn Executor + 'a>,
    executed: bool,
}

impl<'a> DeleteExecutor<'a> {
    pub fn new(bpm: Arc<Mutex<BufferPoolManager>>, child: Box<dyn Executor + 'a>) -> Self {
        DeleteExecutor {
            bpm,
            child,
            executed: false,
        }
    }
}

impl Executor for DeleteExecutor<'_> {
    fn open(&mut self) -> Result<()> {
        self.executed = false;
        self.child.open()
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;

        // Must collect all targets first to avoid "read-while-write" problem:
        // If we delete while iterating, the scan may see inconsistent state
        let mut targets = Vec::new();
        while let Some(tuple) = self.child.next()? {
            if let Some(rid) = tuple.rid {
                targets.push(rid);
            }
        }

        // Now delete all targets
        let deleted_count = targets.len() as i32;
        for rid in targets {
            let page_arc = self.bpm.lock().unwrap().fetch_page_mut(rid.page_id)?;
            let mut page_guard = page_arc.write().unwrap();
            page_guard.delete(rid.slot_id)?;
            drop(page_guard);
            self.bpm.lock().unwrap().unpin_page(rid.page_id, true)?;
        }

        Ok(Some(Tuple::new(vec![Value::Int(deleted_count)])))
    }
}

// Update executor - with child executor (SeqScan + Filter)
pub struct UpdateExecutor<'a> {
    bpm: Arc<Mutex<BufferPoolManager>>,
    child: Box<dyn Executor + 'a>,
    assignments: Vec<AnalyzedAssignment>,
    executed: bool,
}

impl<'a> UpdateExecutor<'a> {
    pub fn new(
        bpm: Arc<Mutex<BufferPoolManager>>,
        child: Box<dyn Executor + 'a>,
        assignments: Vec<AnalyzedAssignment>,
    ) -> Self {
        UpdateExecutor {
            bpm,
            child,
            assignments,
            executed: false,
        }
    }
}

impl Executor for UpdateExecutor<'_> {
    fn open(&mut self) -> Result<()> {
        self.executed = false;
        self.child.open()
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;

        // Must collect all updates first to avoid "read-while-write" problem:
        // UPDATE inserts new tuples, which SeqScan would see and process again,
        // causing an infinite loop
        let mut updates = Vec::new();
        while let Some(tuple) = self.child.next()? {
            if let Some(rid) = tuple.rid {
                let mut new_values = tuple.values.clone();
                for AnalyzedAssignment {
                    column_index,
                    value,
                } in &self.assignments
                {
                    let new_value = evaluate_expr(value, &tuple)?;
                    new_values[*column_index] = new_value;
                }
                updates.push((rid, new_values));
            }
        }

        // Apply updates
        let updated_count = updates.len() as i32;
        for (rid, new_values) in updates {
            // Delete old tuple
            let page_arc = self.bpm.lock().unwrap().fetch_page_mut(rid.page_id)?;
            let mut page_guard = page_arc.write().unwrap();
            page_guard.delete(rid.slot_id)?;
            drop(page_guard);
            self.bpm.lock().unwrap().unpin_page(rid.page_id, true)?;

            // Insert new tuple
            let tuple_data = serialize_tuple(&new_values);
            loop {
                let page_count = self.bpm.lock().unwrap().page_count();
                if page_count > 0 {
                    let last_page_id = page_count - 1;
                    let page_arc = self.bpm.lock().unwrap().fetch_page_mut(last_page_id)?;
                    let mut page_guard = page_arc.write().unwrap();
                    if page_guard.insert(&tuple_data).is_ok() {
                        drop(page_guard);
                        self.bpm.lock().unwrap().unpin_page(last_page_id, true)?;
                        break;
                    }
                    drop(page_guard);
                    self.bpm.lock().unwrap().unpin_page(last_page_id, false)?;
                }

                let (page_id, page_arc) = self.bpm.lock().unwrap().new_page()?;
                let mut page_guard = page_arc.write().unwrap();
                if page_guard.insert(&tuple_data).is_ok() {
                    drop(page_guard);
                    self.bpm.lock().unwrap().unpin_page(page_id, true)?;
                    break;
                }
                drop(page_guard);
                self.bpm.lock().unwrap().unpin_page(page_id, false)?;
            }
        }

        Ok(Some(Tuple::new(vec![Value::Int(updated_count)])))
    }
}

// Expression evaluation
pub fn evaluate_expr(expr: &AnalyzedExpr, tuple: &Tuple) -> Result<Value> {
    match expr {
        AnalyzedExpr::Literal(lit) => Ok(literal_to_value(lit)),
        AnalyzedExpr::ColumnRef(col_ref) => Ok(tuple.values[col_ref.column_index].clone()),
        AnalyzedExpr::BinaryOp {
            left,
            op,
            right,
            ..
        } => {
            let left_val = evaluate_expr(left, tuple)?;
            let right_val = evaluate_expr(right, tuple)?;
            evaluate_binary_op(op, &left_val, &right_val)
        }
        AnalyzedExpr::UnaryOp { op, expr, .. } => {
            let val = evaluate_expr(expr, tuple)?;
            evaluate_unary_op(op, &val)
        }
    }
}

fn literal_to_value(lit: &AnalyzedLiteral) -> Value {
    match &lit.value {
        LiteralValue::Integer(n) => Value::Int(*n as i32),
        LiteralValue::String(s) => Value::Varchar(s.clone()),
        LiteralValue::Boolean(b) => Value::Bool(*b),
        LiteralValue::Null => Value::Null,
    }
}

fn evaluate_binary_op(op: &BinaryOperator, left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Int(l), Value::Int(r)) => {
            let result = match op {
                BinaryOperator::Eq => Value::Bool(*l == *r),
                BinaryOperator::Ne => Value::Bool(*l != *r),
                BinaryOperator::Lt => Value::Bool(*l < *r),
                BinaryOperator::Le => Value::Bool(*l <= *r),
                BinaryOperator::Gt => Value::Bool(*l > *r),
                BinaryOperator::Ge => Value::Bool(*l >= *r),
                BinaryOperator::Add => Value::Int(l + r),
                BinaryOperator::Sub => Value::Int(l - r),
                BinaryOperator::Mul => Value::Int(l * r),
                BinaryOperator::Div => {
                    if *r == 0 {
                        anyhow::bail!("division by zero");
                    }
                    Value::Int(l / r)
                }
                BinaryOperator::And | BinaryOperator::Or => {
                    anyhow::bail!("AND/OR not supported for Int")
                }
            };
            Ok(result)
        }
        (Value::Bool(l), Value::Bool(r)) => {
            let result = match op {
                BinaryOperator::And => Value::Bool(*l && *r),
                BinaryOperator::Or => Value::Bool(*l || *r),
                BinaryOperator::Eq => Value::Bool(*l == *r),
                BinaryOperator::Ne => Value::Bool(*l != *r),
                _ => anyhow::bail!("unsupported operation for Bool"),
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
                _ => anyhow::bail!("unsupported operation for VARCHAR"),
            };
            Ok(result)
        }
        _ => anyhow::bail!("type mismatch in binary operation"),
    }
}

fn evaluate_unary_op(op: &UnaryOperator, val: &Value) -> Result<Value> {
    match (op, val) {
        (UnaryOperator::Not, Value::Bool(b)) => Ok(Value::Bool(!b)),
        (UnaryOperator::Neg, Value::Int(v)) => Ok(Value::Int(-v)),
        _ => anyhow::bail!("unsupported unary operation"),
    }
}

fn evaluate_predicate(expr: &AnalyzedExpr, tuple: &Tuple) -> Result<bool> {
    let val = evaluate_expr(expr, tuple)?;
    match val {
        Value::Bool(b) => Ok(b),
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
    ) -> Result<Self> {
        let executor = Self::build_executor(bpm, catalog, stmt)?;
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

    fn build_executor(
        bpm: Arc<Mutex<BufferPoolManager>>,
        catalog: &'a Catalog,
        stmt: &AnalyzedStatement,
    ) -> Result<Box<dyn Executor + 'a>> {
        match stmt {
            AnalyzedStatement::Select(s) => Ok(Self::build_select_executor(bpm, catalog, s)),
            AnalyzedStatement::Insert(s) => Ok(Box::new(InsertExecutor::new(bpm, s)?)),
            AnalyzedStatement::CreateTable(_) => {
                anyhow::bail!("CREATE TABLE not implemented in executor")
            }
            AnalyzedStatement::Delete(s) => Ok(Self::build_delete_executor(bpm, catalog, s)),
            AnalyzedStatement::Update(s) => Ok(Self::build_update_executor(bpm, catalog, s)),
        }
    }

    fn build_select_executor(
        bpm: Arc<Mutex<BufferPoolManager>>,
        catalog: &'a Catalog,
        stmt: &AnalyzedSelectStatement,
    ) -> Box<dyn Executor + 'a> {
        // Get table_id from RTE
        let rte = &stmt.range_table[stmt.from_rte_index];
        let table_id = match &rte.source {
            TableSource::BaseTable { table_id, .. } => *table_id,
        };

        // SeqScan
        let scan: Box<dyn Executor> = Box::new(SeqScanExecutor::new(bpm, catalog, table_id));

        // Filter (if WHERE clause exists)
        let filtered: Box<dyn Executor> = if let Some(pred) = &stmt.where_clause {
            Box::new(FilterExecutor::new(scan, pred.clone()))
        } else {
            scan
        };

        // Projection - evaluate expressions from select_items
        let exprs: Vec<AnalyzedExpr> = stmt
            .select_items
            .iter()
            .map(|item| item.expr.clone())
            .collect();

        Box::new(ProjectionExecutor::new(filtered, exprs))
    }

    fn build_delete_executor(
        bpm: Arc<Mutex<BufferPoolManager>>,
        catalog: &'a Catalog,
        stmt: &crate::analyzer::AnalyzedDeleteStatement,
    ) -> Box<dyn Executor + 'a> {
        let table_id = Self::get_table_id_from_range_table(&stmt.range_table);

        // SeqScan
        let scan: Box<dyn Executor> =
            Box::new(SeqScanExecutor::new(Arc::clone(&bpm), catalog, table_id));

        // Filter (if WHERE clause exists)
        let filtered: Box<dyn Executor> = if let Some(pred) = &stmt.where_clause {
            Box::new(FilterExecutor::new(scan, pred.clone()))
        } else {
            scan
        };

        Box::new(DeleteExecutor::new(bpm, filtered))
    }

    fn build_update_executor(
        bpm: Arc<Mutex<BufferPoolManager>>,
        catalog: &'a Catalog,
        stmt: &crate::analyzer::AnalyzedUpdateStatement,
    ) -> Box<dyn Executor + 'a> {
        let table_id = Self::get_table_id_from_range_table(&stmt.range_table);

        // SeqScan
        let scan: Box<dyn Executor> =
            Box::new(SeqScanExecutor::new(Arc::clone(&bpm), catalog, table_id));

        // Filter (if WHERE clause exists)
        let filtered: Box<dyn Executor> = if let Some(pred) = &stmt.where_clause {
            Box::new(FilterExecutor::new(scan, pred.clone()))
        } else {
            scan
        };

        Box::new(UpdateExecutor::new(bpm, filtered, stmt.assignments.clone()))
    }

    fn get_table_id_from_range_table(
        range_table: &[crate::analyzer::RangeTableEntry],
    ) -> usize {
        let rte = &range_table[0];
        match &rte.source {
            TableSource::BaseTable { table_id, .. } => *table_id,
        }
    }
}
