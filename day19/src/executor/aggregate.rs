use std::collections::HashMap;

use anyhow::Result;

use crate::analyzer::{AnalyzedAggregate, AnalyzedAggregateArg, AnalyzedExpr, AnalyzedSelectItem};
use crate::ast::AggregateFunction;
use crate::tuple::Value;

use super::{evaluate_binary_op, evaluate_unary_op, literal_to_value, Executor, Tuple};

/// Running state for each aggregate function
#[derive(Debug, Clone)]
enum AggregateAccumulator {
    Count { count: i64 },
    Sum { sum: i64, has_value: bool },
    Avg { sum: i64, count: i64 },
    Min { min: Option<Value> },
    Max { max: Option<Value> },
}

impl AggregateAccumulator {
    fn new(func: &AggregateFunction) -> Self {
        match func {
            AggregateFunction::Count => AggregateAccumulator::Count { count: 0 },
            AggregateFunction::Sum => AggregateAccumulator::Sum {
                sum: 0,
                has_value: false,
            },
            AggregateFunction::Avg => AggregateAccumulator::Avg { sum: 0, count: 0 },
            AggregateFunction::Min => AggregateAccumulator::Min { min: None },
            AggregateFunction::Max => AggregateAccumulator::Max { max: None },
        }
    }

    fn accumulate(&mut self, value: &Value) {
        match self {
            AggregateAccumulator::Count { count } => {
                if *value != Value::Null {
                    *count += 1;
                }
            }
            AggregateAccumulator::Sum { sum, has_value } => {
                if let Value::Int(n) = value {
                    *sum += *n as i64;
                    *has_value = true;
                }
            }
            AggregateAccumulator::Avg { sum, count } => {
                if let Value::Int(n) = value {
                    *sum += *n as i64;
                    *count += 1;
                }
            }
            AggregateAccumulator::Min { min } => {
                if *value == Value::Null {
                    return;
                }
                *min = Some(match min.take() {
                    None => value.clone(),
                    Some(current) => {
                        if compare_values(value, &current) < 0 {
                            value.clone()
                        } else {
                            current
                        }
                    }
                });
            }
            AggregateAccumulator::Max { max } => {
                if *value == Value::Null {
                    return;
                }
                *max = Some(match max.take() {
                    None => value.clone(),
                    Some(current) => {
                        if compare_values(value, &current) > 0 {
                            value.clone()
                        } else {
                            current
                        }
                    }
                });
            }
        }
    }

    fn accumulate_count_star(&mut self) {
        if let AggregateAccumulator::Count { count } = self {
            *count += 1;
        }
    }

    fn finalize(&self) -> Value {
        match self {
            AggregateAccumulator::Count { count } => Value::Int(*count as i32),
            AggregateAccumulator::Sum { sum, has_value } => {
                if *has_value {
                    Value::Int(*sum as i32)
                } else {
                    Value::Null
                }
            }
            AggregateAccumulator::Avg { sum, count } => {
                if *count > 0 {
                    Value::Int((*sum / *count) as i32)
                } else {
                    Value::Null
                }
            }
            AggregateAccumulator::Min { min } => min.clone().unwrap_or(Value::Null),
            AggregateAccumulator::Max { max } => max.clone().unwrap_or(Value::Null),
        }
    }
}

/// State for one group: group key values + accumulators for each aggregate
#[derive(Debug, Clone)]
struct GroupState {
    group_values: Vec<Value>,
    accumulators: Vec<AggregateAccumulator>,
}

/// Aggregate executor using streaming/incremental aggregation.
///
/// Instead of collecting all tuples and then aggregating, this executor:
/// 1. Processes tuples one at a time
/// 2. Updates aggregate state in a HashMap keyed by group values
/// 3. Memory usage is O(number of groups) instead of O(number of tuples)
pub struct AggregateExecutor<'a> {
    child: Box<dyn Executor + 'a>,
    group_by: Vec<AnalyzedExpr>,
    select_items: Vec<AnalyzedSelectItem>,
    /// Aggregate info extracted from select_items
    aggregates: Vec<AnalyzedAggregate>,
    /// Grouped results after processing
    groups: HashMap<Vec<Value>, GroupState>,
    /// Results iterator
    results: Vec<Tuple>,
    pos: usize,
    processed: bool,
}

impl<'a> AggregateExecutor<'a> {
    pub fn new(
        child: Box<dyn Executor + 'a>,
        group_by: Vec<AnalyzedExpr>,
        select_items: Vec<AnalyzedSelectItem>,
    ) -> Self {
        // Extract all aggregates from select_items
        let mut aggregates = Vec::new();
        for item in &select_items {
            Self::collect_aggregates(&item.expr, &mut aggregates);
        }

        AggregateExecutor {
            child,
            group_by,
            select_items,
            aggregates,
            groups: HashMap::new(),
            results: Vec::new(),
            pos: 0,
            processed: false,
        }
    }

    /// Collect all aggregate expressions from an expression tree
    fn collect_aggregates(expr: &AnalyzedExpr, out: &mut Vec<AnalyzedAggregate>) {
        match expr {
            AnalyzedExpr::Aggregate(agg) => {
                // Check if this aggregate is already in the list
                let already_exists = out.iter().any(|a| Self::aggregates_equal(a, agg));
                if !already_exists {
                    out.push(agg.clone());
                }
            }
            AnalyzedExpr::BinaryOp { left, right, .. } => {
                Self::collect_aggregates(left, out);
                Self::collect_aggregates(right, out);
            }
            AnalyzedExpr::UnaryOp { expr, .. } => {
                Self::collect_aggregates(expr, out);
            }
            AnalyzedExpr::Literal(_) | AnalyzedExpr::ColumnRef(_) => {}
        }
    }

    fn aggregates_equal(a: &AnalyzedAggregate, b: &AnalyzedAggregate) -> bool {
        // Simple equality check - same function and same arg structure
        if a.func != b.func {
            return false;
        }
        match (&a.arg, &b.arg) {
            (AnalyzedAggregateArg::Asterisk, AnalyzedAggregateArg::Asterisk) => true,
            (AnalyzedAggregateArg::Expr(e1), AnalyzedAggregateArg::Expr(e2)) => {
                Self::exprs_equal(e1, e2)
            }
            _ => false,
        }
    }

    fn exprs_equal(a: &AnalyzedExpr, b: &AnalyzedExpr) -> bool {
        match (a, b) {
            (AnalyzedExpr::ColumnRef(c1), AnalyzedExpr::ColumnRef(c2)) => {
                c1.column_index == c2.column_index
            }
            (AnalyzedExpr::Literal(l1), AnalyzedExpr::Literal(l2)) => {
                literal_to_value(l1) == literal_to_value(l2)
            }
            _ => false,
        }
    }

    /// Evaluate a non-aggregate expression on a tuple
    fn evaluate_expr(expr: &AnalyzedExpr, tuple: &Tuple) -> Result<Value> {
        match expr {
            AnalyzedExpr::Literal(lit) => Ok(literal_to_value(lit)),
            AnalyzedExpr::ColumnRef(col_ref) => Ok(tuple.values[col_ref.column_index].clone()),
            AnalyzedExpr::BinaryOp {
                left, op, right, ..
            } => {
                let left_val = Self::evaluate_expr(left, tuple)?;
                let right_val = Self::evaluate_expr(right, tuple)?;
                evaluate_binary_op(op, &left_val, &right_val)
            }
            AnalyzedExpr::UnaryOp { op, expr, .. } => {
                let val = Self::evaluate_expr(expr, tuple)?;
                evaluate_unary_op(op, &val)
            }
            AnalyzedExpr::Aggregate(_) => {
                anyhow::bail!("aggregate in evaluate_expr - should use finalized values")
            }
        }
    }

    /// Compute group key from a tuple
    fn compute_group_key(&self, tuple: &Tuple) -> Result<Vec<Value>> {
        self.group_by
            .iter()
            .map(|expr| Self::evaluate_expr(expr, tuple))
            .collect()
    }

    /// Process all input tuples and build aggregate state
    fn process(&mut self) -> Result<()> {
        if self.processed {
            return Ok(());
        }
        self.processed = true;

        // Process each input tuple
        while let Some(tuple) = self.child.next()? {
            let key = if self.group_by.is_empty() {
                vec![]
            } else {
                self.compute_group_key(&tuple)?
            };

            // Get or create group state
            let group = self.groups.entry(key.clone()).or_insert_with(|| GroupState {
                group_values: key,
                accumulators: self
                    .aggregates
                    .iter()
                    .map(|agg| AggregateAccumulator::new(&agg.func))
                    .collect(),
            });

            // Update each accumulator
            for (i, agg) in self.aggregates.iter().enumerate() {
                match &agg.arg {
                    AnalyzedAggregateArg::Asterisk => {
                        group.accumulators[i].accumulate_count_star();
                    }
                    AnalyzedAggregateArg::Expr(expr) => {
                        let val = Self::evaluate_expr(expr, &tuple)?;
                        group.accumulators[i].accumulate(&val);
                    }
                }
            }
        }

        // If no groups and no GROUP BY, create empty group for scalar aggregates
        if self.groups.is_empty() && self.group_by.is_empty() {
            self.groups.insert(
                vec![],
                GroupState {
                    group_values: vec![],
                    accumulators: self
                        .aggregates
                        .iter()
                        .map(|agg| AggregateAccumulator::new(&agg.func))
                        .collect(),
                },
            );
        }

        // Build result tuples
        for (_key, group) in &self.groups {
            // Finalize aggregates
            let finalized: Vec<Value> = group
                .accumulators
                .iter()
                .map(|acc| acc.finalize())
                .collect();

            // Evaluate select items
            let mut output_values = Vec::new();
            for item in &self.select_items {
                let value =
                    self.evaluate_select_expr(&item.expr, &group.group_values, &finalized)?;
                output_values.push(value);
            }

            let result_tuple = Tuple::new(output_values);
            self.results.push(result_tuple);
        }

        Ok(())
    }

    /// Evaluate expression using finalized aggregate values
    fn evaluate_select_expr(
        &self,
        expr: &AnalyzedExpr,
        group_values: &[Value],
        finalized_aggs: &[Value],
    ) -> Result<Value> {
        match expr {
            AnalyzedExpr::Literal(lit) => Ok(literal_to_value(lit)),
            AnalyzedExpr::ColumnRef(col_ref) => {
                // Look up in group_by expressions
                for (i, gb_expr) in self.group_by.iter().enumerate() {
                    if let AnalyzedExpr::ColumnRef(gb_col) = gb_expr {
                        if gb_col.column_index == col_ref.column_index {
                            return Ok(group_values[i].clone());
                        }
                    }
                }
                anyhow::bail!(
                    "column not in GROUP BY: {}",
                    col_ref.column_name
                )
            }
            AnalyzedExpr::BinaryOp {
                left, op, right, ..
            } => {
                let left_val = self.evaluate_select_expr(left, group_values, finalized_aggs)?;
                let right_val = self.evaluate_select_expr(right, group_values, finalized_aggs)?;
                evaluate_binary_op(op, &left_val, &right_val)
            }
            AnalyzedExpr::UnaryOp { op, expr, .. } => {
                let val = self.evaluate_select_expr(expr, group_values, finalized_aggs)?;
                evaluate_unary_op(op, &val)
            }
            AnalyzedExpr::Aggregate(agg) => {
                // Find this aggregate in our list
                for (i, a) in self.aggregates.iter().enumerate() {
                    if Self::aggregates_equal(a, agg) {
                        return Ok(finalized_aggs[i].clone());
                    }
                }
                anyhow::bail!("aggregate not found in list")
            }
        }
    }
}

impl Executor for AggregateExecutor<'_> {
    fn open(&mut self) -> Result<()> {
        self.child.open()?;
        self.groups.clear();
        self.results.clear();
        self.pos = 0;
        self.processed = false;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        self.process()?;

        if self.pos < self.results.len() {
            let tuple = self.results[self.pos].clone();
            self.pos += 1;
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }
}

fn compare_values(a: &Value, b: &Value) -> i32 {
    match (a, b) {
        (Value::Int(a), Value::Int(b)) => a.cmp(b) as i32,
        (Value::Varchar(a), Value::Varchar(b)) => a.cmp(b) as i32,
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b) as i32,
        _ => 0,
    }
}

