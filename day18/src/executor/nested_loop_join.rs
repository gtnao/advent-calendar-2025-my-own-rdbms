use anyhow::Result;

use crate::analyzer::AnalyzedExpr;
use crate::ast::JoinType;
use crate::tuple::Value;

use super::{evaluate_predicate_with_offsets, Executor, Tuple};

/// Nested Loop Join executor supporting INNER and LEFT JOINs.
///
/// # Algorithm Overview
///
/// The nested loop join works by iterating through each row in the left (outer) table
/// and for each left row, scanning all rows in the right (inner) table to find matches.
///
/// ## Join Type Handling
///
/// ### INNER JOIN
/// - Only emit rows where the join condition matches
/// - Simple nested loop: for each left row, scan right and emit matching combinations
///
/// ### LEFT [OUTER] JOIN
/// - Emit all left rows
/// - If a left row has no matching right rows, emit it with NULLs for right columns
/// - Uses `left_matched` flag to track if current left row found any match
pub struct NestedLoopJoinExecutor<'a> {
    left: Box<dyn Executor + 'a>,
    right: Box<dyn Executor + 'a>,
    condition: AnalyzedExpr,
    join_type: JoinType,
    /// Offsets into combined tuple for each RTE.
    /// e.g., if left has 3 columns and right has 2, offsets = [0, 3]
    rte_offsets: Vec<usize>,
    /// Number of columns in right side (for creating NULL tuples)
    right_col_count: usize,
    /// Current left tuple being processed
    current_left: Option<Tuple>,
    /// Whether current left tuple matched any right tuple (for LEFT join)
    left_matched: bool,
}

impl<'a> NestedLoopJoinExecutor<'a> {
    pub fn new(
        left: Box<dyn Executor + 'a>,
        right: Box<dyn Executor + 'a>,
        condition: AnalyzedExpr,
        join_type: JoinType,
        rte_offsets: Vec<usize>,
        right_col_count: usize,
    ) -> Self {
        NestedLoopJoinExecutor {
            left,
            right,
            condition,
            join_type,
            rte_offsets,
            right_col_count,
            current_left: None,
            left_matched: false,
        }
    }

    /// Combine left and right tuples into a single tuple
    fn combine_tuples(&self, left: &Tuple, right: &Tuple) -> Tuple {
        let mut values = left.values.clone();
        values.extend(right.values.clone());
        Tuple::new(values)
    }

    /// Create a tuple with all NULL values for right columns
    fn make_right_null_tuple(&self) -> Tuple {
        Tuple::new(vec![Value::Null; self.right_col_count])
    }
}

impl Executor for NestedLoopJoinExecutor<'_> {
    fn open(&mut self) -> Result<()> {
        self.left.open()?;
        self.current_left = None;
        self.left_matched = false;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        loop {
            // Get next left tuple if needed
            if self.current_left.is_none() {
                match self.left.next()? {
                    Some(left_tuple) => {
                        self.current_left = Some(left_tuple);
                        self.left_matched = false;
                        // Re-scan right executor for new left tuple
                        self.right.open()?;
                    }
                    None => {
                        return Ok(None);
                    }
                }
            }

            let left_tuple = self.current_left.as_ref().unwrap().clone();

            // Get next right tuple
            match self.right.next()? {
                Some(right_tuple) => {
                    let combined = self.combine_tuples(&left_tuple, &right_tuple);

                    // Check join condition
                    if evaluate_predicate_with_offsets(
                        &self.condition,
                        &combined,
                        &self.rte_offsets,
                    )? {
                        self.left_matched = true;
                        return Ok(Some(combined));
                    }
                    // No match, continue to next right tuple
                }
                None => {
                    // Right exhausted for current left
                    // LEFT JOIN: emit unmatched left with NULL right
                    if self.join_type == JoinType::Left && !self.left_matched {
                        let right_null = self.make_right_null_tuple();
                        let combined = self.combine_tuples(&left_tuple, &right_null);
                        self.current_left = None;
                        return Ok(Some(combined));
                    }
                    // Move to next left tuple
                    self.current_left = None;
                }
            }
        }
    }
}
