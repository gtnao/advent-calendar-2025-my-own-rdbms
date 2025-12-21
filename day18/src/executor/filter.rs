use anyhow::Result;

use crate::analyzer::AnalyzedExpr;

use super::{evaluate_predicate, evaluate_predicate_with_offsets, Executor, Tuple};

/// Filter executor for WHERE clause (single table queries)
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
                return Ok(Some(tuple));
            }
        }
        Ok(None)
    }
}

/// Filter executor with RTE offsets (for JOIN queries)
pub struct FilterWithOffsetsExecutor<'a> {
    child: Box<dyn Executor + 'a>,
    predicate: AnalyzedExpr,
    rte_offsets: Vec<usize>,
}

impl<'a> FilterWithOffsetsExecutor<'a> {
    pub fn new(
        child: Box<dyn Executor + 'a>,
        predicate: AnalyzedExpr,
        rte_offsets: Vec<usize>,
    ) -> Self {
        FilterWithOffsetsExecutor {
            child,
            predicate,
            rte_offsets,
        }
    }
}

impl Executor for FilterWithOffsetsExecutor<'_> {
    fn open(&mut self) -> Result<()> {
        self.child.open()
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        while let Some(tuple) = self.child.next()? {
            if evaluate_predicate_with_offsets(&self.predicate, &tuple, &self.rte_offsets)? {
                return Ok(Some(tuple));
            }
        }
        Ok(None)
    }
}
