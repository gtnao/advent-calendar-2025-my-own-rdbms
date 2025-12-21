use anyhow::Result;

use crate::analyzer::AnalyzedExpr;
use crate::tuple::Value;

use super::{evaluate_expr, evaluate_expr_with_offsets, Executor, Tuple};

/// Projection executor for SELECT expressions (single table queries)
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

/// Projection executor with RTE offsets (for JOIN queries)
pub struct ProjectionWithOffsetsExecutor<'a> {
    child: Box<dyn Executor + 'a>,
    exprs: Vec<AnalyzedExpr>,
    rte_offsets: Vec<usize>,
}

impl<'a> ProjectionWithOffsetsExecutor<'a> {
    pub fn new(
        child: Box<dyn Executor + 'a>,
        exprs: Vec<AnalyzedExpr>,
        rte_offsets: Vec<usize>,
    ) -> Self {
        ProjectionWithOffsetsExecutor {
            child,
            exprs,
            rte_offsets,
        }
    }
}

impl Executor for ProjectionWithOffsetsExecutor<'_> {
    fn open(&mut self) -> Result<()> {
        self.child.open()
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if let Some(tuple) = self.child.next()? {
            let projected: Vec<Value> = self
                .exprs
                .iter()
                .map(|expr| evaluate_expr_with_offsets(expr, &tuple, &self.rte_offsets))
                .collect::<Result<Vec<_>>>()?;
            return Ok(Some(Tuple::new(projected)));
        }
        Ok(None)
    }
}
