use arrow::record_batch::RecordBatch;
use ffq_common::{FfqError, Result};
use ffq_planner::{AggExpr, Expr, LogicalPlan};
use futures::TryStreamExt;

use crate::runtime::QueryContext;
use crate::session::SharedSession;

#[derive(Debug, Clone)]
pub struct DataFrame {
    session: SharedSession,
    logical_plan: LogicalPlan,
}

impl DataFrame {
    pub(crate) fn new(session: SharedSession, logical_plan: LogicalPlan) -> Self {
        Self { session, logical_plan }
    }

    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.logical_plan
    }

    /// ctx.table("t") -> TableScan
    pub fn table(session: SharedSession, table: &str) -> Self {
        let plan = LogicalPlan::TableScan {
            table: table.to_string(),
            projection: None,
            filters: vec![],
            schema: None,
        };
        Self::new(session, plan)
    }

    /// df.filter(expr)
    pub fn filter(self, predicate: Expr) -> Self {
        let plan = LogicalPlan::Filter {
            predicate,
            input: Box::new(self.logical_plan),
        };
        Self::new(self.session, plan)
    }

    /// df.join(df2, on)
    /// on = vec![("left_key", "right_key"), ...]
    pub fn join(self, right: DataFrame, on: Vec<(String, String)>) -> Result<Self> {
        // Safety: joining DataFrames from different Engines/sessions is almost certainly a mistake.
        if !std::sync::Arc::ptr_eq(&self.session, &right.session) {
            return Err(FfqError::Planning(
                "cannot join DataFrames from different Engine instances".to_string(),
            ));
        }

        let plan = LogicalPlan::Join {
            left: Box::new(self.logical_plan),
            right: Box::new(right.logical_plan),
            on,
        };
        Ok(Self::new(self.session, plan))
    }

    /// df.groupby(keys)
    pub fn groupby(self, keys: Vec<Expr>) -> GroupedDataFrame {
        GroupedDataFrame {
            session: self.session,
            input: self.logical_plan,
            keys,
        }
    }

    /// df.collect() (async)
    pub async fn collect(&self) -> Result<Vec<RecordBatch>> {
        // Ensure both SQL-built and DataFrame-built plans go through the same analyze/optimize pipeline.
        let optimized = self.session.planner.optimize_logical(self.logical_plan.clone())?;
        let physical = self.session.planner.create_physical_plan(&optimized)?;

        let ctx = QueryContext {
            batch_size_rows: self.session.config.batch_size_rows,
        };

        let stream = self.session.runtime.execute(physical, ctx).await?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        Ok(batches)
    }
}

#[derive(Debug, Clone)]
pub struct GroupedDataFrame {
    session: SharedSession,
    input: LogicalPlan,
    keys: Vec<Expr>,
}

impl GroupedDataFrame {
    /// df.groupby(keys).agg(...)
    pub fn agg(self, aggs: Vec<(AggExpr, String)>) -> DataFrame {
        let plan = LogicalPlan::Aggregate {
            group_exprs: self.keys,
            aggr_exprs: aggs,
            input: Box::new(self.input),
        };
        DataFrame::new(self.session, plan)
    }
}
