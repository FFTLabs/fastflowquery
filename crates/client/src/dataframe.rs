use arrow::record_batch::RecordBatch;
use ffq_common::{FfqError, Result};
use ffq_planner::{AggExpr, Expr, JoinType, LogicalPlan};
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
        Self {
            session,
            logical_plan,
        }
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
            join_type: JoinType::Inner,
            strategy_hint: ffq_planner::JoinStrategyHint::Auto,
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

    pub fn explain(&self) -> Result<String> {
        struct CatalogProvider<'a> {
            catalog: &'a ffq_storage::Catalog,
        }
        impl<'a> ffq_planner::SchemaProvider for CatalogProvider<'a> {
            fn table_schema(&self, table: &str) -> ffq_common::Result<arrow_schema::SchemaRef> {
                let t = self.catalog.get(table)?;
                t.schema_ref()
            }
        }
        impl<'a> ffq_planner::OptimizerContext for CatalogProvider<'a> {
            fn table_stats(&self, table: &str) -> ffq_common::Result<(Option<u64>, Option<u64>)> {
                let t = self.catalog.get(table)?;
                Ok((t.stats.bytes, t.stats.rows))
            }
        }

        let cat = self.session.catalog.read().expect("catalog lock poisoned");
        let provider = CatalogProvider { catalog: &*cat };

        let opt = self.session.planner.optimize_only(
            self.logical_plan.clone(),
            &provider,
            &self.session.config,
        )?;

        Ok(ffq_planner::explain_logical(&opt))
    }

    /// df.collect() (async)
    pub async fn collect(&self) -> Result<Vec<RecordBatch>> {
        // Ensure both SQL-built and DataFrame-built plans go through the same analyze/optimize pipeline.
        // Build a schema provider from the catalog
        struct CatalogProvider<'a> {
            catalog: &'a ffq_storage::Catalog,
        }
        impl<'a> ffq_planner::SchemaProvider for CatalogProvider<'a> {
            fn table_schema(&self, table: &str) -> ffq_common::Result<arrow_schema::SchemaRef> {
                let t = self.catalog.get(table)?;
                t.schema_ref()
            }
        }
        impl<'a> ffq_planner::OptimizerContext for CatalogProvider<'a> {
            fn table_stats(&self, table: &str) -> ffq_common::Result<(Option<u64>, Option<u64>)> {
                let t = self.catalog.get(table)?;
                Ok((t.stats.bytes, t.stats.rows))
            }
        }

        let (analyzed, catalog_snapshot) = {
            let cat_guard = self.session.catalog.read().expect("catalog lock poisoned");
            let provider = CatalogProvider {
                catalog: &*cat_guard,
            };

            let analyzed = self.session.planner.optimize_analyze(
                self.logical_plan.clone(),
                &provider,
                &self.session.config,
            )?;
            (analyzed, std::sync::Arc::new((*cat_guard).clone()))
        };

        let physical = self.session.planner.create_physical_plan(&analyzed)?;

        let ctx = QueryContext {
            batch_size_rows: self.session.config.batch_size_rows,
            mem_budget_bytes: self.session.config.mem_budget_bytes,
            spill_dir: self.session.config.spill_dir.clone(),
        };

        let stream: ffq_execution::stream::SendableRecordBatchStream = self
            .session
            .runtime
            .execute(physical, ctx, catalog_snapshot)
            .await?;

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
