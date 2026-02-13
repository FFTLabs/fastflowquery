use std::collections::HashMap;

use ffq_common::{EngineConfig, Result};
use ffq_planner::{
    Analyzer, LiteralValue, LogicalPlan, Optimizer, OptimizerConfig, OptimizerContext, PhysicalPlan
};

#[derive(Debug, Default)]
pub struct PlannerFacade {
    analyzer: Analyzer,
    optimizer: Optimizer,
}

impl PlannerFacade {
    pub fn new() -> Self {
        Self {
            analyzer: Analyzer::new(),
            optimizer: Optimizer::new(),
        }
    }

    pub fn plan_sql(&self, sql: &str) -> Result<LogicalPlan> {
        self.plan_sql_with_params(sql, &HashMap::new())
    }

    pub fn plan_sql_with_params(
        &self,
        sql: &str,
        params: &HashMap<String, LiteralValue>,
    ) -> Result<LogicalPlan> {
        ffq_planner::sql_to_logical(sql, params)
    }

    /// v1: optimizer first (pushdown changes projection), then analyzer (name->idx, casts)
    pub fn optimize_analyze(
        &self,
        plan: LogicalPlan,
        ctx: &dyn OptimizerContext,
        cfg: &EngineConfig,
    ) -> Result<LogicalPlan> {
        let opt = self.optimizer.optimize(
            plan,
            ctx,
            OptimizerConfig {
                broadcast_threshold_bytes: cfg.broadcast_threshold_bytes,
            },
        )?;
        let analyzed = self.analyzer.analyze(opt, ctx)?;
        Ok(analyzed)
    }

    pub fn optimize_only(
        &self,
        plan: LogicalPlan,
        ctx: &dyn OptimizerContext,
        cfg: &EngineConfig,
    ) -> Result<LogicalPlan> {
        self.optimizer.optimize(
            plan,
            ctx,
            OptimizerConfig {
                broadcast_threshold_bytes: cfg.broadcast_threshold_bytes,
            },
        )
    }

    pub fn create_physical_plan(&self, _logical: &LogicalPlan) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::Placeholder("empty".to_string()))
    }
}
