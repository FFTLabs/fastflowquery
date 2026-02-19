use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::DataType;
use ffq_common::{EngineConfig, Result};
use ffq_planner::{
    Analyzer, LiteralValue, LogicalPlan, Optimizer, OptimizerConfig, OptimizerContext,
    OptimizerRule, PhysicalPlan, ScalarUdfTypeResolver,
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

    pub fn create_physical_plan(&self, logical: &LogicalPlan) -> Result<PhysicalPlan> {
        let cfg = ffq_planner::PhysicalPlannerConfig::default();
        ffq_planner::create_physical_plan(logical, &cfg)
    }

    pub fn register_optimizer_rule(&self, rule: Arc<dyn OptimizerRule>) -> bool {
        self.optimizer.register_rule(rule)
    }

    pub fn deregister_optimizer_rule(&self, name: &str) -> bool {
        self.optimizer.deregister_rule(name)
    }

    pub fn register_scalar_udf_type(
        &self,
        name: impl Into<String>,
        resolver: ScalarUdfTypeResolver,
    ) -> bool {
        self.analyzer.register_scalar_udf_type(name, resolver)
    }

    pub fn deregister_scalar_udf_type(&self, name: &str) -> bool {
        self.analyzer.deregister_scalar_udf_type(name)
    }

    pub fn register_numeric_passthrough_udf_type(&self, name: impl Into<String>) -> bool {
        let resolver: ScalarUdfTypeResolver = Arc::new(|arg_types: &[DataType]| {
            let out = if arg_types
                .iter()
                .any(|dt| matches!(dt, DataType::Float64 | DataType::Float32))
            {
                DataType::Float64
            } else if arg_types
                .iter()
                .all(|dt| matches!(dt, DataType::Int64 | DataType::Int32 | DataType::Int16))
            {
                DataType::Int64
            } else {
                return Err(ffq_common::FfqError::Planning(
                    "scalar udf requires numeric arguments".to_string(),
                ));
            };
            Ok(out)
        });
        self.analyzer.register_scalar_udf_type(name, resolver)
    }
}
