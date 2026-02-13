use std::collections::HashMap;

use ffq_common::Result;
use ffq_planner::{Analyzer, LiteralValue, LogicalPlan, Optimizer, PhysicalPlan, SchemaProvider};

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

    pub fn analyze_optimize(
        &self,
        plan: LogicalPlan,
        provider: &dyn SchemaProvider,
    ) -> Result<LogicalPlan> {
        let analyzed = self.analyzer.analyze(plan, provider)?;
        self.optimizer.optimize(analyzed)
    }

    pub fn create_physical_plan(&self, _logical: &LogicalPlan) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::Placeholder("empty".to_string()))
    }
}
