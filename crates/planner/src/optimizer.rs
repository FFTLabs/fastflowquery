use crate::logical_plan::LogicalPlan;
use ffq_common::Result;

#[derive(Debug, Default)]
pub struct Optimizer;

impl Optimizer {
    pub fn new() -> Self {
        Self
    }

    pub fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        Ok(plan)
    }
}
