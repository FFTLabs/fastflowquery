use crate::logical_plan::LogicalPlan;
use ffq_common::Result;

#[derive(Debug, Default)]
pub struct Analyzer;

impl Analyzer {
    pub fn new() -> Self {
        Self
    }

    pub fn analyze(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        Ok(plan)
    }
}
