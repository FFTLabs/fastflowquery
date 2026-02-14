#[cfg(feature = "grpc")]
pub mod grpc;
pub mod stage;

use ffq_common::Result;
use ffq_planner::PhysicalPlan;
pub use stage::{StageDag, StageId, StageNode};

#[derive(Debug, Default)]
pub struct DistributedRuntime;

impl DistributedRuntime {
    pub fn build_stage_dag(&self, plan: &PhysicalPlan) -> Result<StageDag> {
        Ok(stage::build_stage_dag(plan))
    }
}
