//! Distributed coordinator/worker runtime building blocks.
//!
//! Architecture role:
//! - coordinator state machine and scheduling APIs
//! - worker execution/control-plane integration (feature-gated)
//! - stage DAG construction from physical plans
//!
//! Key modules:
//! - [`coordinator`]
//! - [`stage`]
//! - `worker` (feature-gated)
//! - `grpc` (feature-gated)
//!
//! Feature flags:
//! - `grpc`: enables tonic-generated RPC services and client/server glue.

pub mod coordinator;
#[cfg(feature = "grpc")]
pub mod grpc;
pub mod stage;
#[cfg(feature = "grpc")]
pub mod worker;

pub use coordinator::{
    Coordinator, CoordinatorConfig, MapOutputPartitionMeta, QueryState, QueryStatus, StageMetrics,
    TaskAssignment, TaskState,
};
use ffq_common::Result;
use ffq_planner::PhysicalPlan;
pub use stage::{StageDag, StageId, StageNode};
#[cfg(feature = "grpc")]
pub use worker::{
    DefaultTaskExecutor, GrpcControlPlane, InProcessControlPlane, TaskContext as WorkerTaskContext,
    TaskExecutionResult, TaskExecutor, Worker, WorkerConfig, WorkerControlPlane,
};

#[derive(Debug, Default)]
pub struct DistributedRuntime;

impl DistributedRuntime {
    pub fn build_stage_dag(&self, plan: &PhysicalPlan) -> Result<StageDag> {
        Ok(stage::build_stage_dag(plan))
    }
}
