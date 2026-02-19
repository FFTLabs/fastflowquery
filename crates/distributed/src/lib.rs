#![warn(missing_docs)]

//! Distributed coordinator/worker runtime building blocks.
//!
//! Architecture role:
//! - coordinator state machine and scheduling APIs
//! - worker execution/control-plane integration (feature-gated)
//! - stage DAG construction from physical plans
//! - gRPC control/shuffle/query-result RPC bindings (feature-gated)
//!
//! Key modules:
//! - [`coordinator`]
//! - [`stage`]
//! - `worker` (feature-gated)
//! - `grpc` (feature-gated)
//!
//! Feature flags:
//! - `grpc`: enables tonic-generated RPC services and client/server glue.
//!
//! End-to-end flow:
//! 1. client submits a serialized physical plan (`SubmitQuery`)
//! 2. coordinator cuts stages, creates task attempts, and serves `GetTask`
//! 3. workers execute stage fragments and report `ReportTaskStatus`
//! 4. map-stage workers publish shuffle metadata (`RegisterMapOutput`)
//! 5. downstream stages read shuffle partitions (`FetchShufflePartition`)
//! 6. sink/final stage publishes query results (`RegisterQueryResults`), client reads via `FetchQueryResults`
//!
//! RPC type definitions live in `proto/ffq_distributed.proto` and are generated
//! under `grpc::v1` when `grpc` feature is enabled.

/// Coordinator state machine and scheduling APIs.
pub mod coordinator;
#[cfg(feature = "grpc")]
/// gRPC services/clients and protobuf-generated bindings.
pub mod grpc;
/// Stage DAG modeling and stage-cut construction.
pub mod stage;
#[cfg(feature = "grpc")]
/// Worker runtime and control-plane adapters.
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
/// Thin facade for stage-DAG construction used by client/runtime integration.
pub struct DistributedRuntime;

impl DistributedRuntime {
    /// Build a stage DAG by cutting the physical plan at shuffle-read boundaries.
    pub fn build_stage_dag(&self, plan: &PhysicalPlan) -> Result<StageDag> {
        Ok(stage::build_stage_dag(plan))
    }
}
