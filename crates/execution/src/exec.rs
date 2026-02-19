//! Legacy execution-plan trait used by early v1 scaffolding.
//!
//! New operator code should generally use [`crate::exec_node::ExecNode`].

use arrow::record_batch::RecordBatch;
use ffq_common::Result;
use futures::StreamExt;
use futures::stream::BoxStream;

/// Boxed stream used by [`ExecutionPlan`].
pub type SendableRecordBatchStream = BoxStream<'static, Result<RecordBatch>>;

#[derive(Debug, Clone)]
/// Minimal execution context for legacy `ExecutionPlan`.
pub struct ExecContext {
    pub batch_size_rows: usize,
}

/// Legacy plan trait.
pub trait ExecutionPlan: Send + Sync {
    fn name(&self) -> &'static str;
    fn execute(&self, ctx: ExecContext) -> SendableRecordBatchStream;
}

pub struct EmptyExec;

impl ExecutionPlan for EmptyExec {
    fn name(&self) -> &'static str {
        "EmptyExec"
    }

    fn execute(&self, _ctx: ExecContext) -> SendableRecordBatchStream {
        futures::stream::empty().boxed()
    }
}
