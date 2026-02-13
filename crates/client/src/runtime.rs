use std::fmt::Debug;
use arrow_schema::Schema;
use std::sync::Arc;

use ffq_common::Result;
use ffq_execution::stream::{empty_stream, SendableRecordBatchStream};
use ffq_planner::PhysicalPlan;
use futures::future::BoxFuture;
use futures::FutureExt;

#[derive(Debug, Clone)]
pub struct QueryContext {
    pub batch_size_rows: usize,
    // later: query_id, stage_id, task_id, mem budget, etc.
}

/// Runtime = something that can execute a PhysicalPlan and return a stream of RecordBatches.
pub trait Runtime: Send + Sync + Debug {
    fn execute(&self, plan: PhysicalPlan, ctx: QueryContext)
        -> BoxFuture<'static, Result<SendableRecordBatchStream>>;

    fn shutdown(&self) -> BoxFuture<'static, Result<()>> {
        async { Ok(()) }.boxed()
    }
}

#[derive(Debug, Default)]
pub struct EmbeddedRuntime;

impl EmbeddedRuntime {
    pub fn new() -> Self {
        Self
    }
}

impl Runtime for EmbeddedRuntime {
    fn execute(
        &self,
        _plan: PhysicalPlan,
        _ctx: QueryContext,
    ) -> BoxFuture<'static, Result<SendableRecordBatchStream>> {
        async move {
            let schema = Arc::new(Schema::empty());
            Ok(empty_stream(schema))
        }
        .boxed()
    }
}

#[cfg(feature = "distributed")]
use ffq_distributed::DistributedRuntime as InnerDistributedRuntime;

#[cfg(feature = "distributed")]
#[derive(Debug, Default)]
pub struct DistributedRuntime {
    _inner: InnerDistributedRuntime,
}

#[cfg(feature = "distributed")]
impl DistributedRuntime {
    pub fn new() -> Self {
        Self {
            _inner: InnerDistributedRuntime::default(),
        }
    }
}

#[cfg(feature = "distributed")]
impl Runtime for DistributedRuntime {
    fn execute(
        &self,
        _plan: PhysicalPlan,
        _ctx: QueryContext,
    ) -> BoxFuture<'static, Result<SendableRecordBatchStream>> {
        async move {
            // v1 skeleton: distributed implementation will:
            // - serialize plan
            // - submit to coordinator
            // - stream results back
            let stream = futures::stream::empty::<Result<arrow::record_batch::RecordBatch>>().boxed();
            Ok(stream)
        }
        .boxed()
    }
}
