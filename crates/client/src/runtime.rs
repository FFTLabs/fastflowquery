use std::fmt::Debug;
use std::sync::Arc;

use ffq_common::{FfqError, Result};
use ffq_execution::{SendableRecordBatchStream, TaskContext};
use ffq_planner::PhysicalPlan;
use ffq_storage::parquet_provider::ParquetProvider;
use ffq_storage::{Catalog, StorageProvider};
use futures::future::BoxFuture;
use futures::FutureExt;

#[derive(Debug, Clone)]
pub struct QueryContext {
    pub batch_size_rows: usize,
    // later: query_id, stage_id, task_id, mem budget, etc.
}

/// Runtime = something that can execute a PhysicalPlan and return a stream of RecordBatches.
pub trait Runtime: Send + Sync + Debug {
    fn execute(
        &self,
        plan: PhysicalPlan,
        ctx: QueryContext,
        catalog: Arc<Catalog>,
    ) -> BoxFuture<'static, Result<SendableRecordBatchStream>>;

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
        plan: PhysicalPlan,
        ctx: QueryContext,
        catalog: Arc<Catalog>,
    ) -> BoxFuture<'static, Result<SendableRecordBatchStream>> {
        async move {
            match plan {
                PhysicalPlan::ParquetScan(scan) => {
                    let table = catalog.get(&scan.table)?.clone();
                    let provider = ParquetProvider::new();
                    let node = provider.scan(
                        &table,
                        scan.projection,
                        scan.filters.into_iter().map(|f| format!("{f:?}")).collect(),
                    )?;
                    node.execute(Arc::new(TaskContext {
                        batch_size_rows: ctx.batch_size_rows,
                        mem_budget_bytes: 0,
                    }))
                }
                _ => Err(FfqError::Unsupported(
                    "embedded runtime currently supports only ParquetScan execution".to_string(),
                )),
            }
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
        _catalog: Arc<Catalog>,
    ) -> BoxFuture<'static, Result<SendableRecordBatchStream>> {
        async move {
            // v1 skeleton: distributed implementation will:
            // - serialize plan
            // - submit to coordinator
            // - stream results back
            let stream =
                futures::stream::empty::<Result<arrow::record_batch::RecordBatch>>().boxed();
            Ok(stream)
        }
        .boxed()
    }
}
