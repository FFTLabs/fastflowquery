use arrow::record_batch::RecordBatch;
use ffq_common::Result;
use futures::stream::BoxStream;
use futures::StreamExt;

pub type SendableRecordBatchStream = BoxStream<'static, Result<RecordBatch>>;

#[derive(Debug, Clone)]
pub struct ExecContext {
    pub batch_size_rows: usize,
}

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
