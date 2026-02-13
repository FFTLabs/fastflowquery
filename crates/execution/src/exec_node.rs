use std::sync::Arc;

use arrow_schema::SchemaRef;
use ffq_common::Result;

use crate::context::TaskContext;
use crate::stream::SendableRecordBatchStream;

/// A physical operator instance that can produce RecordBatches.
/// Operators are pull-based (consumer polls the stream),
/// but can also use bounded channels internally for push-based parts (shuffle, etc.).
pub trait ExecNode: Send + Sync {
    fn name(&self) -> &'static str;

    fn schema(&self) -> SchemaRef;

    fn execute(&self, ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream>;
}
