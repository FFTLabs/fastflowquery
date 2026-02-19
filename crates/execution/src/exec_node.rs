//! Physical operator execution-node contract.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use ffq_common::Result;

use crate::context::TaskContext;
use crate::stream::SendableRecordBatchStream;

/// A physical operator instance that can produce RecordBatches.
/// Operators are pull-based (consumer polls the stream),
/// but can also use bounded channels internally for push-based parts (shuffle, etc.).
pub trait ExecNode: Send + Sync {
    /// Stable operator name for explain/logging.
    fn name(&self) -> &'static str;

    /// Output schema for all batches emitted by this node.
    fn schema(&self) -> SchemaRef;

    /// Start execution and return a stream of output batches.
    ///
    /// Implementations should surface deterministic operator failures as
    /// `FfqError::Execution` and configuration/state failures via appropriate
    /// error variants.
    fn execute(&self, ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream>;
}
