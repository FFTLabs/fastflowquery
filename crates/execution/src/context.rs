//! Execution task context shared by physical operators.

use std::sync::Arc;

#[derive(Debug, Clone)]
/// Runtime-level limits and sizing hints for one task.
pub struct TaskContext {
    /// Target batch size for operators that coalesce/split.
    pub batch_size_rows: usize,

    /// Soft memory budget for spill decisions later (v1+).
    pub mem_budget_bytes: usize,
}

/// Shared task context handle passed across operator boundaries.
pub type SharedTaskContext = Arc<TaskContext>;
