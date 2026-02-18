use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TaskContext {
    /// Target batch size for operators that coalesce/split.
    pub batch_size_rows: usize,

    /// Soft memory budget for spill decisions later (v1+).
    pub mem_budget_bytes: usize,
}

pub type SharedTaskContext = Arc<TaskContext>;
