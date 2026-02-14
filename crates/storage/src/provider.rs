use std::sync::Arc;

use ffq_common::Result;
use ffq_execution::ExecNode;

#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub estimated_rows: Option<u64>,
    pub estimated_bytes: Option<u64>,
}

pub type StorageExecNode = Arc<dyn ExecNode>;

pub trait StorageProvider: Send + Sync {
    fn estimate_stats(&self, table: &crate::catalog::TableDef) -> Stats;

    fn scan(
        &self,
        table: &crate::catalog::TableDef,
        projection: Option<Vec<String>>,
        filters: Vec<String>,
    ) -> Result<StorageExecNode>;
}
