use std::sync::Arc;

use ffq_common::Result;
use ffq_execution::ExecNode;

/// Lightweight statistics used by planner/optimizer.
#[derive(Debug, Clone, Default)]
pub struct Stats {
    /// Estimated output row count, if known.
    pub estimated_rows: Option<u64>,
    /// Estimated scanned bytes, if known.
    pub estimated_bytes: Option<u64>,
}

/// Type-erased storage execution node.
pub type StorageExecNode = Arc<dyn ExecNode>;

/// Storage abstraction for table scanning and basic stats estimation.
///
/// Implementations are format/backend-specific (for example parquet, object-store, qdrant).
pub trait StorageProvider: Send + Sync {
    /// Estimates table-level stats for optimizer decisions.
    fn estimate_stats(&self, table: &crate::catalog::TableDef) -> Stats;

    /// Builds scan node for a table with optional projection/filter pushdown hints.
    ///
    /// `projection` and `filters` are best-effort pushdown inputs; providers may partially
    /// apply or ignore unsupported predicates.
    ///
    /// # Errors
    /// Returns an error for invalid table configuration or unsupported format/provider behavior.
    fn scan(
        &self,
        table: &crate::catalog::TableDef,
        projection: Option<Vec<String>>,
        filters: Vec<String>,
    ) -> Result<StorageExecNode>;
}
