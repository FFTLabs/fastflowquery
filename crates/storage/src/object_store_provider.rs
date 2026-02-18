use ffq_common::{FfqError, Result};

use crate::catalog::TableDef;
use crate::provider::{Stats, StorageExecNode, StorageProvider};

/// Experimental placeholder for object-store backed scans (S3/GCS/Azure).
pub struct ObjectStoreProvider;

impl ObjectStoreProvider {
    pub fn new() -> Self {
        Self
    }
}

impl StorageProvider for ObjectStoreProvider {
    fn estimate_stats(&self, table: &TableDef) -> Stats {
        Stats {
            estimated_rows: table.stats.rows,
            estimated_bytes: table.stats.bytes,
        }
    }

    fn scan(
        &self,
        table: &TableDef,
        _projection: Option<Vec<String>>,
        _filters: Vec<String>,
    ) -> Result<StorageExecNode> {
        Err(FfqError::Unsupported(format!(
            "object-store scan is experimental and not implemented yet for '{}'",
            table.name
        )))
    }
}
