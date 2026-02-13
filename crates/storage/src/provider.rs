use arrow::record_batch::RecordBatch;
use ffq_common::Result;
use futures::Stream;
use std::pin::Pin;

pub type RecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;

#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub estimated_rows: Option<u64>,
    pub estimated_bytes: Option<u64>,
}

pub trait StorageProvider: Send + Sync {
    fn estimate_stats(&self, table: &crate::catalog::TableDef) -> Result<Stats>;

    fn scan(
        &self,
        table: &crate::catalog::TableDef,
        projection: Option<Vec<String>>,
        filters: Vec<String>,
    ) -> Result<RecordBatchStream>;
}
