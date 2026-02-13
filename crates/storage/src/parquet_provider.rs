use crate::catalog::TableDef;
use crate::provider::{RecordBatchStream, Stats, StorageProvider};
use arrow::record_batch::RecordBatch;
use ffq_common::{FfqError, Result};
use std::fs::File;
use std::pin::Pin;

pub struct ParquetProvider;

impl ParquetProvider {
    pub fn new() -> Self {
        Self
    }
}

impl StorageProvider for ParquetProvider {
    fn estimate_stats(&self, _table: &TableDef) -> Result<Stats> {
        Ok(Stats::default())
    }

    fn scan(
        &self,
        table: &TableDef,
        _projection: Option<Vec<String>>,
        _filters: Vec<String>,
    ) -> Result<RecordBatchStream> {
        if table.format.to_lowercase() != "parquet" {
            return Err(FfqError::Unsupported(format!(
                "format not supported: {}",
                table.format
            )));
        }

        let _f = File::open(&table.uri)?;
        let stream = futures::stream::empty::<Result<RecordBatch>>();
        Ok(Box::pin(stream) as Pin<Box<dyn futures::Stream<Item = Result<RecordBatch>> + Send>>)
    }
}
