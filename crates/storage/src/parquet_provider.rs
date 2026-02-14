use std::fs::File;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use ffq_common::{FfqError, Result};
use ffq_execution::{ExecNode, SendableRecordBatchStream, StreamAdapter, TaskContext};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::catalog::TableDef;
use crate::provider::{Stats, StorageExecNode, StorageProvider};

pub struct ParquetProvider;

impl ParquetProvider {
    pub fn new() -> Self {
        Self
    }
}

impl StorageProvider for ParquetProvider {
    fn estimate_stats(&self, table: &TableDef) -> Stats {
        Stats {
            estimated_rows: table.stats.rows,
            estimated_bytes: table.stats.bytes,
        }
    }

    fn scan(
        &self,
        table: &TableDef,
        projection: Option<Vec<String>>,
        filters: Vec<String>,
    ) -> Result<StorageExecNode> {
        if table.format.to_lowercase() != "parquet" {
            return Err(FfqError::Unsupported(format!(
                "format not supported by ParquetProvider: {}",
                table.format
            )));
        }

        Ok(Arc::new(ParquetScanNode {
            paths: table.data_paths()?,
            schema: table
                .schema
                .clone()
                .map(Arc::new)
                .unwrap_or_else(|| Arc::new(Schema::empty())),
            projection,
            filters,
        }))
    }
}

pub struct ParquetScanNode {
    paths: Vec<String>,
    schema: SchemaRef,
    #[allow(dead_code)]
    projection: Option<Vec<String>>,
    #[allow(dead_code)]
    filters: Vec<String>,
}

impl ExecNode for ParquetScanNode {
    fn name(&self) -> &'static str {
        "ParquetScanNode"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        // v1 embedded path: read local parquet files eagerly and stream batches.
        let mut out = Vec::<Result<RecordBatch>>::new();
        for path in &self.paths {
            let file = File::open(path)?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| FfqError::Execution(format!("parquet reader build failed: {e}")))?
                .build()
                .map_err(|e| FfqError::Execution(format!("parquet reader open failed: {e}")))?;

            for batch in reader {
                out.push(
                    batch.map_err(|e| FfqError::Execution(format!("parquet decode failed: {e}"))),
                );
            }
        }

        Ok(Box::pin(StreamAdapter::new(
            self.schema.clone(),
            futures::stream::iter(out),
        )))
    }
}
