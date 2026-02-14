use std::fs::File;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use ffq_common::Result;
use ffq_execution::{empty_stream, ExecNode, SendableRecordBatchStream, TaskContext};

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
        Ok(Arc::new(ParquetScanNode {
            uri: table.uri.clone(),
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
    uri: String,
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
        // v1: validate local path and return an empty stream placeholder.
        // Actual parquet decoding is implemented in a later ticket.
        let _file = File::open(&self.uri)?;
        Ok(empty_stream(self.schema.clone()))
    }
}
