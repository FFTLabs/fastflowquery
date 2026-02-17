use std::fs::File;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
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

    pub fn infer_parquet_schema(paths: &[String]) -> Result<arrow_schema::Schema> {
        if paths.is_empty() {
            return Err(FfqError::InvalidConfig(
                "cannot infer parquet schema from empty path list".to_string(),
            ));
        }

        let mut inferred: Option<arrow_schema::Schema> = None;
        for path in paths {
            let file = File::open(path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
                FfqError::Execution(format!(
                    "parquet schema inference reader build failed for '{path}': {e}"
                ))
            })?;
            let schema = builder.schema().as_ref().clone();

            match &inferred {
                None => inferred = Some(schema),
                Some(existing) if existing == &schema => {}
                Some(existing) => {
                    return Err(FfqError::InvalidConfig(format!(
                        "incompatible parquet schemas across table paths; '{}' does not match first schema (first={:?}, current={:?})",
                        path,
                        existing,
                        schema
                    )));
                }
            }
        }

        inferred.ok_or_else(|| {
            FfqError::InvalidConfig("failed to infer parquet schema from input paths".to_string())
        })
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

        let paths = table.data_paths()?;
        let schema = match &table.schema {
            Some(s) => Arc::new(s.clone()),
            None => Arc::new(Self::infer_parquet_schema(&paths)?),
        };

        Ok(Arc::new(ParquetScanNode {
            paths,
            schema,
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use arrow_schema::DataType;

    use super::*;
    use crate::TableStats;

    fn fixture_path(file: &str) -> String {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        root.join("../../tests/fixtures/parquet")
            .join(file)
            .to_string_lossy()
            .to_string()
    }

    #[test]
    fn infer_parquet_schema_from_fixture_file() {
        let paths = vec![fixture_path("lineitem.parquet")];
        let schema = ParquetProvider::infer_parquet_schema(&paths).expect("infer schema");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "l_orderkey");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).name(), "l_partkey");
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
    }

    #[test]
    fn parquet_scan_infers_schema_when_missing_in_catalog() {
        let provider = ParquetProvider::new();
        let table = TableDef {
            name: "lineitem".to_string(),
            uri: fixture_path("lineitem.parquet"),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: None,
            stats: TableStats::default(),
            options: HashMap::new(),
        };
        let node = provider
            .scan(&table, None, Vec::new())
            .expect("scan should infer schema");
        let schema = node.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "l_orderkey");
    }

    #[test]
    fn infer_parquet_schema_rejects_incompatible_files() {
        let paths = vec![fixture_path("lineitem.parquet"), fixture_path("orders.parquet")];
        let err = ParquetProvider::infer_parquet_schema(&paths).expect_err("must reject");
        let msg = format!("{err}");
        assert!(msg.contains("incompatible parquet schemas"));
    }
}
