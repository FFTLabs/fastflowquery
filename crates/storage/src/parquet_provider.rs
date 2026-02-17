use std::fs::File;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
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
                Some(existing) => inferred = Some(merge_schemas(existing, &schema, path)?),
            };
        }

        inferred.ok_or_else(|| {
            FfqError::InvalidConfig("failed to infer parquet schema from input paths".to_string())
        })
    }
}

fn merge_schemas(base: &Schema, next: &Schema, path: &str) -> Result<Schema> {
    if base.fields().len() != next.fields().len() {
        return Err(FfqError::InvalidConfig(format!(
            "incompatible parquet schemas across table paths; '{}' has {} fields but expected {}",
            path,
            next.fields().len(),
            base.fields().len()
        )));
    }

    let mut fields = Vec::with_capacity(base.fields().len());
    for (idx, (left, right)) in base.fields().iter().zip(next.fields().iter()).enumerate() {
        fields.push(merge_field(left.as_ref(), right.as_ref(), idx, path)?);
    }

    Ok(Schema::new_with_metadata(fields, base.metadata().clone()))
}

fn merge_field(left: &Field, right: &Field, idx: usize, path: &str) -> Result<Field> {
    if left.name() != right.name() {
        return Err(FfqError::InvalidConfig(format!(
            "incompatible parquet schemas at field {idx}; '{}' has name '{}' but expected '{}' from first schema",
            path,
            right.name(),
            left.name()
        )));
    }

    let dt = merge_data_types(left.data_type(), right.data_type(), left.name(), path)?;
    let nullable = left.is_nullable() || right.is_nullable();
    Ok(Field::new(left.name(), dt, nullable).with_metadata(left.metadata().clone()))
}

fn merge_data_types(left: &DataType, right: &DataType, field: &str, path: &str) -> Result<DataType> {
    if left == right {
        return Ok(left.clone());
    }

    if let Some(widened) = widen_numeric_type(left, right) {
        return Ok(widened);
    }

    Err(FfqError::InvalidConfig(format!(
        "incompatible parquet schemas at field '{field}'; '{}' has type {:?} but expected {:?}",
        path, right, left
    )))
}

fn widen_numeric_type(left: &DataType, right: &DataType) -> Option<DataType> {
    if !(is_numeric_type(left) && is_numeric_type(right)) {
        return None;
    }

    if matches!(left, DataType::Float64) || matches!(right, DataType::Float64) {
        return Some(DataType::Float64);
    }
    if matches!(left, DataType::Float32) || matches!(right, DataType::Float32) {
        return Some(DataType::Float64);
    }

    match (numeric_family(left), numeric_family(right)) {
        (Some(NumericFamily::Signed(a)), Some(NumericFamily::Signed(b))) => {
            Some(signed_type(a.max(b)))
        }
        (Some(NumericFamily::Unsigned(a)), Some(NumericFamily::Unsigned(b))) => {
            Some(unsigned_type(a.max(b)))
        }
        (Some(NumericFamily::Signed(_)), Some(NumericFamily::Unsigned(_)))
        | (Some(NumericFamily::Unsigned(_)), Some(NumericFamily::Signed(_))) => {
            Some(DataType::Float64)
        }
        _ => None,
    }
}

fn is_numeric_type(dt: &DataType) -> bool {
    numeric_family(dt).is_some() || matches!(dt, DataType::Float32 | DataType::Float64)
}

#[derive(Debug, Clone, Copy)]
enum NumericFamily {
    Signed(u8),
    Unsigned(u8),
}

fn numeric_family(dt: &DataType) -> Option<NumericFamily> {
    match dt {
        DataType::Int8 => Some(NumericFamily::Signed(8)),
        DataType::Int16 => Some(NumericFamily::Signed(16)),
        DataType::Int32 => Some(NumericFamily::Signed(32)),
        DataType::Int64 => Some(NumericFamily::Signed(64)),
        DataType::UInt8 => Some(NumericFamily::Unsigned(8)),
        DataType::UInt16 => Some(NumericFamily::Unsigned(16)),
        DataType::UInt32 => Some(NumericFamily::Unsigned(32)),
        DataType::UInt64 => Some(NumericFamily::Unsigned(64)),
        _ => None,
    }
}

fn signed_type(bits: u8) -> DataType {
    match bits {
        0..=8 => DataType::Int8,
        9..=16 => DataType::Int16,
        17..=32 => DataType::Int32,
        _ => DataType::Int64,
    }
}

fn unsigned_type(bits: u8) -> DataType {
    match bits {
        0..=8 => DataType::UInt8,
        9..=16 => DataType::UInt16,
        17..=32 => DataType::UInt32,
        _ => DataType::UInt64,
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
    use std::fs::File;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use arrow::array::ArrayRef;
    use arrow::array::{Float32Array, Int32Array, Int64Array};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::DataType;
    use parquet::arrow::ArrowWriter;

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
        assert!(msg.contains("field 0"));
    }

    #[test]
    fn infer_parquet_schema_widens_numeric_and_nullable() {
        let p1 = unique_path("merge_int32", "parquet");
        let p2 = unique_path("merge_int64_nullable", "parquet");

        write_parquet_file(
            &p1,
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1_i32, 2])) as ArrayRef],
        );
        write_parquet_file(
            &p2,
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![Some(3_i64), None])) as ArrayRef],
        );

        let paths = vec![
            p1.to_string_lossy().to_string(),
            p2.to_string_lossy().to_string(),
        ];
        let schema = ParquetProvider::infer_parquet_schema(&paths).expect("merge schema");
        assert_eq!(schema.field(0).name(), "v");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert!(schema.field(0).is_nullable());

        let _ = std::fs::remove_file(p1);
        let _ = std::fs::remove_file(p2);
    }

    #[test]
    fn infer_parquet_schema_widens_int_and_float_to_float64() {
        let p1 = unique_path("merge_int64", "parquet");
        let p2 = unique_path("merge_float32", "parquet");

        write_parquet_file(
            &p1,
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2])) as ArrayRef],
        );
        write_parquet_file(
            &p2,
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float32, false)])),
            vec![Arc::new(Float32Array::from(vec![1.25_f32, 2.5])) as ArrayRef],
        );

        let paths = vec![
            p1.to_string_lossy().to_string(),
            p2.to_string_lossy().to_string(),
        ];
        let schema = ParquetProvider::infer_parquet_schema(&paths).expect("merge schema");
        assert_eq!(schema.field(0).name(), "v");
        assert_eq!(schema.field(0).data_type(), &DataType::Float64);

        let _ = std::fs::remove_file(p1);
        let _ = std::fs::remove_file(p2);
    }

    fn write_parquet_file(path: &std::path::Path, schema: Arc<Schema>, cols: Vec<ArrayRef>) {
        let batch = RecordBatch::try_new(schema.clone(), cols).expect("build batch");
        let file = File::create(path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }

    fn unique_path(prefix: &str, ext: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("ffq_storage_{prefix}_{nanos}.{ext}"))
    }
}
