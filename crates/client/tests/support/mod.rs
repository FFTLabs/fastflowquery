#![allow(dead_code)]

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeListBuilder, Float32Array, Float32Builder,
    Float64Array, Int32Array, Int64Array, StringArray, UInt32Array, UInt64Array,
};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use ffq_client::Engine;
use ffq_storage::{TableDef, TableStats};
use parquet::arrow::ArrowWriter;

pub mod integration_queries {
    const SCAN_FILTER_PROJECT_SQL: &str =
        include_str!("../../../../tests/integration/queries/scan_filter_project.sql");
    const JOIN_PROJECTION_SQL: &str =
        include_str!("../../../../tests/integration/queries/join_projection.sql");
    const JOIN_AGGREGATE_SQL: &str =
        include_str!("../../../../tests/integration/queries/join_aggregate.sql");
    const VECTOR_TOPK_COSINE_SQL: &str =
        include_str!("../../../../tests/integration/queries/vector_topk_cosine.sql");
    const VECTOR_TWO_PHASE_SQL: &str =
        include_str!("../../../../tests/integration/queries/vector_two_phase_rerank.sql");

    pub fn scan_filter_project() -> &'static str {
        SCAN_FILTER_PROJECT_SQL.trim()
    }

    pub fn join_projection() -> &'static str {
        JOIN_PROJECTION_SQL.trim()
    }

    pub fn join_aggregate() -> &'static str {
        JOIN_AGGREGATE_SQL.trim()
    }

    pub fn vector_topk_cosine() -> &'static str {
        VECTOR_TOPK_COSINE_SQL.trim()
    }

    pub fn vector_two_phase_rerank() -> &'static str {
        VECTOR_TWO_PHASE_SQL.trim()
    }
}

#[derive(Debug, Clone)]
pub struct IntegrationParquetFixtures {
    pub lineitem: PathBuf,
    pub orders: PathBuf,
    pub docs: PathBuf,
}

pub fn ensure_integration_parquet_fixtures() -> IntegrationParquetFixtures {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/parquet");
    std::fs::create_dir_all(&root).expect("create integration fixture dir");

    let lineitem = root.join("lineitem.parquet");
    let orders = root.join("orders.parquet");
    let docs = root.join("docs.parquet");

    let lineitem_schema = Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
    ]));
    write_parquet(
        &lineitem,
        lineitem_schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 2, 3, 3, 3])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 21, 30, 31, 32])),
        ],
    );

    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
    ]));
    write_parquet(
        &orders,
        orders_schema,
        vec![
            Arc::new(Int64Array::from(vec![2_i64, 3, 4])),
            Arc::new(Int64Array::from(vec![100_i64, 200, 300])),
        ],
    );

    let emb_field = Field::new("item", DataType::Float32, true);
    let docs_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("lang", DataType::Utf8, false),
        Field::new("emb", DataType::FixedSizeList(Arc::new(emb_field), 3), true),
    ]));
    let mut emb_builder = FixedSizeListBuilder::new(Float32Builder::new(), 3);
    let vectors = [
        [1.0_f32, 0.0, 0.0],
        [0.8_f32, 0.2, 0.0],
        [0.0_f32, 1.0, 0.0],
    ];
    for v in vectors {
        for x in v {
            emb_builder.values().append_value(x);
        }
        emb_builder.append(true);
    }
    write_parquet(
        &docs,
        docs_schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(StringArray::from(vec!["doc-1", "doc-2", "doc-3"])),
            Arc::new(StringArray::from(vec!["en", "en", "de"])),
            Arc::new(emb_builder.finish()),
        ],
    );

    IntegrationParquetFixtures {
        lineitem,
        orders,
        docs,
    }
}

pub fn unique_path(prefix: &str, ext: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}_{nanos}.{ext}"))
}

pub fn write_parquet(path: &Path, schema: Arc<Schema>, cols: Vec<ArrayRef>) {
    let batch = RecordBatch::try_new(schema.clone(), cols).expect("build batch");
    let file = File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");
}

pub fn register_parquet_table(
    engine: &Engine,
    name: &str,
    path: &Path,
    schema: Schema,
    stats: TableStats,
) {
    engine.register_table(
        name,
        TableDef {
            name: name.to_string(),
            uri: path.to_string_lossy().into_owned(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some(schema),
            stats,
            options: HashMap::new(),
        },
    );
}

pub fn assert_schema_eq(actual: &Schema, expected: &Schema) {
    let actual_s = schema_text(actual);
    let expected_s = schema_text(expected);
    assert_eq!(
        actual_s, expected_s,
        "schema mismatch\nactual:   {actual_s}\nexpected: {expected_s}"
    );
}

pub fn assert_batches_deterministic(
    first: &[RecordBatch],
    second: &[RecordBatch],
    sort_by: &[&str],
    float_tolerance: f64,
) {
    let a = snapshot_text(first, sort_by, float_tolerance);
    let b = snapshot_text(second, sort_by, float_tolerance);
    assert_eq!(a, b, "normalized batch snapshots differ between runs");
}

pub fn snapshot_text(batches: &[RecordBatch], sort_by: &[&str], float_tolerance: f64) -> String {
    if batches.is_empty() {
        return "schema:<empty>\nrows:\n".to_string();
    }
    let schema = batches[0].schema();
    let mut rows = Vec::new();
    for batch in batches {
        assert_eq!(
            schema_text(batch.schema().as_ref()),
            schema_text(schema.as_ref()),
            "all batches must share same schema for snapshot"
        );
        for row in 0..batch.num_rows() {
            rows.push(extract_row(batch, row, float_tolerance));
        }
    }
    rows.sort_by(|a, b| {
        row_sort_key(a, sort_by)
            .cmp(&row_sort_key(b, sort_by))
            .then_with(|| row_render(a).cmp(&row_render(b)))
    });

    let mut out = String::new();
    out.push_str("schema:");
    out.push_str(&schema_text(schema.as_ref()));
    out.push('\n');
    out.push_str("rows:\n");
    for row in rows {
        out.push_str(&row_render(&row));
        out.push('\n');
    }
    out
}

pub fn assert_or_bless_snapshot(rel_path_from_client_crate: &str, actual: &str) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(rel_path_from_client_crate);
    if should_bless() {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create snapshot parent");
        }
        std::fs::write(&path, actual).expect("write snapshot");
        return;
    }
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!(
            "missing snapshot at {}. Run tests with BLESS=1.",
            path.display()
        )
    });
    if expected != actual {
        panic!(
            "snapshot mismatch at {}\n\n{}\n\nRun with BLESS=1 to accept changes.",
            path.display(),
            unified_diff(&expected, actual)
        );
    }
}

fn schema_text(schema: &Schema) -> String {
    schema
        .fields()
        .iter()
        .map(|f| field_text(f.as_ref()))
        .collect::<Vec<_>>()
        .join(",")
}

fn field_text(field: &Field) -> String {
    format!("{}:{:?}:{}", field.name(), field.data_type(), field.is_nullable())
}

fn extract_row(batch: &RecordBatch, row: usize, float_tolerance: f64) -> Vec<(String, String)> {
    let schema = batch.schema();
    (0..batch.num_columns())
        .map(|col_idx| {
            let name = schema.field(col_idx).name().clone();
            let value = value_text(
                batch.column(col_idx),
                row,
                schema.field(col_idx).data_type(),
                float_tolerance,
            );
            (name, value)
        })
        .collect()
}

fn row_sort_key(row: &[(String, String)], sort_by: &[&str]) -> String {
    if sort_by.is_empty() {
        return row_render(row);
    }
    let mut keys = Vec::with_capacity(sort_by.len());
    for col in sort_by {
        let value = row
            .iter()
            .find(|(name, _)| name == col)
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| "<missing>".to_string());
        keys.push(format!("{col}={value}"));
    }
    keys.join("|")
}

fn row_render(row: &[(String, String)]) -> String {
    row.iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("|")
}

fn value_text(arr: &ArrayRef, row: usize, dt: &DataType, float_tolerance: f64) -> String {
    if arr.is_null(row) {
        return "NULL".to_string();
    }

    match dt {
        DataType::Int64 => arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array")
            .value(row)
            .to_string(),
        DataType::Int32 => arr
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32Array")
            .value(row)
            .to_string(),
        DataType::UInt64 => arr
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("UInt64Array")
            .value(row)
            .to_string(),
        DataType::UInt32 => arr
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("UInt32Array")
            .value(row)
            .to_string(),
        DataType::Float64 => {
            let v = arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("Float64Array")
                .value(row);
            format_float(v, float_tolerance)
        }
        DataType::Float32 => {
            let v = arr
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("Float32Array")
                .value(row) as f64;
            format_float(v, float_tolerance)
        }
        DataType::Boolean => arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("BooleanArray")
            .value(row)
            .to_string(),
        DataType::Utf8 => arr
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray")
            .value(row)
            .to_string(),
        other => panic!("snapshot formatter does not support type: {other:?}"),
    }
}

fn format_float(v: f64, tol: f64) -> String {
    if tol <= 0.0 {
        return format!("{v:.12}");
    }
    let quantized = (v / tol).round() * tol;
    format!("{quantized:.12}")
}

fn should_bless() -> bool {
    matches!(std::env::var("BLESS").as_deref(), Ok("1"))
        || matches!(std::env::var("UPDATE_SNAPSHOTS").as_deref(), Ok("1"))
}

fn unified_diff(expected: &str, actual: &str) -> String {
    let exp: Vec<&str> = expected.lines().collect();
    let act: Vec<&str> = actual.lines().collect();
    let mut out = String::new();
    out.push_str("--- expected\n+++ actual\n");
    let max = exp.len().max(act.len());
    for i in 0..max {
        match (exp.get(i), act.get(i)) {
            (Some(e), Some(a)) if e == a => out.push_str(&format!(" {:04} {e}\n", i + 1)),
            (Some(e), Some(a)) => {
                out.push_str(&format!("-{:04} {e}\n", i + 1));
                out.push_str(&format!("+{:04} {a}\n", i + 1));
            }
            (Some(e), None) => out.push_str(&format!("-{:04} {e}\n", i + 1)),
            (None, Some(a)) => out.push_str(&format!("+{:04} {a}\n", i + 1)),
            (None, None) => {}
        }
    }
    out
}
