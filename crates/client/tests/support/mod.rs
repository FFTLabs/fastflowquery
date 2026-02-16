#![allow(dead_code)]

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray, UInt32Array, UInt64Array,
};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use ffq_client::Engine;
use ffq_storage::{TableDef, TableStats};
use parquet::arrow::ArrowWriter;

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
