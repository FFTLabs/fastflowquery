use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use ffq_client::Engine;
use ffq_common::EngineConfig;
use ffq_storage::TableDef;
use parquet::arrow::ArrowWriter;

fn unique_path(ext: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("ffq_embedded_scan_{nanos}.{ext}"))
}

#[test]
fn register_table_and_scan_parquet_in_embedded_mode() {
    let parquet_path = unique_path("parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .expect("build batch");

    let file = File::create(&parquet_path).expect("create parquet file");
    let mut writer =
        ArrowWriter::try_new(file, schema.clone(), None).expect("create parquet writer");
    writer.write(&batch).expect("write parquet batch");
    writer.close().expect("close parquet writer");

    let engine = Engine::new(EngineConfig::default()).expect("engine");
    engine.register_table(
        "t",
        TableDef {
            name: "ignored".to_string(),
            uri: parquet_path.to_string_lossy().into_owned(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some((*schema).clone()),
            stats: ffq_storage::TableStats::default(),
            options: HashMap::new(),
        },
    );

    let df = engine.table("t").expect("table dataframe");
    let batches = futures::executor::block_on(df.collect()).expect("collect");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3);

    let _ = std::fs::remove_file(parquet_path);
}

#[test]
fn sql_collect_works_when_parquet_schema_is_missing_in_catalog() {
    let parquet_path = unique_path("parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .expect("build batch");

    let file = File::create(&parquet_path).expect("create parquet file");
    let mut writer =
        ArrowWriter::try_new(file, schema.clone(), None).expect("create parquet writer");
    writer.write(&batch).expect("write parquet batch");
    writer.close().expect("close parquet writer");

    let engine = Engine::new(EngineConfig::default()).expect("engine");
    engine.register_table(
        "t",
        TableDef {
            name: "ignored".to_string(),
            uri: parquet_path.to_string_lossy().into_owned(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: None,
            stats: ffq_storage::TableStats::default(),
            options: HashMap::new(),
        },
    );

    let batches = futures::executor::block_on(
        engine
            .sql("SELECT id, name FROM t")
            .expect("sql")
            .collect(),
    )
    .expect("collect");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3);

    let inferred = engine.table_schema("t").expect("table schema");
    assert!(inferred.is_some(), "expected inferred schema to be cached");

    let _ = std::fs::remove_file(parquet_path);
}
