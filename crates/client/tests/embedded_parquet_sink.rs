use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use ffq_client::Engine;
use ffq_common::EngineConfig;
use ffq_storage::TableDef;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;

fn unique_path(prefix: &str, ext: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}_{nanos}.{ext}"))
}

#[test]
fn insert_into_select_writes_parquet_sink() {
    let src_path = unique_path("ffq_sink_src", "parquet");
    let out_dir = unique_path("ffq_sink_out", "dir");
    let out_file = out_dir.join("part-00000.parquet");

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 30])),
        ],
    )
    .expect("batch");
    let file = File::create(&src_path).expect("create src parquet");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");

    let engine = Engine::new(EngineConfig::default()).expect("engine");
    engine.register_table(
        "src",
        TableDef {
            name: "src".to_string(),
            uri: src_path.to_string_lossy().to_string(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some((*schema).clone()),
            stats: ffq_storage::TableStats::default(),
            options: HashMap::new(),
        },
    );
    engine.register_table(
        "dst",
        TableDef {
            name: "dst".to_string(),
            uri: out_dir.to_string_lossy().to_string(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some((*schema).clone()),
            stats: ffq_storage::TableStats::default(),
            options: HashMap::new(),
        },
    );

    let _ = futures::executor::block_on(
        engine
            .sql("INSERT INTO dst SELECT a, b FROM src")
            .expect("insert sql")
            .collect(),
    )
    .expect("execute insert");

    assert!(
        out_file.exists(),
        "expected sink file at {}",
        out_file.display()
    );
    let out_f = File::open(&out_file).expect("open sink file");
    let reader = ParquetRecordBatchReaderBuilder::try_new(out_f)
        .expect("reader build")
        .build()
        .expect("reader");
    let mut rows = 0_usize;
    for b in reader {
        let b = b.expect("decode");
        rows += b.num_rows();
    }
    assert_eq!(rows, 3);

    let _ = std::fs::remove_file(src_path);
    let _ = std::fs::remove_file(out_file);
    let _ = std::fs::remove_dir_all(out_dir);
}
