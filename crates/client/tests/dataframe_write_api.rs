use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use ffq_client::{Engine, WriteMode};
use ffq_common::EngineConfig;
use ffq_storage::TableDef;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn unique_path(prefix: &str, ext: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}_{nanos}.{ext}"))
}

fn catalog_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

fn engine_with_catalog_path(catalog_path: &std::path::Path) -> Engine {
    let mut cfg = EngineConfig::default();
    cfg.catalog_path = Some(catalog_path.to_string_lossy().to_string());
    Engine::new(cfg).expect("engine")
}

fn write_src_parquet(path: &std::path::Path, schema: Arc<Schema>) {
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 30])),
        ],
    )
    .expect("batch");
    let file = File::create(path).expect("create src parquet");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");
}

fn parquet_rows(path: &std::path::Path) -> usize {
    let file = File::open(path).expect("open parquet");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("reader build")
        .build()
        .expect("reader");
    reader.map(|b| b.expect("decode").num_rows()).sum()
}

#[test]
fn dataframe_write_parquet_supports_overwrite_and_append() {
    let src_path = unique_path("ffq_df_write_src", "parquet");
    let out_dir = unique_path("ffq_df_write_out", "dir");
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    write_src_parquet(&src_path, schema.clone());

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

    let df = engine.table("src").expect("src df");
    futures::executor::block_on(df.write_parquet(&out_dir)).expect("overwrite write");
    let p0 = out_dir.join("part-00000.parquet");
    assert!(p0.exists());
    assert_eq!(parquet_rows(&p0), 3);

    futures::executor::block_on(df.write_parquet_with_mode(&out_dir, WriteMode::Append))
        .expect("append write");
    let p1 = out_dir.join("part-00001.parquet");
    assert!(p1.exists());
    assert_eq!(parquet_rows(&p1), 3);

    let _ = std::fs::remove_file(src_path);
    let _ = std::fs::remove_dir_all(out_dir);
}

#[test]
fn dataframe_save_as_table_updates_catalog_and_is_queryable_immediately() {
    let _guard = catalog_lock();
    let src_path = unique_path("ffq_df_save_src", "parquet");
    let catalog_path = unique_path("ffq_catalog", "json");
    let table_name = format!(
        "saved_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    );
    let table_dir = std::path::PathBuf::from("./ffq_tables").join(&table_name);
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    write_src_parquet(&src_path, schema.clone());

    let engine = engine_with_catalog_path(&catalog_path);
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

    let df = engine.table("src").expect("src");
    futures::executor::block_on(df.save_as_table(&table_name)).expect("save overwrite");

    let batches =
        futures::executor::block_on(engine.table(&table_name).expect("saved table").collect())
            .expect("collect saved table");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3);

    futures::executor::block_on(df.save_as_table_with_mode(&table_name, WriteMode::Append))
        .expect("save append");
    let batches2 =
        futures::executor::block_on(engine.table(&table_name).expect("saved table").collect())
            .expect("collect saved table");
    let rows2: usize = batches2.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows2, 6);

    let _ = std::fs::remove_file(catalog_path);
    let _ = std::fs::remove_file(src_path);
    let _ = std::fs::remove_dir_all(table_dir);
}

#[test]
fn save_as_table_persists_across_engine_restart() {
    let _guard = catalog_lock();
    let src_path = unique_path("ffq_df_restart_src", "parquet");
    let catalog_path = unique_path("ffq_catalog_restart", "json");

    let table_name = format!(
        "restart_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    );
    let table_dir = catalog_path
        .parent()
        .expect("catalog parent")
        .join(&table_name);
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    write_src_parquet(&src_path, schema.clone());

    let engine = engine_with_catalog_path(&catalog_path);
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
    futures::executor::block_on(engine.table("src").expect("src").save_as_table(&table_name))
        .expect("save table");
    drop(engine);

    let restarted = engine_with_catalog_path(&catalog_path);
    let rows: usize = futures::executor::block_on(
        restarted
            .table(&table_name)
            .expect("persisted table")
            .collect(),
    )
    .expect("collect persisted table")
    .iter()
    .map(|b| b.num_rows())
    .sum();
    assert_eq!(rows, 3);

    let _ = std::fs::remove_file(catalog_path);
    let _ = std::fs::remove_file(src_path);
    let _ = std::fs::remove_dir_all(table_dir);
}

#[test]
fn failed_save_as_table_leaves_no_catalog_entry_or_partial_data() {
    let _guard = catalog_lock();
    let src_path = unique_path("ffq_df_fail_src", "parquet");
    let catalog_path = unique_path("ffq_catalog_fail", "json");
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    write_src_parquet(&src_path, schema.clone());

    let engine = engine_with_catalog_path(&catalog_path);
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

    let blocked_base = catalog_path
        .parent()
        .expect("catalog parent")
        .join("blocked");
    std::fs::write(&blocked_base, b"not a directory").expect("write blocker file");
    let result = futures::executor::block_on(
        engine
            .table("src")
            .expect("src")
            .save_as_table("blocked/table"),
    );
    assert!(result.is_err(), "expected write failure");
    let query_failed =
        futures::executor::block_on(engine.table("blocked/table").expect("df").collect()).is_err();
    assert!(query_failed, "failed table write must not register table");
    let blocked_table_path = blocked_base.join("table");
    assert!(
        !blocked_table_path.exists(),
        "failed table write should not leave committed data"
    );

    let _ = std::fs::remove_file(catalog_path);
    let _ = std::fs::remove_file(src_path);
    let _ = std::fs::remove_file(blocked_base);
}

#[test]
fn overwrite_retries_are_deterministic() {
    let _guard = catalog_lock();
    let src_path = unique_path("ffq_df_retry_src", "parquet");
    let catalog_path = unique_path("ffq_catalog_retry", "json");
    let table_name = format!(
        "retry_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    );
    let table_dir = catalog_path
        .parent()
        .expect("catalog parent")
        .join(&table_name);
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    write_src_parquet(&src_path, schema.clone());

    let engine = engine_with_catalog_path(&catalog_path);
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

    let df = engine.table("src").expect("src");
    futures::executor::block_on(df.save_as_table_with_mode(&table_name, WriteMode::Overwrite))
        .expect("overwrite one");
    futures::executor::block_on(df.save_as_table_with_mode(&table_name, WriteMode::Overwrite))
        .expect("overwrite retry");

    let p0 = table_dir.join("part-00000.parquet");
    assert!(p0.exists());
    assert_eq!(parquet_rows(&p0), 3);
    let part_count = std::fs::read_dir(&table_dir)
        .expect("read table dir")
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path().is_file()
                && e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("part-") && n.ends_with(".parquet"))
        })
        .count();
    assert_eq!(part_count, 1, "overwrite retries must remain deterministic");

    let _ = std::fs::remove_file(catalog_path);
    let _ = std::fs::remove_file(src_path);
    let _ = std::fs::remove_dir_all(table_dir);
}
