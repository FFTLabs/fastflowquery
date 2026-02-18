use std::collections::HashMap;
use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::sleep;
use std::time::{SystemTime, UNIX_EPOCH};
use std::time::Duration;

use arrow::array::{Int64Array, StringArray};
use arrow::array::Int32Array;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use ffq_client::Engine;
use ffq_common::{EngineConfig, SchemaDriftPolicy, SchemaInferencePolicy};
use ffq_storage::TableDef;
use parquet::arrow::ArrowWriter;

fn unique_path(ext: &str) -> std::path::PathBuf {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    let pid = std::process::id();
    let seq = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("ffq_embedded_scan_{pid}_{nanos}_{seq}.{ext}"))
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

#[test]
fn register_table_checked_infers_schema_immediately_when_enabled() {
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

    let mut cfg = EngineConfig::default();
    cfg.schema_inference = SchemaInferencePolicy::On;
    let engine = Engine::new(cfg).expect("engine");
    engine
        .register_table_checked(
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
        )
        .expect("register table");

    let inferred = engine.table_schema("t").expect("table schema");
    let inferred = inferred.expect("inferred schema");
    assert_eq!(inferred.field(0).name(), "id");
    assert_eq!(inferred.field(1).name(), "name");

    let _ = std::fs::remove_file(parquet_path);
}

#[test]
fn register_table_checked_fails_early_for_bad_parquet_path_when_enabled() {
    let mut cfg = EngineConfig::default();
    cfg.schema_inference = SchemaInferencePolicy::On;
    let engine = Engine::new(cfg).expect("engine");

    let err = engine
        .register_table_checked(
            "bad",
            TableDef {
                name: "ignored".to_string(),
                uri: "/definitely/missing/path.parquet".to_string(),
                paths: Vec::new(),
                format: "parquet".to_string(),
                schema: None,
                stats: ffq_storage::TableStats::default(),
                options: HashMap::new(),
            },
        )
        .expect_err("must fail early");
    let msg = format!("{err}");
    assert!(
        msg.contains("No such file") || msg.contains("cannot find") || msg.contains("missing"),
        "unexpected error message: {msg}"
    );
}

#[test]
fn schema_cache_refreshes_on_drift_when_policy_allows_refresh() {
    let parquet_path = unique_path("parquet");
    write_id_name_parquet(&parquet_path);

    let mut cfg = EngineConfig::default();
    cfg.schema_inference = SchemaInferencePolicy::On;
    cfg.schema_drift_policy = SchemaDriftPolicy::Refresh;
    let engine = Engine::new(cfg).expect("engine");
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

    let first = futures::executor::block_on(
        engine.sql("SELECT id FROM t").expect("sql").collect(),
    )
    .expect("collect first");
    assert_eq!(first.iter().map(|b| b.num_rows()).sum::<usize>(), 3);

    sleep(Duration::from_millis(2));
    write_id_name_city_parquet(&parquet_path);

    let second = futures::executor::block_on(
        engine
            .sql("SELECT city FROM t")
            .expect("sql second")
            .collect(),
    )
    .expect("collect second");
    assert_eq!(second.iter().map(|b| b.num_rows()).sum::<usize>(), 3);

    let _ = std::fs::remove_file(parquet_path);
}

#[test]
fn schema_cache_can_fail_on_drift_when_configured() {
    let parquet_path = unique_path("parquet");
    write_id_name_parquet(&parquet_path);

    let mut cfg = EngineConfig::default();
    cfg.schema_inference = SchemaInferencePolicy::On;
    cfg.schema_drift_policy = SchemaDriftPolicy::Fail;
    let engine = Engine::new(cfg).expect("engine");
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

    let _ = futures::executor::block_on(
        engine.sql("SELECT id FROM t").expect("sql").collect(),
    )
    .expect("collect first");

    sleep(Duration::from_millis(2));
    write_id_name_city_parquet(&parquet_path);

    let err = futures::executor::block_on(
        engine
            .sql("SELECT id FROM t")
            .expect("sql second")
            .collect(),
    )
    .expect_err("expected drift error");
    assert!(format!("{err}").contains("schema drift detected"));

    let _ = std::fs::remove_file(parquet_path);
}

#[test]
fn inferred_schema_writeback_persists_across_restart() {
    let parquet_path = unique_path("parquet");
    write_id_name_parquet(&parquet_path);
    let catalog_path = unique_path("json");

    let mut catalog = ffq_storage::Catalog::new();
    catalog.register_table(TableDef {
        name: "t".to_string(),
        uri: parquet_path.to_string_lossy().to_string(),
        paths: Vec::new(),
        format: "parquet".to_string(),
        schema: None,
        stats: ffq_storage::TableStats::default(),
        options: HashMap::new(),
    });
    catalog
        .save(catalog_path.to_str().expect("catalog path utf8"))
        .expect("save catalog");

    let mut cfg = EngineConfig::default();
    cfg.catalog_path = Some(catalog_path.to_string_lossy().to_string());
    cfg.schema_inference = SchemaInferencePolicy::On;
    cfg.schema_writeback = true;
    let engine = Engine::new(cfg.clone()).expect("engine");

    let rows = futures::executor::block_on(
        engine
            .sql("SELECT id, name FROM t")
            .expect("sql")
            .collect(),
    )
    .expect("collect");
    assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 3);

    let saved = std::fs::read_to_string(&catalog_path).expect("read catalog");
    assert!(saved.contains("schema.inferred_at"), "missing inferred_at marker");
    assert!(saved.contains("schema.fingerprint"), "missing fingerprint marker");
    assert!(saved.contains("\"schema\""), "missing persisted schema");

    let restarted = Engine::new(cfg).expect("restart engine");
    let persisted_schema = restarted.table_schema("t").expect("table schema");
    assert!(persisted_schema.is_some(), "schema should be loaded from writeback");

    let _ = std::fs::remove_file(parquet_path);
    let _ = std::fs::remove_file(catalog_path);
}

#[test]
fn schema_inference_off_requires_predeclared_schema() {
    let parquet_path = unique_path("parquet");
    write_id_name_parquet(&parquet_path);

    let mut cfg = EngineConfig::default();
    cfg.schema_inference = SchemaInferencePolicy::Off;
    let engine = Engine::new(cfg).expect("engine");
    engine.register_table(
        "t",
        TableDef {
            name: "ignored".to_string(),
            uri: parquet_path.to_string_lossy().to_string(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: None,
            stats: ffq_storage::TableStats::default(),
            options: HashMap::new(),
        },
    );

    let err = futures::executor::block_on(
        engine
            .sql("SELECT id FROM t")
            .expect("sql")
            .collect(),
    )
    .expect_err("must fail without schema inference");
    assert!(format!("{err}").contains("has no schema"));

    let _ = std::fs::remove_file(parquet_path);
}

#[test]
fn schema_inference_strict_rejects_numeric_widening_across_files() {
    let p1 = unique_path("parquet");
    let p2 = unique_path("parquet");
    write_single_numeric_parquet_i32(&p1);
    write_single_numeric_parquet_i64(&p2);

    let mut cfg = EngineConfig::default();
    cfg.schema_inference = SchemaInferencePolicy::Strict;
    let engine = Engine::new(cfg).expect("engine");
    let err = engine
        .register_table_checked(
        "t",
        TableDef {
            name: "ignored".to_string(),
            uri: String::new(),
            paths: vec![
                p1.to_string_lossy().to_string(),
                p2.to_string_lossy().to_string(),
            ],
            format: "parquet".to_string(),
            schema: None,
            stats: ffq_storage::TableStats::default(),
            options: HashMap::new(),
        },
    )
        .expect_err("strict should fail registration");
    assert!(format!("{err}").contains("strict policy"));

    let _ = std::fs::remove_file(p1);
    let _ = std::fs::remove_file(p2);
}

#[test]
fn schema_inference_permissive_allows_numeric_widening_across_files() {
    let p1 = unique_path("parquet");
    let p2 = unique_path("parquet");
    write_single_numeric_parquet_i32(&p1);
    write_single_numeric_parquet_i64(&p2);

    let mut cfg = EngineConfig::default();
    cfg.schema_inference = SchemaInferencePolicy::Permissive;
    let engine = Engine::new(cfg).expect("engine");
    engine
        .register_table_checked(
            "t",
            TableDef {
                name: "ignored".to_string(),
                uri: String::new(),
                paths: vec![
                    p1.to_string_lossy().to_string(),
                    p2.to_string_lossy().to_string(),
                ],
                format: "parquet".to_string(),
                schema: None,
                stats: ffq_storage::TableStats::default(),
                options: HashMap::new(),
            },
        )
        .expect("permissive should allow registration");
    let schema = engine
        .table_schema("t")
        .expect("schema fetch")
        .expect("inferred schema");
    assert_eq!(schema.field(0).name(), "v");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);

    let _ = std::fs::remove_file(p1);
    let _ = std::fs::remove_file(p2);
}

fn write_id_name_parquet(path: &std::path::Path) {
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

    let file = File::create(path).expect("create parquet file");
    let mut writer =
        ArrowWriter::try_new(file, schema.clone(), None).expect("create parquet writer");
    writer.write(&batch).expect("write parquet batch");
    writer.close().expect("close parquet writer");
}

fn write_id_name_city_parquet(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(StringArray::from(vec!["x", "y", "z"])),
        ],
    )
    .expect("build batch");

    let file = File::create(path).expect("create parquet file");
    let mut writer =
        ArrowWriter::try_new(file, schema.clone(), None).expect("create parquet writer");
    writer.write(&batch).expect("write parquet batch");
    writer.close().expect("close parquet writer");
}

fn write_single_numeric_parquet_i32(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1_i32, 2]))])
        .expect("build batch");
    let file = File::create(path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("create parquet writer");
    writer.write(&batch).expect("write parquet batch");
    writer.close().expect("close parquet writer");
}

fn write_single_numeric_parquet_i64(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![3_i64, 4]))])
        .expect("build batch");
    let file = File::create(path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("create parquet writer");
    writer.write(&batch).expect("write parquet batch");
    writer.close().expect("close parquet writer");
}
