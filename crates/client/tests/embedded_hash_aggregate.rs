use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use ffq_client::expr::col;
use ffq_client::Engine;
use ffq_common::EngineConfig;
use ffq_planner::AggExpr;
use ffq_storage::TableDef;
use parquet::arrow::ArrowWriter;

fn unique_path(prefix: &str, ext: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}_{nanos}.{ext}"))
}

#[test]
fn hash_aggregate_spills_and_merges() {
    let parquet_path = unique_path("ffq_hash_agg", "parquet");
    let spill_dir = unique_path("ffq_hash_agg_spill", "dir");

    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]));

    let mut keys = Vec::new();
    let mut values = Vec::new();
    for i in 0_i64..500_i64 {
        keys.push(format!("group_{}", i % 7));
        values.push(1_i64);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("build batch");

    let file = File::create(&parquet_path).expect("create parquet file");
    let mut writer =
        ArrowWriter::try_new(file, schema.clone(), None).expect("create parquet writer");
    writer.write(&batch).expect("write parquet");
    writer.close().expect("close writer");

    let mut cfg = EngineConfig::default();
    cfg.mem_budget_bytes = 512;
    cfg.spill_dir = spill_dir.to_string_lossy().into_owned();

    let engine = Engine::new(cfg).expect("engine");
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

    let df = engine
        .table("t")
        .expect("table")
        .groupby(vec![col("k")])
        .agg(vec![
            (AggExpr::Count(col("v")), "cnt".to_string()),
            (AggExpr::Sum(col("v")), "sum_v".to_string()),
            (AggExpr::Avg(col("v")), "avg_v".to_string()),
        ]);

    let batches = futures::executor::block_on(df.collect()).expect("collect");
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];

    let k = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("k");
    let cnt = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("cnt");
    let sum_v = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sum_v");
    let avg_v = batch
        .column(3)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("avg_v");

    let mut seen = 0_i64;
    for row in 0..batch.num_rows() {
        let group = k.value(row);
        assert!(group.starts_with("group_"));
        assert!(cnt.value(row) >= 71);
        assert_eq!(cnt.value(row), sum_v.value(row));
        assert_eq!(avg_v.value(row), 1.0);
        seen += cnt.value(row);
    }
    assert_eq!(seen, 500);

    let _ = std::fs::remove_file(parquet_path);
    let _ = std::fs::remove_dir_all(spill_dir);
}
