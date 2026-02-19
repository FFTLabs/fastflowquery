use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow_schema::{DataType, Field, Schema};
use ffq_client::Engine;
use ffq_common::EngineConfig;
use ffq_storage::TableDef;

#[path = "support/mod.rs"]
mod support;

fn int64_values(batch: &arrow::record_batch::RecordBatch, col_idx: usize) -> Vec<i64> {
    let arr = batch
        .column(col_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    (0..batch.num_rows()).map(|i| arr.value(i)).collect()
}

fn make_engine() -> (Engine, std::path::PathBuf, std::path::PathBuf) {
    let t_path = support::unique_path("ffq_cte_t", "parquet");
    let s_path = support::unique_path("ffq_cte_s", "parquet");

    let t_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    support::write_parquet(
        &t_path,
        t_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    );

    let s_schema = Arc::new(Schema::new(vec![Field::new("k2", DataType::Int64, false)]));
    support::write_parquet(
        &s_path,
        s_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![2_i64, 3]))],
    );

    let engine = Engine::new(EngineConfig::default()).expect("engine");
    engine.register_table(
        "t",
        TableDef {
            name: "ignored".to_string(),
            uri: t_path.to_string_lossy().into_owned(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some((*t_schema).clone()),
            stats: ffq_storage::TableStats::default(),
            options: HashMap::new(),
        },
    );
    engine.register_table(
        "s",
        TableDef {
            name: "ignored".to_string(),
            uri: s_path.to_string_lossy().into_owned(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some((*s_schema).clone()),
            stats: ffq_storage::TableStats::default(),
            options: HashMap::new(),
        },
    );

    (engine, t_path, s_path)
}

#[test]
fn cte_query_runs() {
    let (engine, t_path, s_path) = make_engine();
    let sql = "WITH c AS (SELECT k FROM t) SELECT k FROM c";
    let batches = futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");
    let mut values = batches.iter().flat_map(|b| int64_values(b, 0)).collect::<Vec<_>>();
    values.sort_unstable();
    assert_eq!(values, vec![1, 2, 3]);
    let _ = std::fs::remove_file(t_path);
    let _ = std::fs::remove_file(s_path);
}

#[test]
fn uncorrelated_in_subquery_runs() {
    let (engine, t_path, s_path) = make_engine();
    let sql = "SELECT k FROM t WHERE k IN (SELECT k2 FROM s)";
    let batches = futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");
    let mut values = batches.iter().flat_map(|b| int64_values(b, 0)).collect::<Vec<_>>();
    values.sort_unstable();
    assert_eq!(values, vec![2, 3]);
    let _ = std::fs::remove_file(t_path);
    let _ = std::fs::remove_file(s_path);
}

#[test]
fn uncorrelated_exists_subquery_runs() {
    let (engine, t_path, s_path) = make_engine();
    let sql = "SELECT k FROM t WHERE EXISTS (SELECT k2 FROM s WHERE k2 > 2)";
    let batches = futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");
    let mut values = batches.iter().flat_map(|b| int64_values(b, 0)).collect::<Vec<_>>();
    values.sort_unstable();
    assert_eq!(values, vec![1, 2, 3]);
    let _ = std::fs::remove_file(t_path);
    let _ = std::fs::remove_file(s_path);
}
