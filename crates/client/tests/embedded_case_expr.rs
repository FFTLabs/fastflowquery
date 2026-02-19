use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow_schema::{DataType, Field, Schema};
use ffq_client::Engine;
use ffq_common::EngineConfig;
use ffq_storage::TableDef;

#[path = "support/mod.rs"]
mod support;

fn int64_col(batch: &arrow::record_batch::RecordBatch, idx: usize) -> Vec<i64> {
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    (0..batch.num_rows()).map(|i| arr.value(i)).collect()
}

fn make_engine_with_case_fixture() -> (Engine, std::path::PathBuf) {
    let path = support::unique_path("ffq_case_expr", "parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    support::write_parquet(
        &path,
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    );
    let engine = Engine::new(EngineConfig::default()).expect("engine");
    engine.register_table(
        "t",
        TableDef {
            name: "ignored".to_string(),
            uri: path.to_string_lossy().into_owned(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some((*schema).clone()),
            stats: ffq_storage::TableStats::default(),
            options: HashMap::new(),
        },
    );
    (engine, path)
}

#[test]
fn case_expression_works_in_projection() {
    let (engine, path) = make_engine_with_case_fixture();
    let sql = "SELECT k, CASE WHEN k > 1 THEN k + 10 ELSE 0 END AS c FROM t";
    let batches = futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");
    let mut rows = batches
        .iter()
        .flat_map(|b| {
            let k = int64_col(b, 0);
            let c = int64_col(b, 1);
            k.into_iter().zip(c)
        })
        .collect::<Vec<_>>();
    rows.sort_unstable_by_key(|(k, _)| *k);
    assert_eq!(rows, vec![(1, 0), (2, 12), (3, 13)]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn case_expression_works_in_filter() {
    let (engine, path) = make_engine_with_case_fixture();
    let sql = "SELECT k FROM t WHERE CASE WHEN k > 1 THEN true ELSE false END";
    let batches = futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");
    let mut keys = batches.iter().flat_map(|b| int64_col(b, 0)).collect::<Vec<_>>();
    keys.sort_unstable();
    assert_eq!(keys, vec![2, 3]);
    let _ = std::fs::remove_file(path);
}
