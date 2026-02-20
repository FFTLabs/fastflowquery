use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use ffq_client::Engine;
use ffq_common::EngineConfig;
use ffq_storage::TableDef;

#[path = "support/mod.rs"]
mod support;

fn make_engine_with_window_fixture() -> (Engine, std::path::PathBuf) {
    let path = support::unique_path("ffq_window_mvp", "parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("grp", DataType::Utf8, false),
        Field::new("ord", DataType::Int64, false),
        Field::new("score", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    support::write_parquet(
        &path,
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "A", "A", "B", "B"])),
            Arc::new(Int64Array::from(vec![1_i64, 2, 3, 1, 2])),
            Arc::new(Int64Array::from(vec![10_i64, 10, 20, 7, 9])),
            Arc::new(Int64Array::from(vec![2_i64, 3, 5, 1, 4])),
        ],
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

fn make_engine_with_window_null_fixture() -> (Engine, std::path::PathBuf) {
    let path = support::unique_path("ffq_window_mvp_nulls", "parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("grp", DataType::Utf8, false),
        Field::new("ord", DataType::Int64, true),
        Field::new("score", DataType::Int64, false),
    ]));
    support::write_parquet(
        &path,
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "A", "A"])),
            Arc::new(Int64Array::from(vec![Some(3_i64), None, Some(1_i64)])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 30])),
        ],
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
fn row_number_over_partition_order_is_correct() {
    let (engine, path) = make_engine_with_window_fixture();
    let sql = "SELECT grp, ord, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY ord) AS rn FROM t";
    let batches = futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");

    let mut rows = Vec::new();
    for batch in &batches {
        let grp = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("grp");
        let ord = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ord");
        let rn = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("rn");
        for row in 0..batch.num_rows() {
            rows.push((
                grp.value(row).to_string(),
                ord.value(row),
                rn.value(row),
            ));
        }
    }
    rows.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    assert_eq!(
        rows,
        vec![
            ("A".to_string(), 1, 1),
            ("A".to_string(), 2, 2),
            ("A".to_string(), 3, 3),
            ("B".to_string(), 1, 1),
            ("B".to_string(), 2, 2),
        ]
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn rank_over_partition_order_is_correct() {
    let (engine, path) = make_engine_with_window_fixture();
    let sql = "SELECT grp, ord, score, RANK() OVER (PARTITION BY grp ORDER BY score) AS rnk FROM t";
    let batches = futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");

    let mut rows = Vec::new();
    for batch in &batches {
        let grp = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("grp");
        let ord = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ord");
        let rnk = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("rnk");
        for row in 0..batch.num_rows() {
            rows.push((
                grp.value(row).to_string(),
                ord.value(row),
                rnk.value(row),
            ));
        }
    }
    rows.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    assert_eq!(
        rows,
        vec![
            ("A".to_string(), 1, 1),
            ("A".to_string(), 2, 1),
            ("A".to_string(), 3, 3),
            ("B".to_string(), 1, 1),
            ("B".to_string(), 2, 2),
        ]
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn cumulative_sum_over_partition_order_is_correct() {
    let (engine, path) = make_engine_with_window_fixture();
    let sql = "SELECT grp, ord, SUM(v) OVER (PARTITION BY grp ORDER BY ord) AS running_sum FROM t";
    let batches = futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");

    let mut rows = Vec::new();
    for batch in &batches {
        let grp = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("grp");
        let ord = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ord");
        let running_sum = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("running_sum");
        for row in 0..batch.num_rows() {
            rows.push((
                grp.value(row).to_string(),
                ord.value(row),
                running_sum.value(row),
            ));
        }
    }
    rows.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    assert_eq!(
        rows,
        vec![
            ("A".to_string(), 1, 2.0),
            ("A".to_string(), 2, 5.0),
            ("A".to_string(), 3, 10.0),
            ("B".to_string(), 1, 1.0),
            ("B".to_string(), 2, 5.0),
        ]
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn named_window_desc_nulls_first_executes_correctly() {
    let (engine, path) = make_engine_with_window_null_fixture();
    let sql = "SELECT ord, ROW_NUMBER() OVER w AS rn FROM t WINDOW w AS (PARTITION BY grp ORDER BY ord DESC NULLS FIRST)";
    let batches = futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");

    let mut rows = Vec::new();
    for batch in &batches {
        let ord = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ord");
        let rn = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("rn");
        for row in 0..batch.num_rows() {
            let ord_v = if ord.is_null(row) {
                None
            } else {
                Some(ord.value(row))
            };
            rows.push((ord_v, rn.value(row)));
        }
    }
    rows.sort_unstable_by_key(|(_, rn)| *rn);
    assert_eq!(rows, vec![(None, 1), (Some(3), 2), (Some(1), 3)]);
    let _ = std::fs::remove_file(path);
}
