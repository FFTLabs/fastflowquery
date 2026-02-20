use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow_schema::{DataType, Field, Schema};
use ffq_client::Engine;
use ffq_common::EngineConfig;
use ffq_storage::TableDef;
#[path = "support/mod.rs"]
mod support;

fn col_i64_values(batch: &arrow::record_batch::RecordBatch, col_idx: usize) -> Vec<i64> {
    let arr = batch
        .column(col_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 column");
    (0..batch.num_rows()).map(|i| arr.value(i)).collect()
}

#[test]
fn hash_join_shuffle_with_spill() {
    let left_path = support::unique_path("ffq_join_left", "parquet");
    let right_path = support::unique_path("ffq_join_right", "parquet");
    let spill_dir = support::unique_path("ffq_join_spill", "dir");

    let left_schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
    ]));
    support::write_parquet(
        &left_path,
        left_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 30, 40])),
        ],
    );

    let right_schema = Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_qty", DataType::Int64, false),
    ]));
    support::write_parquet(
        &right_path,
        right_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![2_i64, 2, 4, 7])),
            Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4])),
        ],
    );

    let mut cfg = EngineConfig::default();
    cfg.mem_budget_bytes = 128;
    cfg.spill_dir = spill_dir.to_string_lossy().into_owned();

    let engine = Engine::new(cfg).expect("engine");
    engine.register_table(
        "orders",
        TableDef {
            name: "ignored".to_string(),
            uri: left_path.to_string_lossy().into_owned(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some((*left_schema).clone()),
            stats: ffq_storage::TableStats::default(),
            options: HashMap::new(),
        },
    );
    engine.register_table(
        "lineitem",
        TableDef {
            name: "ignored".to_string(),
            uri: right_path.to_string_lossy().into_owned(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some((*right_schema).clone()),
            stats: ffq_storage::TableStats::default(),
            options: HashMap::new(),
        },
    );

    let joined = engine
        .table("orders")
        .expect("orders")
        .join(
            engine.table("lineitem").expect("lineitem"),
            vec![("o_orderkey".to_string(), "l_orderkey".to_string())],
        )
        .expect("join");

    let batches = futures::executor::block_on(joined.collect()).expect("collect");
    let batches_again = futures::executor::block_on(joined.collect()).expect("collect again");
    support::assert_batches_deterministic(&batches, &batches_again, &["o_orderkey"], 1e-9);
    let snapshot = support::snapshot_text(&batches, &["o_orderkey", "l_qty"], 1e-9);
    support::assert_or_bless_snapshot(
        "tests/snapshots/join/hash_join_shuffle_with_spill.snap",
        &snapshot,
    );
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3);
    let mut keys = batches
        .iter()
        .flat_map(|b| col_i64_values(b, 0))
        .collect::<Vec<_>>();
    keys.sort_unstable();
    assert_eq!(keys, vec![2, 2, 4]);

    let _ = std::fs::remove_file(left_path);
    let _ = std::fs::remove_file(right_path);
    let _ = std::fs::remove_dir_all(spill_dir);
}

#[test]
fn hash_join_broadcast_strategy_and_result() {
    let left_path = support::unique_path("ffq_join_bcast_left", "parquet");
    let right_path = support::unique_path("ffq_join_bcast_right", "parquet");
    let spill_dir = support::unique_path("ffq_join_bcast_spill", "dir");

    let left_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("x", DataType::Int64, false),
    ]));
    support::write_parquet(
        &left_path,
        left_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2])),
            Arc::new(Int64Array::from(vec![100_i64, 200])),
        ],
    );

    let right_schema = Arc::new(Schema::new(vec![
        Field::new("k2", DataType::Int64, false),
        Field::new("y", DataType::Int64, false),
    ]));
    support::write_parquet(
        &right_path,
        right_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![2_i64, 3])),
            Arc::new(Int64Array::from(vec![8_i64, 9])),
        ],
    );

    let mut cfg = EngineConfig::default();
    cfg.mem_budget_bytes = 1024;
    cfg.spill_dir = spill_dir.to_string_lossy().into_owned();
    cfg.broadcast_threshold_bytes = 1024;

    let engine = Engine::new(cfg).expect("engine");
    engine.register_table(
        "small",
        TableDef {
            name: "ignored".to_string(),
            uri: left_path.to_string_lossy().into_owned(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some((*left_schema).clone()),
            stats: ffq_storage::TableStats {
                rows: Some(2),
                bytes: Some(100),
            },
            options: HashMap::new(),
        },
    );
    engine.register_table(
        "big",
        TableDef {
            name: "ignored".to_string(),
            uri: right_path.to_string_lossy().into_owned(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some((*right_schema).clone()),
            stats: ffq_storage::TableStats {
                rows: Some(2_000_000),
                bytes: Some(10_000_000),
            },
            options: HashMap::new(),
        },
    );

    let joined = engine
        .table("small")
        .expect("small")
        .join(
            engine.table("big").expect("big"),
            vec![("k".to_string(), "k2".to_string())],
        )
        .expect("join");

    let explain = joined.explain().expect("explain");
    assert!(explain.contains("strategy=broadcast_left"));

    let batches = futures::executor::block_on(joined.collect()).expect("collect");
    let batches_again = futures::executor::block_on(joined.collect()).expect("collect again");
    support::assert_batches_deterministic(&batches, &batches_again, &["k"], 1e-9);
    let snapshot = support::snapshot_text(&batches, &["k"], 1e-9);
    support::assert_or_bless_snapshot(
        "tests/snapshots/join/hash_join_broadcast_strategy_and_result.snap",
        &snapshot,
    );
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1);
    let keys = batches
        .iter()
        .flat_map(|b| col_i64_values(b, 0))
        .collect::<Vec<_>>();
    assert_eq!(keys, vec![2]);

    let _ = std::fs::remove_file(left_path);
    let _ = std::fs::remove_file(right_path);
    let _ = std::fs::remove_dir_all(spill_dir);
}

#[test]
fn hash_join_adaptive_switches_from_shuffle_plan_to_broadcast() {
    let left_path = support::unique_path("ffq_join_adaptive_left", "parquet");
    let right_path = support::unique_path("ffq_join_adaptive_right", "parquet");
    let spill_dir = support::unique_path("ffq_join_adaptive_spill", "dir");

    let left_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("x", DataType::Int64, false),
    ]));
    support::write_parquet(
        &left_path,
        left_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(Int64Array::from(vec![11_i64, 22, 33])),
        ],
    );

    let right_schema = Arc::new(Schema::new(vec![
        Field::new("k2", DataType::Int64, false),
        Field::new("y", DataType::Int64, false),
    ]));
    support::write_parquet(
        &right_path,
        right_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![2_i64, 3, 4])),
            Arc::new(Int64Array::from(vec![200_i64, 300, 400])),
        ],
    );

    let mut cfg = EngineConfig::default();
    cfg.mem_budget_bytes = 1024 * 1024;
    cfg.spill_dir = spill_dir.to_string_lossy().into_owned();
    cfg.broadcast_threshold_bytes = 128 * 1024;

    let engine = Engine::new(cfg).expect("engine");
    engine.register_table(
        "left_t",
        TableDef {
            name: "ignored".to_string(),
            uri: left_path.to_string_lossy().into_owned(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some((*left_schema).clone()),
            // Intentionally oversized stats to push optimizer into shuffle strategy.
            stats: ffq_storage::TableStats {
                rows: Some(5_000_000),
                bytes: Some(10_000_000),
            },
            options: HashMap::new(),
        },
    );
    engine.register_table(
        "right_t",
        TableDef {
            name: "ignored".to_string(),
            uri: right_path.to_string_lossy().into_owned(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some((*right_schema).clone()),
            stats: ffq_storage::TableStats {
                rows: Some(5_000_000),
                bytes: Some(10_000_000),
            },
            options: HashMap::new(),
        },
    );

    let joined = engine
        .table("left_t")
        .expect("left_t")
        .join(
            engine.table("right_t").expect("right_t"),
            vec![("k".to_string(), "k2".to_string())],
        )
        .expect("join");

    let explain = joined.explain().expect("explain");
    assert!(
        explain.contains("strategy=shuffle"),
        "expected shuffle primary plan, got:\n{explain}"
    );
    assert!(
        explain.contains("adaptive_alternatives="),
        "expected adaptive alternatives in explain:\n{explain}"
    );

    let batches = futures::executor::block_on(joined.collect()).expect("collect");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2);

    let report = engine
        .last_query_stats_report()
        .expect("stats report should exist");
    assert!(
        report.contains("op=Broadcast"),
        "adaptive runtime should choose broadcast alternative:\n{report}"
    );
    assert!(
        !report.contains("op=ShuffleWrite"),
        "adaptive runtime should avoid shuffle subtree when broadcast selected:\n{report}"
    );

    let _ = std::fs::remove_file(left_path);
    let _ = std::fs::remove_file(right_path);
    let _ = std::fs::remove_dir_all(spill_dir);
}

fn make_outer_join_fixture_engine() -> (
    Engine,
    std::path::PathBuf,
    std::path::PathBuf,
    std::path::PathBuf,
) {
    let left_path = support::unique_path("ffq_outer_left", "parquet");
    let right_path = support::unique_path("ffq_outer_right", "parquet");
    let spill_dir = support::unique_path("ffq_outer_spill", "dir");

    let left_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("lval", DataType::Int64, false),
    ]));
    support::write_parquet(
        &left_path,
        left_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 4])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 40])),
        ],
    );

    let right_schema = Arc::new(Schema::new(vec![
        Field::new("k2", DataType::Int64, false),
        Field::new("rval", DataType::Int64, false),
    ]));
    support::write_parquet(
        &right_path,
        right_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![2_i64, 3, 4])),
            Arc::new(Int64Array::from(vec![200_i64, 300, 400])),
        ],
    );

    let mut cfg = EngineConfig::default();
    cfg.mem_budget_bytes = 128;
    cfg.spill_dir = spill_dir.to_string_lossy().into_owned();

    let engine = Engine::new(cfg).expect("engine");
    engine.register_table(
        "l",
        TableDef {
            name: "ignored".to_string(),
            uri: left_path.to_string_lossy().into_owned(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some((*left_schema).clone()),
            stats: ffq_storage::TableStats::default(),
            options: HashMap::new(),
        },
    );
    engine.register_table(
        "r",
        TableDef {
            name: "ignored".to_string(),
            uri: right_path.to_string_lossy().into_owned(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some((*right_schema).clone()),
            stats: ffq_storage::TableStats::default(),
            options: HashMap::new(),
        },
    );
    (engine, left_path, right_path, spill_dir)
}

#[test]
fn hash_join_left_outer_correctness() {
    let (engine, left_path, right_path, spill_dir) = make_outer_join_fixture_engine();
    let query = "SELECT k, lval, k2, rval FROM l LEFT JOIN r ON k = k2";
    let batches =
        futures::executor::block_on(engine.sql(query).expect("sql").collect()).expect("collect");
    let snapshot = support::snapshot_text(&batches, &["k", "k2"], 1e-9);
    support::assert_or_bless_snapshot(
        "tests/snapshots/join/hash_join_left_outer_correctness.snap",
        &snapshot,
    );
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3);
    let _ = std::fs::remove_file(left_path);
    let _ = std::fs::remove_file(right_path);
    let _ = std::fs::remove_dir_all(spill_dir);
}

#[test]
fn hash_join_right_outer_correctness() {
    let (engine, left_path, right_path, spill_dir) = make_outer_join_fixture_engine();
    let query = "SELECT k, lval, k2, rval FROM l RIGHT JOIN r ON k = k2";
    let batches =
        futures::executor::block_on(engine.sql(query).expect("sql").collect()).expect("collect");
    let snapshot = support::snapshot_text(&batches, &["k2", "k"], 1e-9);
    support::assert_or_bless_snapshot(
        "tests/snapshots/join/hash_join_right_outer_correctness.snap",
        &snapshot,
    );
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3);
    let _ = std::fs::remove_file(left_path);
    let _ = std::fs::remove_file(right_path);
    let _ = std::fs::remove_dir_all(spill_dir);
}

#[test]
fn hash_join_full_outer_correctness() {
    let (engine, left_path, right_path, spill_dir) = make_outer_join_fixture_engine();
    let query = "SELECT k, lval, k2, rval FROM l FULL OUTER JOIN r ON k = k2";
    let batches =
        futures::executor::block_on(engine.sql(query).expect("sql").collect()).expect("collect");
    let snapshot = support::snapshot_text(&batches, &["k", "k2"], 1e-9);
    support::assert_or_bless_snapshot(
        "tests/snapshots/join/hash_join_full_outer_correctness.snap",
        &snapshot,
    );
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 4);
    let _ = std::fs::remove_file(left_path);
    let _ = std::fs::remove_file(right_path);
    let _ = std::fs::remove_dir_all(spill_dir);
}
