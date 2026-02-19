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

#[test]
fn uncorrelated_exists_truth_table_non_empty_subquery() {
    let (engine, t_path, s_path) = make_engine();

    let exists_sql = "SELECT k FROM t WHERE EXISTS (SELECT k2 FROM s)";
    let exists_batches =
        futures::executor::block_on(engine.sql(exists_sql).expect("sql").collect()).expect("collect");
    let mut exists_values = exists_batches
        .iter()
        .flat_map(|b| int64_values(b, 0))
        .collect::<Vec<_>>();
    exists_values.sort_unstable();
    assert_eq!(exists_values, vec![1, 2, 3]);

    let not_exists_sql = "SELECT k FROM t WHERE NOT EXISTS (SELECT k2 FROM s)";
    let not_exists_batches = futures::executor::block_on(
        engine.sql(not_exists_sql).expect("sql").collect(),
    )
    .expect("collect");
    let not_exists_values = not_exists_batches
        .iter()
        .flat_map(|b| int64_values(b, 0))
        .collect::<Vec<_>>();
    assert!(not_exists_values.is_empty(), "unexpected rows: {not_exists_values:?}");

    let _ = std::fs::remove_file(t_path);
    let _ = std::fs::remove_file(s_path);
}

#[test]
fn correlated_exists_rewrites_and_runs() {
    let (engine, t_path, s_path) = make_engine();

    let sql = "SELECT k FROM t WHERE EXISTS (SELECT k2 FROM s WHERE s.k2 = t.k)";
    let batches = futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");
    let mut values = batches
        .iter()
        .flat_map(|b| int64_values(b, 0))
        .collect::<Vec<_>>();
    values.sort_unstable();
    assert_eq!(values, vec![2, 3]);

    let sql_with_inner_filter =
        "SELECT k FROM t WHERE EXISTS (SELECT k2 FROM s WHERE s.k2 = t.k AND s.k2 > 2)";
    let filtered_batches = futures::executor::block_on(
        engine.sql(sql_with_inner_filter).expect("sql").collect(),
    )
    .expect("collect");
    let filtered_values = filtered_batches
        .iter()
        .flat_map(|b| int64_values(b, 0))
        .collect::<Vec<_>>();
    assert_eq!(filtered_values, vec![3]);

    let _ = std::fs::remove_file(t_path);
    let _ = std::fs::remove_file(s_path);
}

#[test]
fn correlated_not_exists_rewrites_and_runs() {
    let (engine, t_path, s_path) = make_engine();

    let sql = "SELECT k FROM t WHERE NOT EXISTS (SELECT k2 FROM s WHERE s.k2 = t.k)";
    let batches = futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");
    let values = batches
        .iter()
        .flat_map(|b| int64_values(b, 0))
        .collect::<Vec<_>>();
    assert_eq!(values, vec![1]);

    let _ = std::fs::remove_file(t_path);
    let _ = std::fs::remove_file(s_path);
}

#[test]
fn uncorrelated_exists_truth_table_empty_subquery() {
    let (engine, t_path, s_path) = make_engine();
    let sempty_path = support::unique_path("ffq_cte_sempty", "parquet");
    let sempty_schema = Arc::new(Schema::new(vec![Field::new("k2", DataType::Int64, false)]));
    support::write_parquet(
        &sempty_path,
        sempty_schema.clone(),
        vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
    );
    engine.register_table(
        "sempty_exists",
        TableDef {
            name: "ignored".to_string(),
            uri: sempty_path.to_string_lossy().into_owned(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some((*sempty_schema).clone()),
            stats: ffq_storage::TableStats::default(),
            options: HashMap::new(),
        },
    );

    let exists_empty_sql = "SELECT k FROM t WHERE EXISTS (SELECT k2 FROM sempty_exists)";
    let exists_empty_batches = futures::executor::block_on(
        engine.sql(exists_empty_sql).expect("sql").collect(),
    )
    .expect("collect");
    let exists_empty_values = exists_empty_batches
        .iter()
        .flat_map(|b| int64_values(b, 0))
        .collect::<Vec<_>>();
    assert!(exists_empty_values.is_empty(), "unexpected rows: {exists_empty_values:?}");

    let not_exists_empty_sql = "SELECT k FROM t WHERE NOT EXISTS (SELECT k2 FROM sempty_exists)";
    let not_exists_empty_batches = futures::executor::block_on(
        engine.sql(not_exists_empty_sql).expect("sql").collect(),
    )
    .expect("collect");
    let mut not_exists_empty_values = not_exists_empty_batches
        .iter()
        .flat_map(|b| int64_values(b, 0))
        .collect::<Vec<_>>();
    not_exists_empty_values.sort_unstable();
    assert_eq!(not_exists_empty_values, vec![1, 2, 3]);

    let _ = std::fs::remove_file(t_path);
    let _ = std::fs::remove_file(s_path);
    let _ = std::fs::remove_file(sempty_path);
}

#[test]
fn scalar_subquery_comparison_runs() {
    let (engine, t_path, s_path) = make_engine();
    let sql = "SELECT k FROM t WHERE k = (SELECT max(k2) FROM s)";
    let batches = futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");
    let values = batches.iter().flat_map(|b| int64_values(b, 0)).collect::<Vec<_>>();
    assert_eq!(values, vec![3]);
    let _ = std::fs::remove_file(t_path);
    let _ = std::fs::remove_file(s_path);
}

#[test]
fn scalar_subquery_errors_on_multiple_rows() {
    let (engine, t_path, s_path) = make_engine();
    let sql = "SELECT k FROM t WHERE k = (SELECT k2 FROM s)";
    let err = futures::executor::block_on(engine.sql(sql).expect("sql").collect())
        .expect_err("expected scalar-subquery multi-row error");
    assert!(
        err.to_string()
            .contains("scalar subquery returned more than one row"),
        "unexpected error: {err}"
    );
    let _ = std::fs::remove_file(t_path);
    let _ = std::fs::remove_file(s_path);
}

fn make_engine_with_in_null_fixtures() -> (Engine, Vec<std::path::PathBuf>) {
    let t_path = support::unique_path("ffq_in_null_t", "parquet");
    let s_null_path = support::unique_path("ffq_in_null_snull", "parquet");
    let s_empty_path = support::unique_path("ffq_in_null_sempty", "parquet");
    let s_all_null_path = support::unique_path("ffq_in_null_sallnull", "parquet");

    let t_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
    support::write_parquet(
        &t_path,
        t_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![Some(1_i64), Some(2), None]))],
    );

    let s_schema = Arc::new(Schema::new(vec![Field::new("k2", DataType::Int64, true)]));
    support::write_parquet(
        &s_null_path,
        s_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![Some(2_i64), None]))],
    );
    support::write_parquet(
        &s_empty_path,
        s_schema.clone(),
        vec![Arc::new(Int64Array::from(Vec::<Option<i64>>::new()))],
    );
    support::write_parquet(
        &s_all_null_path,
        s_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![None, None]))],
    );

    let engine = Engine::new(EngineConfig::default()).expect("engine");
    for (name, path, schema) in [
        ("tnull", &t_path, &t_schema),
        ("snull", &s_null_path, &s_schema),
        ("sempty", &s_empty_path, &s_schema),
        ("sallnull", &s_all_null_path, &s_schema),
    ] {
        engine.register_table(
            name,
            TableDef {
                name: "ignored".to_string(),
                uri: path.to_string_lossy().into_owned(),
                paths: Vec::new(),
                format: "parquet".to_string(),
                schema: Some((**schema).clone()),
                stats: ffq_storage::TableStats::default(),
                options: HashMap::new(),
            },
        );
    }
    (
        engine,
        vec![t_path, s_null_path, s_empty_path, s_all_null_path],
    )
}

#[test]
fn in_not_in_null_semantics_with_null_in_rhs() {
    let (engine, paths) = make_engine_with_in_null_fixtures();

    let in_sql = "SELECT k FROM tnull WHERE k IN (SELECT k2 FROM snull)";
    let in_batches =
        futures::executor::block_on(engine.sql(in_sql).expect("sql").collect()).expect("collect");
    let in_values = in_batches
        .iter()
        .flat_map(|b| int64_values(b, 0))
        .collect::<Vec<_>>();
    assert_eq!(in_values, vec![2]);

    let not_in_sql = "SELECT k FROM tnull WHERE k NOT IN (SELECT k2 FROM snull)";
    let not_in_batches = futures::executor::block_on(
        engine.sql(not_in_sql).expect("sql").collect(),
    )
    .expect("collect");
    let not_in_values = not_in_batches
        .iter()
        .flat_map(|b| int64_values(b, 0))
        .collect::<Vec<_>>();
    assert!(not_in_values.is_empty(), "unexpected rows: {not_in_values:?}");

    for p in paths {
        let _ = std::fs::remove_file(p);
    }
}

#[test]
fn in_not_in_null_semantics_with_empty_rhs_and_all_null_rhs() {
    let (engine, paths) = make_engine_with_in_null_fixtures();

    let in_empty_sql = "SELECT k FROM tnull WHERE k IN (SELECT k2 FROM sempty)";
    let in_empty_batches = futures::executor::block_on(
        engine.sql(in_empty_sql).expect("sql").collect(),
    )
    .expect("collect");
    let in_empty_values = in_empty_batches
        .iter()
        .flat_map(|b| int64_values(b, 0))
        .collect::<Vec<_>>();
    assert!(in_empty_values.is_empty(), "unexpected rows: {in_empty_values:?}");

    let not_in_empty_sql = "SELECT k FROM tnull WHERE k NOT IN (SELECT k2 FROM sempty)";
    let not_in_empty_batches = futures::executor::block_on(
        engine.sql(not_in_empty_sql).expect("sql").collect(),
    )
    .expect("collect");
    let mut not_in_empty_values = not_in_empty_batches
        .iter()
        .flat_map(|b| int64_values(b, 0))
        .collect::<Vec<_>>();
    not_in_empty_values.sort_unstable();
    assert_eq!(not_in_empty_values, vec![1, 2]);

    let in_all_null_sql = "SELECT k FROM tnull WHERE k IN (SELECT k2 FROM sallnull)";
    let in_all_null_batches = futures::executor::block_on(
        engine.sql(in_all_null_sql).expect("sql").collect(),
    )
    .expect("collect");
    let in_all_null_values = in_all_null_batches
        .iter()
        .flat_map(|b| int64_values(b, 0))
        .collect::<Vec<_>>();
    assert!(in_all_null_values.is_empty(), "unexpected rows: {in_all_null_values:?}");

    let not_in_all_null_sql = "SELECT k FROM tnull WHERE k NOT IN (SELECT k2 FROM sallnull)";
    let not_in_all_null_batches = futures::executor::block_on(
        engine.sql(not_in_all_null_sql).expect("sql").collect(),
    )
    .expect("collect");
    let not_in_all_null_values = not_in_all_null_batches
        .iter()
        .flat_map(|b| int64_values(b, 0))
        .collect::<Vec<_>>();
    assert!(
        not_in_all_null_values.is_empty(),
        "unexpected rows: {not_in_all_null_values:?}"
    );

    for p in paths {
        let _ = std::fs::remove_file(p);
    }
}
