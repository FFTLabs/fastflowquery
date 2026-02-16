#![cfg(feature = "distributed")]

use std::env;

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use ffq_client::Engine;
use ffq_common::EngineConfig;
use ffq_storage::TableStats;
#[path = "support/mod.rs"]
mod support;

fn collect_group_counts(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    for batch in batches {
        let k = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("k");
        let c = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("c");
        for i in 0..batch.num_rows() {
            out.push((k.value(i), c.value(i)));
        }
    }
    out.sort_unstable();
    out
}

fn collect_join_rows(batches: &[RecordBatch]) -> Vec<(i64, i64, i64)> {
    let mut out = Vec::new();
    for batch in batches {
        let k = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("l_orderkey");
        let part = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("l_partkey");
        let cust = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("o_custkey");
        for i in 0..batch.num_rows() {
            out.push((k.value(i), part.value(i), cust.value(i)));
        }
    }
    out.sort_unstable();
    out
}

async fn run_query(engine: &Engine, sql: &str) -> Vec<RecordBatch> {
    engine
        .sql(sql)
        .expect("sql")
        .collect()
        .await
        .expect("collect")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires external coordinator + worker cluster; run with scripts/run-distributed-integration.sh"]
async fn distributed_integration_runner_returns_expected_join_agg_results() {
    let endpoint = env::var("FFQ_COORDINATOR_ENDPOINT").expect(
        "FFQ_COORDINATOR_ENDPOINT must be set (example: http://127.0.0.1:50051). Start docker compose stack first.",
    );
    assert!(
        endpoint.starts_with("http://") || endpoint.starts_with("https://"),
        "FFQ_COORDINATOR_ENDPOINT must include scheme (http://...)"
    );

    let prev_catalog = env::var("FFQ_CATALOG_PATH").ok();
    let catalog_path = support::unique_path("ffq_integration_distributed_catalog", "json");
    env::set_var("FFQ_CATALOG_PATH", catalog_path.to_string_lossy().to_string());

    let fixtures = support::ensure_integration_parquet_fixtures();
    let engine = Engine::new(EngineConfig::default()).expect("distributed engine");
    support::register_parquet_table(
        &engine,
        "lineitem",
        &fixtures.lineitem,
        Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_partkey", DataType::Int64, false),
        ]),
        TableStats::default(),
    );
    support::register_parquet_table(
        &engine,
        "orders",
        &fixtures.orders,
        Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
        ]),
        TableStats::default(),
    );

    let agg_sql = support::integration_queries::join_aggregate();
    let join_sql = support::integration_queries::join_projection();
    let scan_sql = support::integration_queries::scan_filter_project();

    let agg_batches = run_query(&engine, agg_sql).await;
    let join_batches = run_query(&engine, join_sql).await;
    let scan_batches = run_query(&engine, scan_sql).await;

    let agg_rows: usize = agg_batches.iter().map(|b| b.num_rows()).sum();
    let join_rows: usize = join_batches.iter().map(|b| b.num_rows()).sum();
    let scan_rows: usize = scan_batches.iter().map(|b| b.num_rows()).sum();
    assert!(agg_rows > 0, "aggregate query returned no rows");
    assert!(join_rows > 0, "join query returned no rows");
    assert!(scan_rows > 0, "scan query returned no rows");

    let agg = collect_group_counts(&agg_batches);
    let join = collect_join_rows(&join_batches);
    assert_eq!(agg, vec![(2, 2), (3, 3)]);
    assert_eq!(
        join,
        vec![
            (2, 20, 100),
            (2, 21, 100),
            (3, 30, 200),
            (3, 31, 200),
            (3, 32, 200)
        ]
    );

    // Parity checks: execute the same shared suite in embedded mode and compare normalized outputs.
    env::remove_var("FFQ_COORDINATOR_ENDPOINT");
    let embedded_engine = Engine::new(EngineConfig::default()).expect("embedded engine");
    support::register_parquet_table(
        &embedded_engine,
        "lineitem",
        &fixtures.lineitem,
        Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_partkey", DataType::Int64, false),
        ]),
        TableStats::default(),
    );
    support::register_parquet_table(
        &embedded_engine,
        "orders",
        &fixtures.orders,
        Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
        ]),
        TableStats::default(),
    );

    for (name, sql, sort_by) in [
        ("scan_filter_project", scan_sql, vec!["l_orderkey", "l_partkey"]),
        (
            "join_projection",
            join_sql,
            vec!["l_orderkey", "l_partkey", "o_custkey"],
        ),
        ("join_aggregate", agg_sql, vec!["l_orderkey"]),
    ] {
        let dist_batches = run_query(&engine, sql).await;
        let emb_batches = run_query(&embedded_engine, sql).await;
        let dist_norm = support::snapshot_text(&dist_batches, &sort_by, 1e-9);
        let emb_norm = support::snapshot_text(&emb_batches, &sort_by, 1e-9);
        assert_eq!(
            dist_norm, emb_norm,
            "distributed vs embedded mismatch for query {name}"
        );
    }

    if let Some(prev) = prev_catalog {
        env::set_var("FFQ_CATALOG_PATH", prev);
    } else {
        env::remove_var("FFQ_CATALOG_PATH");
    }
    let _ = std::fs::remove_file(catalog_path);
}
