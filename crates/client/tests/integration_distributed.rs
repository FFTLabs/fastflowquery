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

    let agg_batches = engine
        .sql(support::integration_queries::join_aggregate())
        .expect("agg sql")
        .collect()
        .await
        .expect("agg collect");
    let join_batches = engine
        .sql(support::integration_queries::join_projection())
        .expect("join sql")
        .collect()
        .await
        .expect("join collect");

    let agg_rows: usize = agg_batches.iter().map(|b| b.num_rows()).sum();
    let join_rows: usize = join_batches.iter().map(|b| b.num_rows()).sum();
    assert!(agg_rows > 0, "aggregate query returned no rows");
    assert!(join_rows > 0, "join query returned no rows");

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

    if let Some(prev) = prev_catalog {
        env::set_var("FFQ_CATALOG_PATH", prev);
    } else {
        env::remove_var("FFQ_CATALOG_PATH");
    }
    let _ = std::fs::remove_file(catalog_path);
}
