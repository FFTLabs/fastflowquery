#[cfg(feature = "vector")]
use std::collections::HashMap;
#[cfg(feature = "vector")]
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use ffq_client::Engine;
use ffq_common::EngineConfig;
#[cfg(feature = "vector")]
use ffq_planner::LiteralValue;
use ffq_storage::TableStats;
#[path = "support/mod.rs"]
mod support;

fn register_core_tables(engine: &Engine, fixtures: &support::IntegrationParquetFixtures) {
    support::register_parquet_table(
        engine,
        "lineitem",
        &fixtures.lineitem,
        Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_partkey", DataType::Int64, false),
        ]),
        TableStats::default(),
    );
    support::register_parquet_table(
        engine,
        "orders",
        &fixtures.orders,
        Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
        ]),
        TableStats::default(),
    );
}

#[cfg(feature = "vector")]
fn register_docs_table(engine: &Engine, fixtures: &support::IntegrationParquetFixtures) {
    let emb_field = Field::new("item", DataType::Float32, true);
    support::register_parquet_table(
        engine,
        "docs",
        &fixtures.docs,
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("title", DataType::Utf8, false),
            Field::new("lang", DataType::Utf8, false),
            Field::new("emb", DataType::FixedSizeList(Arc::new(emb_field), 3), true),
        ]),
        TableStats::default(),
    );
}

#[test]
fn embedded_integration_runner_emits_normalized_core_snapshots() {
    let fixtures = support::ensure_integration_parquet_fixtures();
    let engine = Engine::new(EngineConfig::default()).expect("engine");
    register_core_tables(&engine, &fixtures);

    let scan_batches = futures::executor::block_on(
        engine
            .sql(support::integration_queries::scan_filter_project())
            .expect("scan sql")
            .collect(),
    )
    .expect("scan collect");
    let join_batches = futures::executor::block_on(
        engine
            .sql(support::integration_queries::join_projection())
            .expect("join sql")
            .collect(),
    )
    .expect("join collect");
    let agg_batches = futures::executor::block_on(
        engine
            .sql(support::integration_queries::join_aggregate())
            .expect("agg sql")
            .collect(),
    )
    .expect("agg collect");

    let mut snapshot = String::new();
    snapshot.push_str("## scan_filter_project\n");
    snapshot.push_str(&support::snapshot_text(
        &scan_batches,
        &["l_orderkey", "l_partkey"],
        1e-9,
    ));
    snapshot.push('\n');
    snapshot.push_str("## join_projection\n");
    snapshot.push_str(&support::snapshot_text(
        &join_batches,
        &["l_orderkey", "l_partkey", "o_custkey"],
        1e-9,
    ));
    snapshot.push('\n');
    snapshot.push_str("## join_aggregate\n");
    snapshot.push_str(&support::snapshot_text(&agg_batches, &["l_orderkey"], 1e-9));

    support::assert_or_bless_snapshot("tests/snapshots/integration/embedded_core.snap", &snapshot);
}

#[cfg(feature = "vector")]
#[test]
fn embedded_integration_runner_emits_normalized_vector_snapshot() {
    let fixtures = support::ensure_integration_parquet_fixtures();
    let engine = Engine::new(EngineConfig::default()).expect("engine");
    register_docs_table(&engine, &fixtures);

    let mut params = HashMap::new();
    params.insert(
        "q".to_string(),
        LiteralValue::VectorF32(vec![1.0, 0.0, 0.0]),
    );

    let batches = futures::executor::block_on(
        engine
            .sql_with_params(support::integration_queries::vector_topk_cosine(), params)
            .expect("vector topk sql")
            .collect(),
    )
    .expect("vector topk collect");

    let snapshot = support::snapshot_text(&batches, &["id"], 1e-9);
    support::assert_or_bless_snapshot("tests/snapshots/integration/embedded_vector_topk.snap", &snapshot);
}
