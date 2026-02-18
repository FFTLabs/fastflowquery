use arrow_schema::{DataType, Field, Schema};
use ffq_client::Engine;
use ffq_common::EngineConfig;
use ffq_storage::TableStats;
#[path = "support/mod.rs"]
mod support;

fn register_base_tables(engine: &Engine, fixtures: &support::IntegrationParquetFixtures) {
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

#[test]
fn deterministic_local_parquet_fixtures_produce_stable_normalized_outputs() {
    let fixtures_first = support::ensure_integration_parquet_fixtures();
    let fixtures_second = support::ensure_integration_parquet_fixtures();
    assert_eq!(fixtures_first.lineitem, fixtures_second.lineitem);
    assert_eq!(fixtures_first.orders, fixtures_second.orders);
    assert!(fixtures_first.lineitem.exists());
    assert!(fixtures_first.orders.exists());

    let engine = Engine::new(EngineConfig::default()).expect("engine");
    register_base_tables(&engine, &fixtures_first);

    let scan_sql = support::integration_queries::scan_filter_project();
    let join_sql = support::integration_queries::join_projection();
    let agg_sql = support::integration_queries::join_aggregate();

    let scan_1 = futures::executor::block_on(engine.sql(scan_sql).expect("scan sql").collect())
        .expect("scan collect");
    let scan_2 = futures::executor::block_on(engine.sql(scan_sql).expect("scan sql").collect())
        .expect("scan collect");
    support::assert_batches_deterministic(&scan_1, &scan_2, &["l_orderkey", "l_partkey"], 1e-9);

    let join_1 = futures::executor::block_on(engine.sql(join_sql).expect("join sql").collect())
        .expect("join collect");
    let join_2 = futures::executor::block_on(engine.sql(join_sql).expect("join sql").collect())
        .expect("join collect");
    support::assert_batches_deterministic(
        &join_1,
        &join_2,
        &["l_orderkey", "l_partkey", "o_custkey"],
        1e-9,
    );

    let agg_1 = futures::executor::block_on(engine.sql(agg_sql).expect("agg sql").collect())
        .expect("agg collect");
    let agg_2 = futures::executor::block_on(engine.sql(agg_sql).expect("agg sql").collect())
        .expect("agg collect");
    support::assert_batches_deterministic(&agg_1, &agg_2, &["l_orderkey"], 1e-9);
}
