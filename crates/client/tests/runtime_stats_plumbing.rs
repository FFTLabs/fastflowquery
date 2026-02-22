use std::sync::Arc;

use arrow::array::Int64Array;
use arrow_schema::{DataType, Field, Schema};
use ffq_client::Engine;
use ffq_common::EngineConfig;
use ffq_storage::TableStats;

#[path = "support/mod.rs"]
mod support;

#[test]
fn collect_populates_stage_and_operator_stats_report() {
    let engine = Engine::new(EngineConfig::default()).expect("engine");
    let path = support::unique_path("ffq_runtime_stats", "parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    support::write_parquet(
        &path,
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 1, 2, 3])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 30, 40])),
        ],
    );
    support::register_parquet_table(
        &engine,
        "t",
        &path,
        (*schema).clone(),
        TableStats::default(),
    );

    let df = engine
        .sql("SELECT k, SUM(v) AS s FROM t GROUP BY k")
        .expect("sql");
    let _batches = futures::executor::block_on(df.collect()).expect("collect");

    let report = engine
        .last_query_stats_report()
        .expect("runtime stats report must exist");
    assert!(report.contains("query_id="), "{report}");
    assert!(report.contains("stages:"), "{report}");
    assert!(report.contains("operators:"), "{report}");
    assert!(report.contains("stage=0"), "{report}");
    assert!(
        report.contains("HashAggregate")
            || report.contains("FinalHashAggregate")
            || report.contains("PartialHashAggregate"),
        "{report}"
    );

    let _ = std::fs::remove_file(path);
}
