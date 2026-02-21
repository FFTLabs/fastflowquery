use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use ffq_client::Engine;
use ffq_client::expr::col;
use ffq_common::EngineConfig;
use ffq_planner::AggExpr;
use ffq_storage::TableDef;
#[path = "support/mod.rs"]
mod support;

fn register_src_table(engine: &Engine, path: &std::path::Path, schema: &Schema) {
    engine.register_table(
        "t",
        TableDef {
            name: "ignored".to_string(),
            uri: path.to_string_lossy().into_owned(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some(schema.clone()),
            stats: ffq_storage::TableStats::default(),
            options: HashMap::new(),
        },
    );
}

fn run_grouped_aggs(engine: &Engine) -> Vec<arrow::record_batch::RecordBatch> {
    let df = engine
        .table("t")
        .expect("table")
        .groupby(vec![col("k")])
        .agg(vec![
            (AggExpr::Count(col("v")), "cnt".to_string()),
            (AggExpr::Sum(col("v")), "sum_v".to_string()),
            (AggExpr::Min(col("v")), "min_v".to_string()),
            (AggExpr::Max(col("v")), "max_v".to_string()),
            (AggExpr::Avg(col("v")), "avg_v".to_string()),
        ]);
    futures::executor::block_on(df.collect()).expect("collect")
}

#[test]
fn hash_aggregate_deterministic_with_spill_and_non_spill_parity() {
    let parquet_path = support::unique_path("ffq_hash_agg", "parquet");
    let spill_dir_low = support::unique_path("ffq_hash_agg_spill_low", "dir");
    let spill_dir_high = support::unique_path("ffq_hash_agg_spill_high", "dir");

    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]));

    let mut keys = Vec::new();
    let mut values = Vec::new();
    for g in 0_i64..7_i64 {
        for j in 0_i64..120_i64 {
            keys.push(format!("group_{g}"));
            values.push(((j * (g + 3)) % 23) - 7);
        }
    }

    support::write_parquet(
        &parquet_path,
        schema.clone(),
        vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(Int64Array::from(values)),
        ],
    );

    let mut low_mem_cfg = EngineConfig::default();
    low_mem_cfg.mem_budget_bytes = 512;
    low_mem_cfg.spill_dir = spill_dir_low.to_string_lossy().into_owned();

    let mut high_mem_cfg = EngineConfig::default();
    high_mem_cfg.mem_budget_bytes = 256 * 1024 * 1024;
    high_mem_cfg.spill_dir = spill_dir_high.to_string_lossy().into_owned();

    let spill_engine = Engine::new(low_mem_cfg).expect("spill engine");
    let non_spill_engine = Engine::new(high_mem_cfg).expect("non-spill engine");
    register_src_table(&spill_engine, &parquet_path, schema.as_ref());
    register_src_table(&non_spill_engine, &parquet_path, schema.as_ref());

    let spill_batches = run_grouped_aggs(&spill_engine);
    let spill_batches_again = run_grouped_aggs(&spill_engine);
    support::assert_batches_deterministic(&spill_batches, &spill_batches_again, &["k"], 1e-9);

    let non_spill_batches = run_grouped_aggs(&non_spill_engine);
    let non_spill_batches_again = run_grouped_aggs(&non_spill_engine);
    support::assert_batches_deterministic(
        &non_spill_batches,
        &non_spill_batches_again,
        &["k"],
        1e-9,
    );

    support::assert_batches_deterministic(&spill_batches, &non_spill_batches, &["k"], 1e-9);

    let snapshot = support::snapshot_text(&spill_batches, &["k"], 1e-9);
    support::assert_or_bless_snapshot(
        "tests/snapshots/aggregate/hash_aggregate_count_sum_min_max_avg.snap",
        &snapshot,
    );

    assert_eq!(spill_batches.len(), 1);
    let batch = &spill_batches[0];
    let expected_schema = Schema::new(vec![
        Field::new("k", DataType::Utf8, true),
        Field::new("cnt", DataType::Int64, true),
        Field::new("sum_v", DataType::Int64, true),
        Field::new("min_v", DataType::Int64, true),
        Field::new("max_v", DataType::Int64, true),
        Field::new("avg_v", DataType::Float64, true),
    ]);
    support::assert_schema_eq(batch.schema().as_ref(), &expected_schema);

    let k = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("k");
    let cnt = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("cnt");
    let sum_v = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sum_v");
    let min_v = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("min_v");
    let max_v = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("max_v");
    let avg_v = batch
        .column(5)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("avg_v");

    let mut groups = Vec::new();
    let mut seen = 0_i64;
    for row in 0..batch.num_rows() {
        let group = k.value(row);
        groups.push(group.to_string());
        assert!(group.starts_with("group_"));
        assert_eq!(cnt.value(row), 120);
        assert!(min_v.value(row) <= max_v.value(row));
        let cnt_f = cnt.value(row) as f64;
        let avg_expected = sum_v.value(row) as f64 / cnt_f;
        assert!((avg_v.value(row) - avg_expected).abs() < 1e-9);
        seen += cnt.value(row);
    }
    groups.sort();
    assert_eq!(
        groups,
        vec![
            "group_0", "group_1", "group_2", "group_3", "group_4", "group_5", "group_6"
        ]
    );
    assert_eq!(seen, 840);

    let _ = std::fs::remove_file(parquet_path);
    let _ = std::fs::remove_dir_all(spill_dir_low);
    let _ = std::fs::remove_dir_all(spill_dir_high);
}

#[test]
fn grouped_sum_float64_regression_repro_should_match_expected_groups() {
    let parquet_path = support::unique_path("ffq_hash_agg_grouped_sum_float64", "parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Utf8, false),
    ]));
    support::write_parquet(
        &parquet_path,
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 5])),
            Arc::new(Float64Array::from(vec![1.25_f64, 2.00, 3.75, 4.00, 5.50])),
            Arc::new(Float64Array::from(vec![10.0_f64, 20.0, 30.0, 40.0, 50.0])),
            Arc::new(Float64Array::from(vec![0.0_f64, 0.0, 0.0, 0.0, 0.0])),
            Arc::new(Float64Array::from(vec![0.0_f64, 0.0, 0.0, 0.0, 0.0])),
            Arc::new(StringArray::from(vec!["N", "N", "R", "N", "R"])),
            Arc::new(StringArray::from(vec!["O", "F", "O", "F", "O"])),
            Arc::new(StringArray::from(vec![
                "1998-01-01",
                "1998-01-02",
                "1998-01-03",
                "1998-01-04",
                "1998-01-05",
            ])),
        ],
    );

    let engine = Engine::new(EngineConfig::default()).expect("engine");
    register_src_table(&engine, &parquet_path, schema.as_ref());

    let batches = futures::executor::block_on(
        engine
            .sql(
                "SELECT l_linestatus, SUM(l_quantity) AS sum_qty \
                 FROM t GROUP BY l_linestatus",
            )
            .expect("sql")
            .collect(),
    )
    .expect("collect");

    let snapshot = support::snapshot_text(&batches, &["l_linestatus"], 1e-9);
    let expected = "\
schema:l_linestatus:Utf8:true,sum_qty:Float64:true\n\
rows:\n\
l_linestatus=F|sum_qty=6.000000000000\n\
l_linestatus=O|sum_qty=10.500000000000\n";
    assert_eq!(snapshot, expected);

    let _ = std::fs::remove_file(parquet_path);
}

#[test]
fn count_distinct_grouped_is_correct_and_spill_stable() {
    let parquet_path = support::unique_path("ffq_hash_agg_count_distinct", "parquet");
    let spill_dir = support::unique_path("ffq_hash_agg_count_distinct_spill", "dir");
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("v", DataType::Int64, true),
    ]));
    support::write_parquet(
        &parquet_path,
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                "a", "a", "a", "a", "b", "b", "b", "b",
            ])),
            Arc::new(Int64Array::from(vec![
                Some(1_i64),
                Some(1),
                Some(2),
                None,
                Some(3),
                Some(3),
                Some(4),
                None,
            ])),
        ],
    );

    let mut cfg = EngineConfig::default();
    cfg.mem_budget_bytes = 128;
    cfg.spill_dir = spill_dir.to_string_lossy().into_owned();
    let engine = Engine::new(cfg).expect("engine");
    register_src_table(&engine, &parquet_path, schema.as_ref());

    let batches = futures::executor::block_on(
        engine
            .sql("SELECT k, COUNT(DISTINCT v) AS cd FROM t GROUP BY k")
            .expect("sql")
            .collect(),
    )
    .expect("collect");
    let batches_again = futures::executor::block_on(
        engine
            .sql("SELECT k, COUNT(DISTINCT v) AS cd FROM t GROUP BY k")
            .expect("sql")
            .collect(),
    )
    .expect("collect");
    support::assert_batches_deterministic(&batches, &batches_again, &["k"], 1e-9);
    let snapshot = support::snapshot_text(&batches, &["k"], 1e-9);
    let expected = "\
schema:k:Utf8:true,cd:Int64:true\n\
rows:\n\
k=a|cd=2\n\
k=b|cd=2\n";
    assert_eq!(snapshot, expected);

    let _ = std::fs::remove_file(parquet_path);
    let _ = std::fs::remove_dir_all(spill_dir);
}
