use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use ffq_client::Engine;
use ffq_common::EngineConfig;
use ffq_storage::TableDef;

#[path = "support/mod.rs"]
mod support;

fn build_engine() -> (Engine, Vec<std::path::PathBuf>) {
    let engine = Engine::new(EngineConfig::default()).expect("engine");
    let w_path = support::unique_path("ffq_window_matrix_w", "parquet");
    let o_path = support::unique_path("ffq_window_matrix_orders", "parquet");

    let w_schema = Arc::new(Schema::new(vec![
        Field::new("grp", DataType::Utf8, false),
        Field::new("ord", DataType::Int64, false),
        Field::new("score", DataType::Int64, true),
        Field::new("v", DataType::Int64, false),
    ]));
    support::write_parquet(
        &w_path,
        w_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                "A", "A", "A", "A", "B", "B", "B", "B",
            ])),
            Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 1, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![
                Some(10_i64),
                Some(10),
                None,
                Some(20),
                None,
                Some(5),
                Some(5),
                Some(8),
            ])),
            Arc::new(Int64Array::from(vec![2_i64, 3, 4, 5, 1, 2, 3, 4])),
        ],
    );
    engine.register_table(
        "w",
        TableDef {
            name: "w".to_string(),
            uri: w_path.to_string_lossy().into_owned(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some((*w_schema).clone()),
            stats: ffq_storage::TableStats::default(),
            options: HashMap::new(),
        },
    );

    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
    ]));
    support::write_parquet(
        &o_path,
        orders_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![100_i64, 200, 300, 400])),
        ],
    );
    engine.register_table(
        "orders",
        TableDef {
            name: "orders".to_string(),
            uri: o_path.to_string_lossy().into_owned(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some((*orders_schema).clone()),
            stats: ffq_storage::TableStats::default(),
            options: HashMap::new(),
        },
    );

    (engine, vec![w_path, o_path])
}

#[test]
fn embedded_window_correctness_edge_matrix_snapshot() {
    let (engine, paths) = build_engine();

    let cases = vec![
        (
            "ranking_nulls_ties",
            "SELECT grp, ord, score, \
                ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score ASC NULLS LAST) AS rn, \
                RANK() OVER (PARTITION BY grp ORDER BY score ASC NULLS LAST) AS rnk, \
                DENSE_RANK() OVER (PARTITION BY grp ORDER BY score ASC NULLS LAST) AS dr \
             FROM w",
            vec!["grp", "ord"],
        ),
        (
            "frames_rows_range_groups",
            "SELECT grp, ord, score, \
                SUM(v) OVER (PARTITION BY grp ORDER BY ord ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s_rows, \
                SUM(v) OVER (PARTITION BY grp ORDER BY ord RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s_range, \
                SUM(v) OVER (PARTITION BY grp ORDER BY score GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS s_groups \
             FROM w",
            vec!["grp", "ord"],
        ),
        (
            "offsets_and_value_windows",
            "SELECT grp, ord, score, \
                LAG(score, 1, 999) OVER (PARTITION BY grp ORDER BY ord) AS lag_s, \
                LEAD(score, 2, 111) OVER (PARTITION BY grp ORDER BY ord) AS lead_s, \
                FIRST_VALUE(score) OVER (PARTITION BY grp ORDER BY ord) AS fv, \
                LAST_VALUE(score) OVER (PARTITION BY grp ORDER BY ord ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv, \
                NTH_VALUE(score, 2) OVER (PARTITION BY grp ORDER BY ord ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nv2 \
             FROM w",
            vec!["grp", "ord"],
        ),
        (
            "exclusion_modes",
            "SELECT grp, ord, \
                SUM(v) OVER (PARTITION BY grp ORDER BY ord ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE NO OTHERS) AS s_all, \
                SUM(v) OVER (PARTITION BY grp ORDER BY ord ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) AS s_cur, \
                SUM(v) OVER (PARTITION BY grp ORDER BY score ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS s_group, \
                SUM(v) OVER (PARTITION BY grp ORDER BY score ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) AS s_ties \
             FROM w",
            vec!["grp", "ord"],
        ),
        (
            "mixed_window_join_filter",
            "SELECT w.grp, w.ord, o.o_custkey, \
                ROW_NUMBER() OVER (PARTITION BY w.grp ORDER BY w.ord) AS rn, \
                SUM(w.v) OVER (PARTITION BY w.grp ORDER BY w.ord ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum \
             FROM w \
             JOIN orders o ON w.ord = o.o_orderkey \
             WHERE w.v >= 2",
            vec!["grp", "ord", "o_custkey"],
        ),
    ];

    let mut snapshot = String::new();
    for (name, sql, sort_by) in cases {
        let batches =
            futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");
        snapshot.push_str(&format!("## {name}\n"));
        snapshot.push_str(&support::snapshot_text(&batches, &sort_by, 1e-9));
        snapshot.push('\n');
    }

    support::assert_or_bless_snapshot(
        "tests/snapshots/window/embedded_window_edge_matrix.snap",
        &snapshot,
    );

    for p in paths {
        let _ = std::fs::remove_file(p);
    }
}
