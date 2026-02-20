use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow_schema::{DataType, Field, Schema};
use ffq_client::Engine;
use ffq_common::EngineConfig;
use ffq_storage::TableDef;

#[path = "support/mod.rs"]
mod support;

fn register_int64_table(
    engine: &Engine,
    name: &str,
    path: &std::path::Path,
    values: Vec<Option<i64>>,
) {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
    support::write_parquet(path, schema.clone(), vec![Arc::new(Int64Array::from(values))]);
    engine.register_table(
        name,
        TableDef {
            name: name.to_string(),
            uri: path.to_string_lossy().into_owned(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some((*schema).clone()),
            stats: ffq_storage::TableStats::default(),
            options: HashMap::new(),
        },
    );
}

fn build_engine() -> (Engine, Vec<std::path::PathBuf>) {
    let engine = Engine::new(EngineConfig::default()).expect("engine");
    let t_path = support::unique_path("ffq_subquery_matrix_t", "parquet");
    let s_path = support::unique_path("ffq_subquery_matrix_s", "parquet");
    let u_path = support::unique_path("ffq_subquery_matrix_u", "parquet");
    let e_path = support::unique_path("ffq_subquery_matrix_e", "parquet");
    let an_path = support::unique_path("ffq_subquery_matrix_an", "parquet");

    register_int64_table(&engine, "t", &t_path, vec![Some(1), Some(2), Some(3), None]);
    register_int64_table(&engine, "s", &s_path, vec![Some(2), None, Some(3), Some(2)]);
    register_int64_table(&engine, "u", &u_path, vec![Some(2), None]);
    register_int64_table(&engine, "e", &e_path, Vec::<Option<i64>>::new());
    register_int64_table(&engine, "allnull", &an_path, vec![None, None]);

    (engine, vec![t_path, s_path, u_path, e_path, an_path])
}

#[test]
fn embedded_subquery_cte_edge_matrix_snapshot() {
    let (engine, paths) = build_engine();

    let cases = vec![
        (
            "nested_in_subquery",
            "SELECT k FROM t WHERE k IN (SELECT k FROM s WHERE k IN (SELECT k FROM u))",
            vec!["k"],
        ),
        (
            "nested_scalar_subquery",
            "SELECT k FROM t
             WHERE k IN (
                 SELECT k FROM s
                 WHERE k > (
                     SELECT max(k) FROM u WHERE k IS NOT NULL
                 )
             )",
            vec!["k"],
        ),
        (
            "mixed_cte_plus_subquery",
            "WITH base AS (
                SELECT k FROM t WHERE k IS NOT NULL
            ),
            picked AS (
                SELECT k FROM base WHERE EXISTS (SELECT k FROM s WHERE s.k = base.k)
            )
            SELECT k FROM picked WHERE k IN (SELECT k FROM u WHERE k IS NOT NULL)",
            vec!["k"],
        ),
        (
            "not_in_null_rhs_pitfall",
            "SELECT k FROM t WHERE k NOT IN (SELECT k FROM s)",
            vec!["k"],
        ),
        (
            "not_in_empty_rhs",
            "SELECT k FROM t WHERE k NOT IN (SELECT k FROM e)",
            vec!["k"],
        ),
        (
            "in_empty_rhs",
            "SELECT k FROM t WHERE k IN (SELECT k FROM e)",
            vec!["k"],
        ),
        (
            "in_all_null_rhs",
            "SELECT k FROM t WHERE k IN (SELECT k FROM allnull)",
            vec!["k"],
        ),
        (
            "not_in_all_null_rhs",
            "SELECT k FROM t WHERE k NOT IN (SELECT k FROM allnull)",
            vec!["k"],
        ),
    ];

    let mut snapshot = String::new();
    for (name, sql, sort_by) in cases {
        let batches = futures::executor::block_on(engine.sql(sql).expect("sql").collect())
            .expect("collect");
        snapshot.push_str(&format!("## {name}\n"));
        snapshot.push_str(&support::snapshot_text(&batches, &sort_by, 1e-9));
        snapshot.push('\n');
    }

    support::assert_or_bless_snapshot(
        "tests/snapshots/subquery/embedded_cte_subquery_edge_matrix.snap",
        &snapshot,
    );

    for p in paths {
        let _ = std::fs::remove_file(p);
    }
}
