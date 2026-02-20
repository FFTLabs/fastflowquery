use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use ffq_client::Engine;
use ffq_common::EngineConfig;
use ffq_storage::TableDef;

#[path = "support/mod.rs"]
mod support;

fn make_engine_with_window_fixture() -> (Engine, std::path::PathBuf) {
    let path = support::unique_path("ffq_window_mvp", "parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("grp", DataType::Utf8, false),
        Field::new("ord", DataType::Int64, false),
        Field::new("score", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    support::write_parquet(
        &path,
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "A", "A", "B", "B"])),
            Arc::new(Int64Array::from(vec![1_i64, 2, 3, 1, 2])),
            Arc::new(Int64Array::from(vec![10_i64, 10, 20, 7, 9])),
            Arc::new(Int64Array::from(vec![2_i64, 3, 5, 1, 4])),
        ],
    );
    let engine = Engine::new(EngineConfig::default()).expect("engine");
    engine.register_table(
        "t",
        TableDef {
            name: "ignored".to_string(),
            uri: path.to_string_lossy().into_owned(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some((*schema).clone()),
            stats: ffq_storage::TableStats::default(),
            options: HashMap::new(),
        },
    );
    (engine, path)
}

fn make_engine_with_window_null_fixture() -> (Engine, std::path::PathBuf) {
    let path = support::unique_path("ffq_window_mvp_nulls", "parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("grp", DataType::Utf8, false),
        Field::new("ord", DataType::Int64, true),
        Field::new("score", DataType::Int64, false),
    ]));
    support::write_parquet(
        &path,
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "A", "A"])),
            Arc::new(Int64Array::from(vec![Some(3_i64), None, Some(1_i64)])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 30])),
        ],
    );
    let engine = Engine::new(EngineConfig::default()).expect("engine");
    engine.register_table(
        "t",
        TableDef {
            name: "ignored".to_string(),
            uri: path.to_string_lossy().into_owned(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some((*schema).clone()),
            stats: ffq_storage::TableStats::default(),
            options: HashMap::new(),
        },
    );
    (engine, path)
}

#[test]
fn row_number_over_partition_order_is_correct() {
    let (engine, path) = make_engine_with_window_fixture();
    let sql = "SELECT grp, ord, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY ord) AS rn FROM t";
    let batches =
        futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");

    let mut rows = Vec::new();
    for batch in &batches {
        let grp = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("grp");
        let ord = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ord");
        let rn = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("rn");
        for row in 0..batch.num_rows() {
            rows.push((grp.value(row).to_string(), ord.value(row), rn.value(row)));
        }
    }
    rows.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    assert_eq!(
        rows,
        vec![
            ("A".to_string(), 1, 1),
            ("A".to_string(), 2, 2),
            ("A".to_string(), 3, 3),
            ("B".to_string(), 1, 1),
            ("B".to_string(), 2, 2),
        ]
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn rank_over_partition_order_is_correct() {
    let (engine, path) = make_engine_with_window_fixture();
    let sql = "SELECT grp, ord, score, RANK() OVER (PARTITION BY grp ORDER BY score) AS rnk FROM t";
    let batches =
        futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");

    let mut rows = Vec::new();
    for batch in &batches {
        let grp = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("grp");
        let ord = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ord");
        let rnk = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("rnk");
        for row in 0..batch.num_rows() {
            rows.push((grp.value(row).to_string(), ord.value(row), rnk.value(row)));
        }
    }
    rows.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    assert_eq!(
        rows,
        vec![
            ("A".to_string(), 1, 1),
            ("A".to_string(), 2, 1),
            ("A".to_string(), 3, 3),
            ("B".to_string(), 1, 1),
            ("B".to_string(), 2, 2),
        ]
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn cumulative_sum_over_partition_order_is_correct() {
    let (engine, path) = make_engine_with_window_fixture();
    let sql = "SELECT grp, ord, SUM(v) OVER (PARTITION BY grp ORDER BY ord) AS running_sum FROM t";
    let batches =
        futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");

    let mut rows = Vec::new();
    for batch in &batches {
        let grp = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("grp");
        let ord = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ord");
        let running_sum = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("running_sum");
        for row in 0..batch.num_rows() {
            rows.push((
                grp.value(row).to_string(),
                ord.value(row),
                running_sum.value(row),
            ));
        }
    }
    rows.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    assert_eq!(
        rows,
        vec![
            ("A".to_string(), 1, 2.0),
            ("A".to_string(), 2, 5.0),
            ("A".to_string(), 3, 10.0),
            ("B".to_string(), 1, 1.0),
            ("B".to_string(), 2, 5.0),
        ]
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn named_window_desc_nulls_first_executes_correctly() {
    let (engine, path) = make_engine_with_window_null_fixture();
    let sql = "SELECT ord, ROW_NUMBER() OVER w AS rn FROM t WINDOW w AS (PARTITION BY grp ORDER BY ord DESC NULLS FIRST)";
    let batches =
        futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");

    let mut rows = Vec::new();
    for batch in &batches {
        let ord = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ord");
        let rn = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("rn");
        for row in 0..batch.num_rows() {
            let ord_v = if ord.is_null(row) {
                None
            } else {
                Some(ord.value(row))
            };
            rows.push((ord_v, rn.value(row)));
        }
    }
    rows.sort_unstable_by_key(|(_, rn)| *rn);
    assert_eq!(rows, vec![(None, 1), (Some(3), 2), (Some(1), 3)]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn expanded_window_functions_ranking_and_value_semantics() {
    let (engine, path) = make_engine_with_window_fixture();
    let sql = "SELECT grp, ord, score, \
                    DENSE_RANK() OVER (PARTITION BY grp ORDER BY score) AS dr, \
                    PERCENT_RANK() OVER (PARTITION BY grp ORDER BY score) AS pr, \
                    CUME_DIST() OVER (PARTITION BY grp ORDER BY score) AS cd, \
                    NTILE(2) OVER (PARTITION BY grp ORDER BY score) AS nt, \
                    LAG(score) OVER (PARTITION BY grp ORDER BY ord) AS lag_s, \
                    LEAD(score, 2, 999) OVER (PARTITION BY grp ORDER BY ord) AS lead_s, \
                    FIRST_VALUE(score) OVER (PARTITION BY grp ORDER BY ord) AS fv, \
                    LAST_VALUE(score) OVER (PARTITION BY grp ORDER BY ord ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv, \
                    NTH_VALUE(score, 2) OVER (PARTITION BY grp ORDER BY ord ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nv \
               FROM t";
    let batches =
        futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");

    #[derive(Debug, Clone, PartialEq)]
    struct Row {
        grp: String,
        ord: i64,
        score: i64,
        dr: i64,
        pr: f64,
        cd: f64,
        nt: i64,
        lag_s: Option<i64>,
        lead_s: i64,
        fv: i64,
        lv: i64,
        nv: i64,
    }

    let mut rows = Vec::new();
    for batch in &batches {
        let grp = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("grp");
        let ord = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ord");
        let score = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("score");
        let dr = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("dr");
        let pr = batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("pr");
        let cd = batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("cd");
        let nt = batch
            .column(6)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("nt");
        let lag_s = batch
            .column(7)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("lag_s");
        let lead_s = batch
            .column(8)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("lead_s");
        let fv = batch
            .column(9)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("fv");
        let lv = batch
            .column(10)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("lv");
        let nv = batch
            .column(11)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("nv");
        for i in 0..batch.num_rows() {
            rows.push(Row {
                grp: grp.value(i).to_string(),
                ord: ord.value(i),
                score: score.value(i),
                dr: dr.value(i),
                pr: pr.value(i),
                cd: cd.value(i),
                nt: nt.value(i),
                lag_s: if lag_s.is_null(i) {
                    None
                } else {
                    Some(lag_s.value(i))
                },
                lead_s: lead_s.value(i),
                fv: fv.value(i),
                lv: lv.value(i),
                nv: nv.value(i),
            });
        }
    }
    rows.sort_unstable_by(|a, b| a.grp.cmp(&b.grp).then(a.ord.cmp(&b.ord)));

    let expected = vec![
        Row {
            grp: "A".to_string(),
            ord: 1,
            score: 10,
            dr: 1,
            pr: 0.0,
            cd: 2.0 / 3.0,
            nt: 1,
            lag_s: None,
            lead_s: 20,
            fv: 10,
            lv: 20,
            nv: 10,
        },
        Row {
            grp: "A".to_string(),
            ord: 2,
            score: 10,
            dr: 1,
            pr: 0.0,
            cd: 2.0 / 3.0,
            nt: 1,
            lag_s: Some(10),
            lead_s: 999,
            fv: 10,
            lv: 20,
            nv: 10,
        },
        Row {
            grp: "A".to_string(),
            ord: 3,
            score: 20,
            dr: 2,
            pr: 1.0,
            cd: 1.0,
            nt: 2,
            lag_s: Some(10),
            lead_s: 999,
            fv: 10,
            lv: 20,
            nv: 10,
        },
        Row {
            grp: "B".to_string(),
            ord: 1,
            score: 7,
            dr: 1,
            pr: 0.0,
            cd: 0.5,
            nt: 1,
            lag_s: None,
            lead_s: 999,
            fv: 7,
            lv: 9,
            nv: 9,
        },
        Row {
            grp: "B".to_string(),
            ord: 2,
            score: 9,
            dr: 2,
            pr: 1.0,
            cd: 1.0,
            nt: 2,
            lag_s: Some(7),
            lead_s: 999,
            fv: 7,
            lv: 9,
            nv: 9,
        },
    ];

    assert_eq!(rows.len(), expected.len());
    for (actual, exp) in rows.iter().zip(expected.iter()) {
        assert_eq!(actual.grp, exp.grp);
        assert_eq!(actual.ord, exp.ord);
        assert_eq!(actual.score, exp.score);
        assert_eq!(actual.dr, exp.dr);
        assert!((actual.pr - exp.pr).abs() < 1e-9);
        assert!((actual.cd - exp.cd).abs() < 1e-9);
        assert_eq!(actual.nt, exp.nt);
        assert_eq!(actual.lag_s, exp.lag_s);
        assert_eq!(actual.lead_s, exp.lead_s);
        assert_eq!(actual.fv, exp.fv);
        assert_eq!(actual.lv, exp.lv);
        assert_eq!(actual.nv, exp.nv);
    }

    let _ = std::fs::remove_file(path);
}

#[test]
fn window_frames_rows_range_groups_are_correct() {
    let (engine, path) = make_engine_with_window_fixture();
    let sql = "SELECT grp, ord, score, \
                    SUM(score) OVER (PARTITION BY grp ORDER BY ord ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s_rows, \
                    SUM(score) OVER (PARTITION BY grp ORDER BY ord RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS s_range, \
                    SUM(score) OVER (PARTITION BY grp ORDER BY score GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS s_groups \
               FROM t";
    let batches =
        futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");

    #[derive(Debug)]
    struct Row {
        grp: String,
        ord: i64,
        s_rows: f64,
        s_range: f64,
        s_groups: f64,
    }
    let mut rows = Vec::new();
    for batch in &batches {
        let grp = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("grp");
        let ord = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ord");
        let s_rows = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("s_rows");
        let s_range = batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("s_range");
        let s_groups = batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("s_groups");
        for i in 0..batch.num_rows() {
            rows.push(Row {
                grp: grp.value(i).to_string(),
                ord: ord.value(i),
                s_rows: s_rows.value(i),
                s_range: s_range.value(i),
                s_groups: s_groups.value(i),
            });
        }
    }
    rows.sort_unstable_by(|a, b| a.grp.cmp(&b.grp).then(a.ord.cmp(&b.ord)));

    let expected = [
        ("A", 1, 20.0, 10.0, 40.0),
        ("A", 2, 40.0, 20.0, 40.0),
        ("A", 3, 30.0, 30.0, 20.0),
        ("B", 1, 16.0, 7.0, 16.0),
        ("B", 2, 16.0, 16.0, 9.0),
    ];
    for (actual, exp) in rows.iter().zip(expected.iter()) {
        assert_eq!(actual.grp, exp.0);
        assert_eq!(actual.ord, exp.1);
        assert!((actual.s_rows - exp.2).abs() < 1e-9);
        assert!((actual.s_range - exp.3).abs() < 1e-9);
        assert!((actual.s_groups - exp.4).abs() < 1e-9);
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn aggregate_window_functions_count_avg_min_max_are_correct() {
    let (engine, path) = make_engine_with_window_fixture();
    let sql = "SELECT grp, ord, score, \
                    COUNT(score) OVER (PARTITION BY grp ORDER BY ord) AS cnt, \
                    AVG(score) OVER (PARTITION BY grp ORDER BY ord) AS avg_s, \
                    MIN(score) OVER (PARTITION BY grp ORDER BY ord) AS min_s, \
                    MAX(score) OVER (PARTITION BY grp ORDER BY ord) AS max_s \
               FROM t";
    let batches =
        futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");

    #[derive(Debug, Clone, PartialEq)]
    struct Row {
        grp: String,
        ord: i64,
        cnt: i64,
        avg_s: f64,
        min_s: i64,
        max_s: i64,
    }
    let mut rows = Vec::new();
    for batch in &batches {
        let grp = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("grp");
        let ord = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ord");
        let cnt = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("cnt");
        let avg_s = batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("avg_s");
        let min_s = batch
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("min_s");
        let max_s = batch
            .column(6)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("max_s");
        for i in 0..batch.num_rows() {
            rows.push(Row {
                grp: grp.value(i).to_string(),
                ord: ord.value(i),
                cnt: cnt.value(i),
                avg_s: avg_s.value(i),
                min_s: min_s.value(i),
                max_s: max_s.value(i),
            });
        }
    }
    rows.sort_unstable_by(|a, b| a.grp.cmp(&b.grp).then(a.ord.cmp(&b.ord)));

    let expected = vec![
        Row {
            grp: "A".to_string(),
            ord: 1,
            cnt: 1,
            avg_s: 10.0,
            min_s: 10,
            max_s: 10,
        },
        Row {
            grp: "A".to_string(),
            ord: 2,
            cnt: 2,
            avg_s: 10.0,
            min_s: 10,
            max_s: 10,
        },
        Row {
            grp: "A".to_string(),
            ord: 3,
            cnt: 3,
            avg_s: 40.0 / 3.0,
            min_s: 10,
            max_s: 20,
        },
        Row {
            grp: "B".to_string(),
            ord: 1,
            cnt: 1,
            avg_s: 7.0,
            min_s: 7,
            max_s: 7,
        },
        Row {
            grp: "B".to_string(),
            ord: 2,
            cnt: 2,
            avg_s: 8.0,
            min_s: 7,
            max_s: 9,
        },
    ];

    assert_eq!(rows.len(), expected.len());
    for (actual, exp) in rows.iter().zip(expected.iter()) {
        assert_eq!(actual.grp, exp.grp);
        assert_eq!(actual.ord, exp.ord);
        assert_eq!(actual.cnt, exp.cnt);
        assert!((actual.avg_s - exp.avg_s).abs() < 1e-9);
        assert_eq!(actual.min_s, exp.min_s);
        assert_eq!(actual.max_s, exp.max_s);
    }

    let _ = std::fs::remove_file(path);
}

#[test]
fn frame_exclusion_semantics_apply_in_sql_queries() {
    let (engine, path) = make_engine_with_window_fixture();
    let sql = "SELECT grp, ord, \
                    SUM(score) OVER (PARTITION BY grp ORDER BY score ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) AS s_cur, \
                    SUM(score) OVER (PARTITION BY grp ORDER BY score ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS s_group, \
                    SUM(score) OVER (PARTITION BY grp ORDER BY score ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) AS s_ties, \
                    RANK() OVER (PARTITION BY grp ORDER BY score ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP) AS rnk \
               FROM t";
    let batches =
        futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");

    let mut rows = Vec::new();
    for batch in &batches {
        let grp = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("grp");
        let ord = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ord");
        let s_cur = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("s_cur");
        let s_group = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("s_group");
        let s_ties = batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("s_ties");
        let rnk = batch
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("rnk");
        for i in 0..batch.num_rows() {
            rows.push((
                grp.value(i).to_string(),
                ord.value(i),
                s_cur.value(i),
                s_group.value(i),
                s_ties.value(i),
                rnk.value(i),
            ));
        }
    }

    rows.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    assert_eq!(
        rows,
        vec![
            ("A".to_string(), 1, 30.0, 20.0, 30.0, 1),
            ("A".to_string(), 2, 30.0, 20.0, 30.0, 1),
            ("A".to_string(), 3, 20.0, 20.0, 40.0, 3),
            ("B".to_string(), 1, 9.0, 9.0, 16.0, 1),
            ("B".to_string(), 2, 7.0, 7.0, 16.0, 2),
        ]
    );

    let _ = std::fs::remove_file(path);
}

#[test]
fn window_output_types_and_nullability_follow_rules() {
    let (engine, path) = make_engine_with_window_fixture();
    let sql = "SELECT \
                    ROW_NUMBER() OVER (PARTITION BY grp ORDER BY ord) AS rn, \
                    COUNT(score) OVER (PARTITION BY grp ORDER BY ord) AS cnt, \
                    PERCENT_RANK() OVER (PARTITION BY grp ORDER BY ord) AS pr, \
                    SUM(score) OVER (PARTITION BY grp ORDER BY ord) AS s, \
                    LAG(score, 1, 0.5) OVER (PARTITION BY grp ORDER BY ord) AS lg \
               FROM t";
    let batches =
        futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");
    let schema = batches[0].schema();

    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert!(!schema.field(0).is_nullable());

    assert_eq!(schema.field(1).data_type(), &DataType::Int64);
    assert!(!schema.field(1).is_nullable());

    assert_eq!(schema.field(2).data_type(), &DataType::Float64);
    assert!(!schema.field(2).is_nullable());

    assert_eq!(schema.field(3).data_type(), &DataType::Float64);
    assert!(schema.field(3).is_nullable());

    assert_eq!(schema.field(4).data_type(), &DataType::Float64);
    assert!(schema.field(4).is_nullable());

    let _ = std::fs::remove_file(path);
}

#[test]
fn window_null_ordering_truth_table_is_honored() {
    let (engine, path) = make_engine_with_window_null_fixture();
    let sql = "SELECT ord, \
                    ROW_NUMBER() OVER (ORDER BY ord ASC NULLS FIRST) AS rn_af, \
                    ROW_NUMBER() OVER (ORDER BY ord ASC NULLS LAST) AS rn_al, \
                    ROW_NUMBER() OVER (ORDER BY ord DESC NULLS FIRST) AS rn_df, \
                    ROW_NUMBER() OVER (ORDER BY ord DESC NULLS LAST) AS rn_dl \
               FROM t";
    let batches =
        futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");

    let mut rows = Vec::new();
    for batch in &batches {
        let ord = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ord");
        let rn_af = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("rn_af");
        let rn_al = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("rn_al");
        let rn_df = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("rn_df");
        let rn_dl = batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("rn_dl");
        for i in 0..batch.num_rows() {
            rows.push((
                if ord.is_null(i) {
                    None
                } else {
                    Some(ord.value(i))
                },
                rn_af.value(i),
                rn_al.value(i),
                rn_df.value(i),
                rn_dl.value(i),
            ));
        }
    }
    rows.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(
        rows,
        vec![
            (None, 1, 3, 1, 3),
            (Some(1), 2, 1, 3, 2),
            (Some(3), 3, 2, 2, 1),
        ]
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn window_tie_ordering_is_deterministic_across_runs() {
    let (engine, path) = make_engine_with_window_fixture();
    let sql = "SELECT grp, ord, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score) AS rn FROM t";

    let run_once = |engine: &Engine| {
        let batches =
            futures::executor::block_on(engine.sql(sql).expect("sql").collect()).expect("collect");
        let mut rows = Vec::new();
        for batch in &batches {
            let grp = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("grp");
            let ord = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("ord");
            let rn = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("rn");
            for i in 0..batch.num_rows() {
                rows.push((grp.value(i).to_string(), ord.value(i), rn.value(i)));
            }
        }
        rows.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        rows
    };

    let first = run_once(&engine);
    let second = run_once(&engine);
    assert_eq!(first, second);
    assert_eq!(first.len(), 5);
    let a1 = first
        .iter()
        .find(|(g, o, _)| g == "A" && *o == 1)
        .expect("A/1");
    let a2 = first
        .iter()
        .find(|(g, o, _)| g == "A" && *o == 2)
        .expect("A/2");
    let a3 = first
        .iter()
        .find(|(g, o, _)| g == "A" && *o == 3)
        .expect("A/3");
    let b1 = first
        .iter()
        .find(|(g, o, _)| g == "B" && *o == 1)
        .expect("B/1");
    let b2 = first
        .iter()
        .find(|(g, o, _)| g == "B" && *o == 2)
        .expect("B/2");
    assert!(a1.2 == 1 || a1.2 == 2);
    assert!(a2.2 == 1 || a2.2 == 2);
    assert_ne!(a1.2, a2.2);
    assert_eq!(a3.2, 3);
    assert_eq!(b1.2, 1);
    assert_eq!(b2.2, 2);

    let _ = std::fs::remove_file(path);
}
