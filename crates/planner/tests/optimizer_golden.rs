use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use ffq_planner::{
    explain_logical, BinaryOp, Expr, JoinStrategyHint, LogicalPlan, Optimizer, OptimizerConfig,
    OptimizerContext, SchemaProvider, TableMetadata,
};

#[derive(Clone)]
struct TestTable {
    schema: SchemaRef,
    stats: (Option<u64>, Option<u64>),
    meta: TableMetadata,
}

#[derive(Default)]
struct TestCtx {
    tables: HashMap<String, TestTable>,
}

impl TestCtx {
    fn with_table(
        mut self,
        name: &str,
        schema: Schema,
        stats: (Option<u64>, Option<u64>),
        format: &str,
        options: HashMap<String, String>,
    ) -> Self {
        self.tables.insert(
            name.to_string(),
            TestTable {
                schema: Arc::new(schema),
                stats,
                meta: TableMetadata {
                    format: format.to_string(),
                    options,
                },
            },
        );
        self
    }
}

impl SchemaProvider for TestCtx {
    fn table_schema(&self, table: &str) -> ffq_common::Result<SchemaRef> {
        self.tables
            .get(table)
            .map(|t| Arc::clone(&t.schema))
            .ok_or_else(|| ffq_common::FfqError::Planning(format!("unknown table: {table}")))
    }
}

impl OptimizerContext for TestCtx {
    fn table_stats(&self, table: &str) -> ffq_common::Result<(Option<u64>, Option<u64>)> {
        self.tables
            .get(table)
            .map(|t| t.stats)
            .ok_or_else(|| ffq_common::FfqError::Planning(format!("unknown table: {table}")))
    }

    fn table_metadata(&self, table: &str) -> ffq_common::Result<Option<TableMetadata>> {
        Ok(self.tables.get(table).map(|t| t.meta.clone()))
    }
}

fn optimizer_snapshot(name: &str, plan: LogicalPlan, ctx: &TestCtx) {
    let before = explain_logical(&plan);
    let after = explain_logical(
        &Optimizer::new()
            .optimize(plan, ctx, OptimizerConfig::default())
            .expect("optimize"),
    );
    let snapshot = format!(
        "# optimizer-golden: {name}\n\n## before\n{before}\n## after\n{after}"
    );

    let path = snapshot_path(name);
    if should_bless() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create snapshot dir");
        }
        fs::write(&path, snapshot).expect("write snapshot");
        return;
    }

    let expected = fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!(
            "missing snapshot at {}. Run with BLESS=1 to create it.",
            path.display()
        )
    });
    if expected != snapshot {
        panic!(
            "snapshot mismatch for {name}\npath: {}\n\n{}\n\nRun with BLESS=1 to accept changes.",
            path.display(),
            unified_diff(&expected, &snapshot)
        );
    }
}

fn should_bless() -> bool {
    matches!(std::env::var("BLESS").as_deref(), Ok("1"))
        || matches!(std::env::var("UPDATE_SNAPSHOTS").as_deref(), Ok("1"))
}

fn snapshot_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("snapshots")
        .join("optimizer")
        .join(format!("{name}.snap"))
}

fn unified_diff(expected: &str, actual: &str) -> String {
    let exp: Vec<&str> = expected.lines().collect();
    let act: Vec<&str> = actual.lines().collect();
    let mut out = String::new();
    out.push_str("--- expected\n+++ actual\n");
    let max = exp.len().max(act.len());
    for i in 0..max {
        match (exp.get(i), act.get(i)) {
            (Some(e), Some(a)) if e == a => {
                out.push_str(&format!(" {:04} {e}\n", i + 1));
            }
            (Some(e), Some(a)) => {
                out.push_str(&format!("-{:04} {e}\n", i + 1));
                out.push_str(&format!("+{:04} {a}\n", i + 1));
            }
            (Some(e), None) => out.push_str(&format!("-{:04} {e}\n", i + 1)),
            (None, Some(a)) => out.push_str(&format!("+{:04} {a}\n", i + 1)),
            (None, None) => {}
        }
    }
    out
}

fn test_ctx() -> TestCtx {
    let t_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]);
    let big_schema = Schema::new(vec![Field::new("k2", DataType::Int64, false)]);
    let small_schema = Schema::new(vec![Field::new("k", DataType::Int64, false)]);
    TestCtx::default()
        .with_table(
            "t",
            t_schema,
            (Some(1024), Some(100)),
            "parquet",
            HashMap::new(),
        )
        .with_table(
            "big",
            big_schema,
            (Some(10_000_000), Some(2_000_000)),
            "parquet",
            HashMap::new(),
        )
        .with_table(
            "small",
            small_schema,
            (Some(100), Some(2)),
            "parquet",
            HashMap::new(),
        )
}

#[test]
fn golden_filter_merge_and_projection_pushdown() {
    let plan = LogicalPlan::Projection {
        exprs: vec![(Expr::Column("id".to_string()), "id".to_string())],
        input: Box::new(LogicalPlan::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Literal(ffq_planner::LiteralValue::Int64(10))),
            },
            input: Box::new(LogicalPlan::Filter {
                predicate: Expr::BinaryOp {
                    left: Box::new(Expr::Column("v".to_string())),
                    op: BinaryOp::Eq,
                    right: Box::new(Expr::Literal(ffq_planner::LiteralValue::Int64(1))),
                },
                input: Box::new(LogicalPlan::TableScan {
                    table: "t".to_string(),
                    projection: None,
                    filters: vec![],
                }),
            }),
        }),
    };
    optimizer_snapshot("filter_merge_and_projection_pushdown", plan, &test_ctx());
}

#[test]
fn golden_join_strategy_hint_broadcast_left() {
    let plan = LogicalPlan::Join {
        left: Box::new(LogicalPlan::TableScan {
            table: "small".to_string(),
            projection: None,
            filters: vec![],
        }),
        right: Box::new(LogicalPlan::TableScan {
            table: "big".to_string(),
            projection: None,
            filters: vec![],
        }),
        on: vec![("k".to_string(), "k2".to_string())],
        join_type: ffq_planner::JoinType::Inner,
        strategy_hint: JoinStrategyHint::Auto,
    };
    optimizer_snapshot("join_strategy_hint_broadcast_left", plan, &test_ctx());
}
