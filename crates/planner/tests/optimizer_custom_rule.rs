use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use ffq_planner::{
    BinaryOp, Expr, LogicalPlan, Optimizer, OptimizerConfig, OptimizerContext, OptimizerRule,
    SchemaProvider,
};

struct TestCtx {
    schema: SchemaRef,
}

impl SchemaProvider for TestCtx {
    fn table_schema(&self, _table: &str) -> ffq_common::Result<SchemaRef> {
        Ok(Arc::clone(&self.schema))
    }
}

impl OptimizerContext for TestCtx {
    fn table_stats(&self, _table: &str) -> ffq_common::Result<(Option<u64>, Option<u64>)> {
        Ok((None, None))
    }
}

struct GtToGte11Rule;

impl OptimizerRule for GtToGte11Rule {
    fn name(&self) -> &str {
        "test_gt_to_gte_11"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _ctx: &dyn OptimizerContext,
        _cfg: OptimizerConfig,
    ) -> ffq_common::Result<LogicalPlan> {
        fn rewrite_expr(expr: Expr) -> Expr {
            match expr {
                Expr::BinaryOp { left, op, right } => {
                    let left = rewrite_expr(*left);
                    let right = rewrite_expr(*right);
                    match (op, &right) {
                        (BinaryOp::Gt, Expr::Literal(ffq_planner::LiteralValue::Int64(10))) => {
                            Expr::BinaryOp {
                                left: Box::new(left),
                                op: BinaryOp::GtEq,
                                right: Box::new(Expr::Literal(ffq_planner::LiteralValue::Int64(
                                    11,
                                ))),
                            }
                        }
                        _ => Expr::BinaryOp {
                            left: Box::new(left),
                            op,
                            right: Box::new(right),
                        },
                    }
                }
                Expr::And(a, b) => {
                    Expr::And(Box::new(rewrite_expr(*a)), Box::new(rewrite_expr(*b)))
                }
                Expr::Or(a, b) => Expr::Or(Box::new(rewrite_expr(*a)), Box::new(rewrite_expr(*b))),
                Expr::Not(x) => Expr::Not(Box::new(rewrite_expr(*x))),
                Expr::Cast { expr, to_type } => Expr::Cast {
                    expr: Box::new(rewrite_expr(*expr)),
                    to_type,
                },
                Expr::ScalarUdf { name, args } => Expr::ScalarUdf {
                    name,
                    args: args.into_iter().map(rewrite_expr).collect(),
                },
                other => other,
            }
        }

        fn rewrite_plan(plan: LogicalPlan) -> LogicalPlan {
            match plan {
                LogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
                    predicate: rewrite_expr(predicate),
                    input: Box::new(rewrite_plan(*input)),
                },
                LogicalPlan::Projection { exprs, input } => LogicalPlan::Projection {
                    exprs: exprs
                        .into_iter()
                        .map(|(e, n)| (rewrite_expr(e), n))
                        .collect(),
                    input: Box::new(rewrite_plan(*input)),
                },
                LogicalPlan::Limit { n, input } => LogicalPlan::Limit {
                    n,
                    input: Box::new(rewrite_plan(*input)),
                },
                LogicalPlan::TopKByScore {
                    score_expr,
                    k,
                    input,
                } => LogicalPlan::TopKByScore {
                    score_expr: rewrite_expr(score_expr),
                    k,
                    input: Box::new(rewrite_plan(*input)),
                },
                LogicalPlan::Aggregate {
                    group_exprs,
                    aggr_exprs,
                    input,
                } => LogicalPlan::Aggregate {
                    group_exprs: group_exprs.into_iter().map(rewrite_expr).collect(),
                    aggr_exprs,
                    input: Box::new(rewrite_plan(*input)),
                },
                LogicalPlan::Join {
                    left,
                    right,
                    on,
                    join_type,
                    strategy_hint,
                } => LogicalPlan::Join {
                    left: Box::new(rewrite_plan(*left)),
                    right: Box::new(rewrite_plan(*right)),
                    on,
                    join_type,
                    strategy_hint,
                },
                LogicalPlan::InsertInto {
                    table,
                    columns,
                    input,
                } => LogicalPlan::InsertInto {
                    table,
                    columns,
                    input: Box::new(rewrite_plan(*input)),
                },
                LogicalPlan::TableScan {
                    table,
                    projection,
                    filters,
                } => LogicalPlan::TableScan {
                    table,
                    projection,
                    filters: filters.into_iter().map(rewrite_expr).collect(),
                },
                other => other,
            }
        }

        Ok(rewrite_plan(plan))
    }
}

#[test]
fn custom_optimizer_rule_rewrites_gt_to_gte_11() {
    let ctx = TestCtx {
        schema: Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Int64, false),
        ])),
    };
    let plan = LogicalPlan::Filter {
        predicate: Expr::BinaryOp {
            left: Box::new(Expr::Column("x".to_string())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(ffq_planner::LiteralValue::Int64(10))),
        },
        input: Box::new(LogicalPlan::TableScan {
            table: "t".to_string(),
            projection: None,
            filters: vec![],
        }),
    };

    let optimizer = Optimizer::new();
    optimizer.register_rule(Arc::new(GtToGte11Rule));
    let optimized = optimizer
        .optimize(plan, &ctx, OptimizerConfig::default())
        .expect("optimize");
    match optimized {
        LogicalPlan::TableScan { filters, .. } => {
            assert_eq!(filters.len(), 1);
            match &filters[0] {
                Expr::BinaryOp { op, right, .. } => {
                    assert_eq!(*op, BinaryOp::GtEq);
                    match right.as_ref() {
                        Expr::Literal(ffq_planner::LiteralValue::Int64(v)) => assert_eq!(*v, 11),
                        other => panic!("expected rewritten right literal, got {other:?}"),
                    }
                }
                other => panic!("expected binary predicate, got {other:?}"),
            }
        }
        other => panic!("expected table scan with pushed filter, got {other:?}"),
    }
}
