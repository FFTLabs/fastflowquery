use ffq_common::Result;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use crate::analyzer::SchemaProvider;
use crate::logical_plan::{BinaryOp, Expr, JoinStrategyHint, JoinType, LiteralValue, LogicalPlan};

/// Configuration knobs for rule-based optimization.
#[derive(Debug, Clone, Copy)]
pub struct OptimizerConfig {
    /// Max table byte size eligible for broadcast join hinting.
    pub broadcast_threshold_bytes: u64,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            broadcast_threshold_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
/// Table metadata exposed to optimizer rewrite rules.
pub struct TableMetadata {
    /// Storage format (for example `parquet`, `qdrant`).
    pub format: String,
    /// Provider-specific options used by rewrite rules.
    pub options: HashMap<String, String>,
}

/// Provide table stats for join hinting + schemas for pushdown decisions.
pub trait OptimizerContext: SchemaProvider {
    /// Return `(bytes, rows)` estimates for a table.
    fn table_stats(&self, table: &str) -> Result<(Option<u64>, Option<u64>)>; // (bytes, rows)

    /// Return table metadata used by rewrite rules.
    fn table_metadata(&self, _table: &str) -> Result<Option<TableMetadata>> {
        Ok(None)
    }

    /// Convenience getter for table format.
    fn table_format(&self, table: &str) -> Result<Option<String>> {
        Ok(self.table_metadata(table)?.map(|m| m.format))
    }

    /// Convenience getter for table options map.
    fn table_options(&self, table: &str) -> Result<Option<HashMap<String, String>>> {
        Ok(self.table_metadata(table)?.map(|m| m.options))
    }
}

/// Rule-based optimizer for v1 logical plans.
///
/// The implementation is intentionally conservative: pushdowns and rewrites are
/// applied only when correctness preconditions are satisfied; otherwise, the
/// original logical behavior is preserved.
pub struct Optimizer {
    custom_rules: RwLock<HashMap<String, Arc<dyn OptimizerRule>>>,
}

/// Custom optimizer rule hook.
pub trait OptimizerRule: Send + Sync {
    /// Stable rule name used by registry.
    fn name(&self) -> &str;
    /// Rewrite input plan and return transformed plan.
    fn rewrite(
        &self,
        plan: LogicalPlan,
        ctx: &dyn OptimizerContext,
        cfg: OptimizerConfig,
    ) -> Result<LogicalPlan>;
}

impl std::fmt::Debug for Optimizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self
            .custom_rules
            .read()
            .map(|m| m.len())
            .unwrap_or_default();
        f.debug_struct("Optimizer")
            .field("custom_rules", &count)
            .finish()
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Optimizer {
    /// Create a new optimizer.
    pub fn new() -> Self {
        Self {
            custom_rules: RwLock::new(HashMap::new()),
        }
    }

    /// Register or replace a custom optimizer rule.
    ///
    /// Returns `true` when an existing rule with the same name was replaced.
    pub fn register_rule(&self, rule: Arc<dyn OptimizerRule>) -> bool {
        self.custom_rules
            .write()
            .expect("optimizer rule lock poisoned")
            .insert(rule.name().to_string(), rule)
            .is_some()
    }

    /// Deregister a custom optimizer rule by name.
    ///
    /// Returns `true` when an existing rule was removed.
    pub fn deregister_rule(&self, name: &str) -> bool {
        self.custom_rules
            .write()
            .expect("optimizer rule lock poisoned")
            .remove(name)
            .is_some()
    }

    /// Apply v1 rule pipeline to a logical plan.
    ///
    /// Pass order is fixed and intentionally conservative:
    /// 1. constant folding
    /// 2. filter merge
    /// 3. projection pushdown
    /// 4. predicate pushdown
    /// 5. join strategy hinting
    /// 6. vector index rewrite
    ///
    /// Rewrite contract:
    /// - When rewrite preconditions are not met, optimizer must preserve a
    ///   valid fallback plan (for example `TopKByScore`).
    pub fn optimize(
        &self,
        plan: LogicalPlan,
        ctx: &dyn OptimizerContext,
        cfg: OptimizerConfig,
    ) -> Result<LogicalPlan> {
        // 1) constant folding
        let plan = rewrite_plan_exprs(plan, &fold_constants_expr);

        // 2) filter merge
        let plan = merge_filters(plan);

        // 3) projection pushdown
        let plan = projection_pushdown(plan, ctx)?;

        // 4) predicate pushdown (incl join sides)
        let plan = predicate_pushdown(plan, ctx)?;

        // 5) join strategy hint
        let plan = join_strategy_hint(plan, ctx, cfg)?;

        // 6) rewrite to vector index execution when possible
        let mut plan = vector_index_rewrite(plan, ctx)?;

        // 7) user-registered custom rules (deterministic by name)
        let mut rules = self
            .custom_rules
            .read()
            .expect("optimizer rule lock poisoned")
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect::<Vec<_>>();
        rules.sort_by(|a, b| a.0.cmp(&b.0));
        for (_name, rule) in rules {
            plan = rule.rewrite(plan, ctx, cfg)?;
        }

        Ok(plan)
    }
}

// -----------------------------
// 1) Constant folding
// -----------------------------

fn fold_constants_expr(e: Expr) -> Expr {
    match e {
        Expr::Not(inner) => {
            let inner = fold_constants_expr(*inner);
            match inner {
                Expr::Literal(LiteralValue::Boolean(b)) => Expr::Literal(LiteralValue::Boolean(!b)),
                _ => Expr::Not(Box::new(inner)),
            }
        }
        Expr::And(a, b) => {
            let a = fold_constants_expr(*a);
            let b = fold_constants_expr(*b);
            match (&a, &b) {
                (Expr::Literal(LiteralValue::Boolean(false)), _) => {
                    Expr::Literal(LiteralValue::Boolean(false))
                }
                (_, Expr::Literal(LiteralValue::Boolean(false))) => {
                    Expr::Literal(LiteralValue::Boolean(false))
                }
                (Expr::Literal(LiteralValue::Boolean(true)), _) => b,
                (_, Expr::Literal(LiteralValue::Boolean(true))) => a,
                _ => Expr::And(Box::new(a), Box::new(b)),
            }
        }
        Expr::Or(a, b) => {
            let a = fold_constants_expr(*a);
            let b = fold_constants_expr(*b);
            match (&a, &b) {
                (Expr::Literal(LiteralValue::Boolean(true)), _) => {
                    Expr::Literal(LiteralValue::Boolean(true))
                }
                (_, Expr::Literal(LiteralValue::Boolean(true))) => {
                    Expr::Literal(LiteralValue::Boolean(true))
                }
                (Expr::Literal(LiteralValue::Boolean(false)), _) => b,
                (_, Expr::Literal(LiteralValue::Boolean(false))) => a,
                _ => Expr::Or(Box::new(a), Box::new(b)),
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let l = fold_constants_expr(*left);
            let r = fold_constants_expr(*right);

            if let (Expr::Literal(lv), Expr::Literal(rv)) = (&l, &r) {
                if let Some(out) = eval_binary(lv.clone(), op, rv.clone()) {
                    return Expr::Literal(out);
                }
            }

            Expr::BinaryOp {
                left: Box::new(l),
                op,
                right: Box::new(r),
            }
        }
        Expr::Cast { expr, to_type } => {
            let inner = fold_constants_expr(*expr);
            Expr::Cast {
                expr: Box::new(inner),
                to_type,
            }
        }
        Expr::CaseWhen { branches, else_expr } => Expr::CaseWhen {
            branches: branches
                .into_iter()
                .map(|(c, v)| (fold_constants_expr(c), fold_constants_expr(v)))
                .collect(),
            else_expr: else_expr.map(|e| Box::new(fold_constants_expr(*e))),
        },
        #[cfg(feature = "vector")]
        Expr::CosineSimilarity { vector, query } => Expr::CosineSimilarity {
            vector: Box::new(fold_constants_expr(*vector)),
            query: Box::new(fold_constants_expr(*query)),
        },
        #[cfg(feature = "vector")]
        Expr::L2Distance { vector, query } => Expr::L2Distance {
            vector: Box::new(fold_constants_expr(*vector)),
            query: Box::new(fold_constants_expr(*query)),
        },
        #[cfg(feature = "vector")]
        Expr::DotProduct { vector, query } => Expr::DotProduct {
            vector: Box::new(fold_constants_expr(*vector)),
            query: Box::new(fold_constants_expr(*query)),
        },
        Expr::ScalarUdf { name, args } => Expr::ScalarUdf {
            name,
            args: args.into_iter().map(fold_constants_expr).collect(),
        },
        other => other,
    }
}

fn eval_binary(l: LiteralValue, op: BinaryOp, r: LiteralValue) -> Option<LiteralValue> {
    use LiteralValue::*;
    match (l, op, r) {
        (Boolean(a), BinaryOp::Eq, Boolean(b)) => Some(Boolean(a == b)),
        (Boolean(a), BinaryOp::NotEq, Boolean(b)) => Some(Boolean(a != b)),

        (Int64(a), BinaryOp::Plus, Int64(b)) => Some(Int64(a + b)),
        (Int64(a), BinaryOp::Minus, Int64(b)) => Some(Int64(a - b)),
        (Int64(a), BinaryOp::Multiply, Int64(b)) => Some(Int64(a * b)),
        (Int64(a), BinaryOp::Divide, Int64(b)) if b != 0 => Some(Int64(a / b)),
        (Int64(a), BinaryOp::Eq, Int64(b)) => Some(Boolean(a == b)),
        (Int64(a), BinaryOp::NotEq, Int64(b)) => Some(Boolean(a != b)),
        (Int64(a), BinaryOp::Lt, Int64(b)) => Some(Boolean(a < b)),
        (Int64(a), BinaryOp::LtEq, Int64(b)) => Some(Boolean(a <= b)),
        (Int64(a), BinaryOp::Gt, Int64(b)) => Some(Boolean(a > b)),
        (Int64(a), BinaryOp::GtEq, Int64(b)) => Some(Boolean(a >= b)),

        (Float64(a), BinaryOp::Plus, Float64(b)) => Some(Float64(a + b)),
        (Float64(a), BinaryOp::Minus, Float64(b)) => Some(Float64(a - b)),
        (Float64(a), BinaryOp::Multiply, Float64(b)) => Some(Float64(a * b)),
        (Float64(a), BinaryOp::Divide, Float64(b)) if b != 0.0 => Some(Float64(a / b)),
        (Float64(a), BinaryOp::Eq, Float64(b)) => Some(Boolean(a == b)),
        (Float64(a), BinaryOp::NotEq, Float64(b)) => Some(Boolean(a != b)),
        (Float64(a), BinaryOp::Lt, Float64(b)) => Some(Boolean(a < b)),
        (Float64(a), BinaryOp::LtEq, Float64(b)) => Some(Boolean(a <= b)),
        (Float64(a), BinaryOp::Gt, Float64(b)) => Some(Boolean(a > b)),
        (Float64(a), BinaryOp::GtEq, Float64(b)) => Some(Boolean(a >= b)),

        (Utf8(a), BinaryOp::Eq, Utf8(b)) => Some(Boolean(a == b)),
        (Utf8(a), BinaryOp::NotEq, Utf8(b)) => Some(Boolean(a != b)),
        _ => None,
    }
}

// -----------------------------
// 2) Filter merge
// -----------------------------

fn merge_filters(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { predicate, input } => {
            let input = merge_filters(*input);
            if let LogicalPlan::Filter {
                predicate: inner_pred,
                input: inner_input,
            } = input
            {
                // Filter(Filter(x)) => Filter(x) with merged predicate
                LogicalPlan::Filter {
                    predicate: Expr::And(Box::new(inner_pred), Box::new(predicate)),
                    input: inner_input,
                }
            } else {
                LogicalPlan::Filter {
                    predicate,
                    input: Box::new(input),
                }
            }
        }
        other => map_children(other, merge_filters),
    }
}

// -----------------------------
// 3) Projection pushdown
// -----------------------------

fn projection_pushdown(plan: LogicalPlan, ctx: &dyn OptimizerContext) -> Result<LogicalPlan> {
    // We do a simple "required columns" pushdown. This is conservative:
    // - we only push into TableScan.projection
    // - we also include scan filters and join keys automatically
    let (p, _req) = proj_rewrite(plan, None, ctx)?;
    Ok(p)
}

fn proj_rewrite(
    plan: LogicalPlan,
    required: Option<HashSet<String>>,
    ctx: &dyn OptimizerContext,
) -> Result<(LogicalPlan, HashSet<String>)> {
    match plan {
        LogicalPlan::Limit { n, input } => {
            let (new_in, req) = proj_rewrite(*input, required, ctx)?;
            Ok((
                LogicalPlan::Limit {
                    n,
                    input: Box::new(new_in),
                },
                req,
            ))
        }
        LogicalPlan::TopKByScore {
            score_expr,
            k,
            input,
        } => {
            let mut req = required.unwrap_or_default();
            req.extend(expr_columns(&score_expr));
            let (new_in, child_req) = proj_rewrite(*input, Some(req), ctx)?;
            Ok((
                LogicalPlan::TopKByScore {
                    score_expr,
                    k,
                    input: Box::new(new_in),
                },
                child_req,
            ))
        }
        LogicalPlan::UnionAll { left, right } => {
            let (new_left, _lreq) = proj_rewrite(*left, None, ctx)?;
            let (new_right, _rreq) = proj_rewrite(*right, None, ctx)?;
            Ok((
                LogicalPlan::UnionAll {
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                },
                required.unwrap_or_default(),
            ))
        }
        LogicalPlan::CteRef { name, plan } => {
            let (new_plan, req) = proj_rewrite(*plan, required, ctx)?;
            Ok((
                LogicalPlan::CteRef {
                    name,
                    plan: Box::new(new_plan),
                },
                req,
            ))
        }

        LogicalPlan::Filter { predicate, input } => {
            let mut req = required.unwrap_or_default();
            req.extend(expr_columns(&predicate));
            let (new_in, child_req) = proj_rewrite(*input, Some(req), ctx)?;
            Ok((
                LogicalPlan::Filter {
                    predicate,
                    input: Box::new(new_in),
                },
                child_req,
            ))
        }
        LogicalPlan::InSubqueryFilter {
            input,
            expr,
            subquery,
            negated,
            correlation,
        } => {
            // Keep full left input shape before analysis so correlated-IN decorrelation
            // can still discover/use outer reference columns.
            let (new_in, child_req) = proj_rewrite(*input, None, ctx)?;
            let (new_sub, _sub_req) = proj_rewrite(*subquery, None, ctx)?;
            Ok((
                LogicalPlan::InSubqueryFilter {
                    input: Box::new(new_in),
                    expr,
                    subquery: Box::new(new_sub),
                    negated,
                    correlation,
                },
                child_req,
            ))
        }
        LogicalPlan::ExistsSubqueryFilter {
            input,
            subquery,
            negated,
            correlation,
        } => {
            // Keep full left input shape before analysis so correlated-EXISTS
            // decorrelation can still discover/use outer reference columns.
            let (new_in, child_req) = proj_rewrite(*input, None, ctx)?;
            let (new_sub, _sub_req) = proj_rewrite(*subquery, None, ctx)?;
            Ok((
                LogicalPlan::ExistsSubqueryFilter {
                    input: Box::new(new_in),
                    subquery: Box::new(new_sub),
                    negated,
                    correlation,
                },
                child_req,
            ))
        }
        LogicalPlan::ScalarSubqueryFilter {
            input,
            expr,
            op,
            subquery,
            correlation,
        } => {
            let mut req = required.unwrap_or_default();
            req.extend(expr_columns(&expr));
            let (new_in, child_req) = proj_rewrite(*input, Some(req), ctx)?;
            let (new_sub, _sub_req) = proj_rewrite(*subquery, None, ctx)?;
            Ok((
                LogicalPlan::ScalarSubqueryFilter {
                    input: Box::new(new_in),
                    expr,
                    op,
                    subquery: Box::new(new_sub),
                    correlation,
                },
                child_req,
            ))
        }

        LogicalPlan::Projection { exprs, input } => {
            // Optional column pruning: if parent only needs subset of projection outputs,
            // keep only those.
            let kept_exprs = if let Some(req) = &required {
                exprs
                    .into_iter()
                    .filter(|(_e, name)| req.contains(name))
                    .collect::<Vec<_>>()
            } else {
                exprs
            };

            let mut child_req = HashSet::new();
            for (e, _name) in &kept_exprs {
                child_req.extend(expr_columns(e));
            }

            let (new_in, _) = proj_rewrite(*input, Some(child_req.clone()), ctx)?;
            Ok((
                LogicalPlan::Projection {
                    exprs: kept_exprs,
                    input: Box::new(new_in),
                },
                child_req,
            ))
        }

        LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => {
            // v1: no column pruning across aggregates yet
            let mut child_req = HashSet::new();
            for g in &group_exprs {
                child_req.extend(expr_columns(g));
            }
            for (agg, _name) in &aggr_exprs {
                child_req.extend(agg_columns(agg));
            }
            let (new_in, _) = proj_rewrite(*input, Some(child_req.clone()), ctx)?;
            Ok((
                LogicalPlan::Aggregate {
                    group_exprs,
                    aggr_exprs,
                    input: Box::new(new_in),
                },
                child_req,
            ))
        }
        LogicalPlan::Window { exprs, input } => {
            let mut child_req = required.unwrap_or_default();
            for w in &exprs {
                for p in &w.partition_by {
                    child_req.extend(expr_columns(p));
                }
                for o in &w.order_by {
                    child_req.extend(expr_columns(&o.expr));
                }
                match &w.func {
                    crate::logical_plan::WindowFunction::Count(arg)
                    | crate::logical_plan::WindowFunction::Sum(arg)
                    | crate::logical_plan::WindowFunction::Avg(arg)
                    | crate::logical_plan::WindowFunction::Min(arg)
                    | crate::logical_plan::WindowFunction::Max(arg) => {
                        child_req.extend(expr_columns(arg));
                    }
                    crate::logical_plan::WindowFunction::Lag { expr, default, .. }
                    | crate::logical_plan::WindowFunction::Lead { expr, default, .. } => {
                        child_req.extend(expr_columns(expr));
                        if let Some(d) = default {
                            child_req.extend(expr_columns(d));
                        }
                    }
                    crate::logical_plan::WindowFunction::FirstValue(expr)
                    | crate::logical_plan::WindowFunction::LastValue(expr)
                    | crate::logical_plan::WindowFunction::NthValue { expr, .. } => {
                        child_req.extend(expr_columns(expr));
                    }
                    _ => {}
                }
            }
            let (new_in, _) = proj_rewrite(*input, Some(child_req.clone()), ctx)?;
            Ok((
                LogicalPlan::Window {
                    exprs,
                    input: Box::new(new_in),
                },
                child_req,
            ))
        }

        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
            strategy_hint,
        } => {
            // Split required columns by side if we can.
            let left_cols = plan_output_columns(&left, ctx)?;
            let right_cols = plan_output_columns(&right, ctx)?;

            let mut req_left: HashSet<String> = HashSet::new();
            let mut req_right: HashSet<String> = HashSet::new();

            // join keys always required
            for (lk, rk) in &on {
                req_left.insert(strip_qual(lk));
                req_right.insert(strip_qual(rk));
            }

            if let Some(req) = required {
                for c in req {
                    let c2 = strip_qual(&c);
                    let in_l = left_cols.contains(&c2);
                    let in_r = right_cols.contains(&c2);
                    if in_l && !in_r {
                        req_left.insert(c2);
                    } else if in_r && !in_l {
                        req_right.insert(c2);
                    } else {
                        // ambiguous: keep both sides conservative
                        req_left.insert(c2.clone());
                        req_right.insert(c2);
                    }
                }
            } else {
                // Join is itself the root (or parent did not constrain output columns):
                // preserve full join output shape, not just join keys.
                req_left.extend(left_cols);
                req_right.extend(right_cols);
            }

            let (new_l, _) = proj_rewrite(*left, Some(req_left), ctx)?;
            let (new_r, _) = proj_rewrite(*right, Some(req_right), ctx)?;

            Ok((
                LogicalPlan::Join {
                    left: Box::new(new_l),
                    right: Box::new(new_r),
                    on,
                    join_type,
                    strategy_hint,
                },
                HashSet::new(),
            ))
        }
        LogicalPlan::InsertInto {
            table,
            columns,
            input,
        } => {
            let (new_in, _) = proj_rewrite(*input, None, ctx)?;
            Ok((
                LogicalPlan::InsertInto {
                    table,
                    columns,
                    input: Box::new(new_in),
                },
                HashSet::new(),
            ))
        }
        LogicalPlan::VectorTopK {
            table,
            query_vector,
            k,
            filter,
        } => Ok((
            LogicalPlan::VectorTopK {
                table,
                query_vector,
                k,
                filter,
            },
            HashSet::new(),
        )),

        LogicalPlan::TableScan {
            table,
            projection,
            filters,
        } => {
            // Compute required cols from parent and from scan filters.
            let mut req = required.unwrap_or_default();
            for f in &filters {
                req.extend(expr_columns(f));
            }

            if req.is_empty() {
                // no restriction -> keep existing projection
                return Ok((
                    LogicalPlan::TableScan {
                        table,
                        projection,
                        filters,
                    },
                    HashSet::new(),
                ));
            }

            // Validate against schema (and keep stable ordering)
            let schema = ctx.table_schema(&table)?;
            let mut cols = vec![];
            for field in schema.fields() {
                let name = field.name();
                if req.contains(name) {
                    cols.push(name.to_string());
                }
            }

            Ok((
                LogicalPlan::TableScan {
                    table,
                    projection: Some(cols),
                    filters,
                },
                HashSet::new(),
            ))
        }
    }
}

// -----------------------------
// 4) Predicate pushdown
// -----------------------------

fn predicate_pushdown(plan: LogicalPlan, ctx: &dyn OptimizerContext) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Filter { predicate, input } => {
            let input = predicate_pushdown(*input, ctx)?;

            // push into TableScan
            if let LogicalPlan::TableScan {
                table,
                projection,
                mut filters,
            } = input
            {
                // Keep CASE predicates above scan: parquet pushdown path does
                // not evaluate general expression trees.
                if expr_contains_case(&predicate) {
                    return Ok(LogicalPlan::Filter {
                        predicate,
                        input: Box::new(LogicalPlan::TableScan {
                            table,
                            projection,
                            filters,
                        }),
                    });
                }
                filters.push(predicate);
                return Ok(LogicalPlan::TableScan {
                    table,
                    projection,
                    filters,
                });
            }

            // push below passthrough projection
            if let LogicalPlan::Projection {
                exprs,
                input: inner,
            } = input
            {
                if projection_is_passthrough(&exprs) {
                    let pushed = LogicalPlan::Filter {
                        predicate,
                        input: inner,
                    };
                    return Ok(LogicalPlan::Projection {
                        exprs,
                        input: Box::new(pushed),
                    });
                } else {
                    return Ok(LogicalPlan::Filter {
                        predicate,
                        input: Box::new(LogicalPlan::Projection {
                            exprs,
                            input: inner,
                        }),
                    });
                }
            }

            // push below subquery filters by pushing only the left/input branch.
            if let LogicalPlan::InSubqueryFilter {
                input: sub_input,
                expr,
                subquery,
                negated,
                correlation,
            } = input
            {
                let pushed_left = predicate_pushdown(
                    LogicalPlan::Filter {
                        predicate,
                        input: sub_input,
                    },
                    ctx,
                )?;
                let pushed_subquery = predicate_pushdown(*subquery, ctx)?;
                return Ok(LogicalPlan::InSubqueryFilter {
                    input: Box::new(pushed_left),
                    expr,
                    subquery: Box::new(pushed_subquery),
                    negated,
                    correlation,
                });
            }
            if let LogicalPlan::ExistsSubqueryFilter {
                input: sub_input,
                subquery,
                negated,
                correlation,
            } = input
            {
                let pushed_left = predicate_pushdown(
                    LogicalPlan::Filter {
                        predicate,
                        input: sub_input,
                    },
                    ctx,
                )?;
                let pushed_subquery = predicate_pushdown(*subquery, ctx)?;
                return Ok(LogicalPlan::ExistsSubqueryFilter {
                    input: Box::new(pushed_left),
                    subquery: Box::new(pushed_subquery),
                    negated,
                    correlation,
                });
            }
            if let LogicalPlan::ScalarSubqueryFilter {
                input: sub_input,
                expr,
                op,
                subquery,
                correlation,
            } = input
            {
                let pushed_left = predicate_pushdown(
                    LogicalPlan::Filter {
                        predicate,
                        input: sub_input,
                    },
                    ctx,
                )?;
                let pushed_subquery = predicate_pushdown(*subquery, ctx)?;
                return Ok(LogicalPlan::ScalarSubqueryFilter {
                    input: Box::new(pushed_left),
                    expr,
                    op,
                    subquery: Box::new(pushed_subquery),
                    correlation,
                });
            }

            // push to join sides
            if let LogicalPlan::Join {
                left,
                right,
                on,
                join_type,
                strategy_hint,
            } = input
            {
                if join_type != JoinType::Inner {
                    return Ok(LogicalPlan::Filter {
                        predicate,
                        input: Box::new(LogicalPlan::Join {
                            left,
                            right,
                            on,
                            join_type,
                            strategy_hint,
                        }),
                    });
                }

                let conjuncts = split_conjuncts(predicate);
                let left_cols = plan_output_columns(&left, ctx)?;
                let right_cols = plan_output_columns(&right, ctx)?;

                let mut left_push = vec![];
                let mut right_push = vec![];
                let mut keep = vec![];

                for c in conjuncts {
                    let cols = expr_columns(&c);
                    let mut seen_l = false;
                    let mut seen_r = false;
                    for col in cols {
                        let col2 = strip_qual(&col);
                        if left_cols.contains(&col2) {
                            seen_l = true;
                        }
                        if right_cols.contains(&col2) {
                            seen_r = true;
                        }
                    }
                    if seen_l && !seen_r {
                        left_push.push(c);
                    } else if seen_r && !seen_l {
                        right_push.push(c);
                    } else {
                        keep.push(c);
                    }
                }

                let new_left = if left_push.is_empty() {
                    *left
                } else {
                    LogicalPlan::Filter {
                        predicate: combine_conjuncts(left_push),
                        input: left,
                    }
                };

                let new_right = if right_push.is_empty() {
                    *right
                } else {
                    LogicalPlan::Filter {
                        predicate: combine_conjuncts(right_push),
                        input: right,
                    }
                };

                let join = LogicalPlan::Join {
                    left: Box::new(predicate_pushdown(new_left, ctx)?),
                    right: Box::new(predicate_pushdown(new_right, ctx)?),
                    on,
                    join_type,
                    strategy_hint,
                };

                if keep.is_empty() {
                    return Ok(join);
                }
                return Ok(LogicalPlan::Filter {
                    predicate: combine_conjuncts(keep),
                    input: Box::new(join),
                });
            }

            Ok(LogicalPlan::Filter {
                predicate,
                input: Box::new(input),
            })
        }

        other => try_map_children(other, |p| predicate_pushdown(p, ctx)),
    }
}

// -----------------------------
// 5) Join strategy hint
// -----------------------------

fn join_strategy_hint(
    plan: LogicalPlan,
    ctx: &dyn OptimizerContext,
    cfg: OptimizerConfig,
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
            strategy_hint: _,
        } => {
            let l_bytes = estimate_bytes(&left, ctx)?;
            let r_bytes = estimate_bytes(&right, ctx)?;

            let hint = if let (Some(lb), Some(rb)) = (l_bytes, r_bytes) {
                if lb <= cfg.broadcast_threshold_bytes && lb <= rb {
                    JoinStrategyHint::BroadcastLeft
                } else if rb <= cfg.broadcast_threshold_bytes && rb < lb {
                    JoinStrategyHint::BroadcastRight
                } else {
                    JoinStrategyHint::Shuffle
                }
            } else {
                // no stats -> conservative
                JoinStrategyHint::Shuffle
            };

            Ok(LogicalPlan::Join {
                left: Box::new(join_strategy_hint(*left, ctx, cfg)?),
                right: Box::new(join_strategy_hint(*right, ctx, cfg)?),
                on,
                join_type,
                strategy_hint: hint,
            })
        }
        other => try_map_children(other, |p| join_strategy_hint(p, ctx, cfg)),
    }
}

fn vector_index_rewrite(plan: LogicalPlan, ctx: &dyn OptimizerContext) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Filter { predicate, input } => Ok(LogicalPlan::Filter {
            predicate,
            input: Box::new(vector_index_rewrite(*input, ctx)?),
        }),
        LogicalPlan::InSubqueryFilter {
            input,
            expr,
            subquery,
            negated,
            correlation,
        } => Ok(LogicalPlan::InSubqueryFilter {
            input: Box::new(vector_index_rewrite(*input, ctx)?),
            expr,
            subquery: Box::new(vector_index_rewrite(*subquery, ctx)?),
            negated,
            correlation,
        }),
        LogicalPlan::ExistsSubqueryFilter {
            input,
            subquery,
            negated,
            correlation,
        } => Ok(LogicalPlan::ExistsSubqueryFilter {
            input: Box::new(vector_index_rewrite(*input, ctx)?),
            subquery: Box::new(vector_index_rewrite(*subquery, ctx)?),
            negated,
            correlation,
        }),
        LogicalPlan::ScalarSubqueryFilter {
            input,
            expr,
            op,
            subquery,
            correlation,
        } => Ok(LogicalPlan::ScalarSubqueryFilter {
            input: Box::new(vector_index_rewrite(*input, ctx)?),
            expr,
            op,
            subquery: Box::new(vector_index_rewrite(*subquery, ctx)?),
            correlation,
        }),
        LogicalPlan::Projection { exprs, input } => {
            let rewritten_input = vector_index_rewrite(*input, ctx)?;
            #[cfg(feature = "vector")]
            if let Some(vector_topk) =
                try_rewrite_projection_topk_to_vector(&exprs, &rewritten_input, ctx)?
            {
                return Ok(LogicalPlan::Projection {
                    exprs,
                    input: Box::new(vector_topk),
                });
            }
            Ok(LogicalPlan::Projection {
                exprs,
                input: Box::new(rewritten_input),
            })
        }
        LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => Ok(LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input: Box::new(vector_index_rewrite(*input, ctx)?),
        }),
        LogicalPlan::Window { exprs, input } => Ok(LogicalPlan::Window {
            exprs,
            input: Box::new(vector_index_rewrite(*input, ctx)?),
        }),
        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
            strategy_hint,
        } => Ok(LogicalPlan::Join {
            left: Box::new(vector_index_rewrite(*left, ctx)?),
            right: Box::new(vector_index_rewrite(*right, ctx)?),
            on,
            join_type,
            strategy_hint,
        }),
        LogicalPlan::Limit { n, input } => Ok(LogicalPlan::Limit {
            n,
            input: Box::new(vector_index_rewrite(*input, ctx)?),
        }),
        LogicalPlan::TopKByScore {
            score_expr,
            k,
            input,
        } => Ok(LogicalPlan::TopKByScore {
            score_expr,
            k,
            input: Box::new(vector_index_rewrite(*input, ctx)?),
        }),
        LogicalPlan::InsertInto {
            table,
            columns,
            input,
        } => Ok(LogicalPlan::InsertInto {
            table,
            columns,
            input: Box::new(vector_index_rewrite(*input, ctx)?),
        }),
        LogicalPlan::UnionAll { left, right } => Ok(LogicalPlan::UnionAll {
            left: Box::new(vector_index_rewrite(*left, ctx)?),
            right: Box::new(vector_index_rewrite(*right, ctx)?),
        }),
        LogicalPlan::CteRef { name, plan } => Ok(LogicalPlan::CteRef {
            name,
            plan: Box::new(vector_index_rewrite(*plan, ctx)?),
        }),
        leaf @ LogicalPlan::TableScan { .. } => Ok(leaf),
        leaf @ LogicalPlan::VectorTopK { .. } => Ok(leaf),
    }
}

#[cfg(feature = "vector")]
fn try_rewrite_projection_topk_to_vector(
    exprs: &[(Expr, String)],
    input: &LogicalPlan,
    ctx: &dyn OptimizerContext,
) -> Result<Option<LogicalPlan>> {
    if let Some(two_phase) = try_rewrite_projection_topk_to_two_phase(exprs, input, ctx)? {
        return Ok(Some(two_phase));
    }
    match evaluate_vector_topk_rewrite(exprs, input, ctx)? {
        VectorRewriteDecision::Apply {
            table,
            query_vector,
            k,
            filter,
        } => Ok(Some(LogicalPlan::VectorTopK {
            table,
            query_vector,
            k,
            filter,
        })),
        VectorRewriteDecision::Fallback { .. } => Ok(None),
    }
}

#[cfg(feature = "vector")]
fn try_rewrite_projection_topk_to_two_phase(
    _exprs: &[(Expr, String)],
    input: &LogicalPlan,
    ctx: &dyn OptimizerContext,
) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::TopKByScore {
        score_expr,
        k,
        input: topk_input,
    } = input
    else {
        return Ok(None);
    };
    if *k == 0 {
        return Ok(None);
    }
    let LogicalPlan::TableScan {
        table: docs_table,
        filters,
        ..
    } = topk_input.as_ref()
    else {
        return Ok(None);
    };
    let Expr::CosineSimilarity { vector, query } = score_expr else {
        return Ok(None);
    };
    let docs_options = match ctx.table_options(docs_table)? {
        Some(v) => v,
        None => return Ok(None),
    };
    let index_table = match docs_options.get("vector.index_table") {
        Some(v) if !v.trim().is_empty() => v.clone(),
        _ => return Ok(None),
    };
    if ctx.table_format(&index_table)?.as_deref() != Some("qdrant") {
        return Ok(None);
    }

    let emb_col = expr_col_name(vector)?;
    if let Some(cfg_emb_col) = docs_options.get("vector.embedding_column") {
        if strip_qual(cfg_emb_col) != strip_qual(&emb_col) {
            return Ok(None);
        }
    }
    let Expr::Literal(LiteralValue::VectorF32(query_vector)) = query.as_ref() else {
        return Ok(None);
    };

    let id_col = docs_options
        .get("vector.id_column")
        .cloned()
        .unwrap_or_else(|| "id".to_string());
    let prefetch_k = prefetch_k(*k, &docs_options);

    let mut rerank_input = LogicalPlan::Join {
        left: Box::new(LogicalPlan::TableScan {
            table: docs_table.clone(),
            projection: None,
            filters: Vec::new(),
        }),
        right: Box::new(LogicalPlan::VectorTopK {
            table: index_table,
            query_vector: query_vector.clone(),
            k: prefetch_k,
            filter: None,
        }),
        on: vec![(id_col, "id".to_string())],
        join_type: JoinType::Inner,
        strategy_hint: JoinStrategyHint::BroadcastRight,
    };
    rerank_input = LogicalPlan::Projection {
        exprs: two_phase_join_projection_exprs(docs_table, ctx)?,
        input: Box::new(rerank_input),
    };
    if !filters.is_empty() {
        rerank_input = LogicalPlan::Filter {
            predicate: combine_conjuncts(filters.clone()),
            input: Box::new(rerank_input),
        };
    }
    Ok(Some(LogicalPlan::TopKByScore {
        score_expr: score_expr.clone(),
        k: *k,
        input: Box::new(rerank_input),
    }))
}

#[cfg(feature = "vector")]
fn expr_col_name(e: &Expr) -> Result<String> {
    match e {
        Expr::Column(c) => Ok(c.clone()),
        Expr::ColumnRef { name, .. } => Ok(name.clone()),
        _ => Err(ffq_common::FfqError::Planning(
            "expected column expression".to_string(),
        )),
    }
}

#[cfg(feature = "vector")]
fn prefetch_k(k: usize, options: &HashMap<String, String>) -> usize {
    let multiplier = options
        .get("vector.prefetch_multiplier")
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(4);
    let mut out = k.saturating_mul(multiplier).max(k);
    if let Some(cap) = options
        .get("vector.prefetch_cap")
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
    {
        out = out.min(cap).max(k);
    }
    out
}

#[cfg(feature = "vector")]
fn two_phase_join_projection_exprs(
    docs_table: &str,
    ctx: &dyn OptimizerContext,
) -> Result<Vec<(Expr, String)>> {
    let schema = ctx.table_schema(docs_table)?;
    let mut out: Vec<(Expr, String)> = schema
        .fields()
        .iter()
        .map(|f| {
            let name = f.name().to_string();
            (Expr::Column(format!("{docs_table}.{name}")), name)
        })
        .collect();
    if schema.index_of("score").is_err() {
        out.push((Expr::Column("score".to_string()), "score".to_string()));
    }
    if schema.index_of("payload").is_err() {
        out.push((Expr::Column("payload".to_string()), "payload".to_string()));
    }
    Ok(out)
}

#[cfg(feature = "vector")]
enum VectorRewriteDecision {
    Apply {
        table: String,
        query_vector: Vec<f32>,
        k: usize,
        filter: Option<String>,
    },
    Fallback {
        _reason: &'static str,
    },
}

#[cfg(feature = "vector")]
fn evaluate_vector_topk_rewrite(
    exprs: &[(Expr, String)],
    input: &LogicalPlan,
    ctx: &dyn OptimizerContext,
) -> Result<VectorRewriteDecision> {
    if !projection_supported_for_vector_topk(exprs) {
        return Ok(VectorRewriteDecision::Fallback {
            _reason: "projection not satisfiable from [id,score,payload]",
        });
    }
    let LogicalPlan::TopKByScore {
        score_expr,
        k,
        input,
    } = input
    else {
        return Ok(VectorRewriteDecision::Fallback {
            _reason: "input is not TopKByScore",
        });
    };
    if *k == 0 {
        return Ok(VectorRewriteDecision::Fallback {
            _reason: "k must be > 0",
        });
    }
    let LogicalPlan::TableScan { table, filters, .. } = input.as_ref() else {
        return Ok(VectorRewriteDecision::Fallback {
            _reason: "TopK input is not TableScan",
        });
    };
    if ctx.table_format(table)?.as_deref() != Some("qdrant") {
        return Ok(VectorRewriteDecision::Fallback {
            _reason: "table format is not qdrant",
        });
    }
    let Expr::CosineSimilarity { vector, query } = score_expr else {
        return Ok(VectorRewriteDecision::Fallback {
            _reason: "score expr is not cosine_similarity",
        });
    };
    if !matches!(vector.as_ref(), Expr::Column(_) | Expr::ColumnRef { .. }) {
        return Ok(VectorRewriteDecision::Fallback {
            _reason: "vector arg is not a column",
        });
    }
    let Expr::Literal(LiteralValue::VectorF32(query_vector)) = query.as_ref() else {
        return Ok(VectorRewriteDecision::Fallback {
            _reason: "query arg is not vector literal",
        });
    };
    let filter = match translate_qdrant_filter(filters) {
        Ok(v) => v,
        Err(_) => {
            return Ok(VectorRewriteDecision::Fallback {
                _reason: "filter translation unsupported",
            });
        }
    };

    Ok(VectorRewriteDecision::Apply {
        table: table.clone(),
        query_vector: query_vector.clone(),
        k: *k,
        filter,
    })
}

#[cfg(feature = "vector")]
fn projection_supported_for_vector_topk(exprs: &[(Expr, String)]) -> bool {
    exprs.iter().all(|(e, _)| {
        matches!(
            e,
            Expr::Column(c) if c == "id" || c == "score" || c == "payload"
        ) || matches!(
            e,
            Expr::ColumnRef { name, .. } if name == "id" || name == "score" || name == "payload"
        )
    })
}

#[cfg(feature = "vector")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct QdrantFilterSpec {
    must: Vec<QdrantMatchClause>,
}

#[cfg(feature = "vector")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct QdrantMatchClause {
    field: String,
    value: serde_json::Value,
}

#[cfg(feature = "vector")]
fn translate_qdrant_filter(filters: &[Expr]) -> Result<Option<String>> {
    if filters.is_empty() {
        return Ok(None);
    }
    let mut clauses = Vec::new();
    for f in filters {
        collect_qdrant_match_clauses(f, &mut clauses)?;
    }
    let encoded = serde_json::to_string(&QdrantFilterSpec { must: clauses })
        .map_err(|e| ffq_common::FfqError::Planning(format!("qdrant filter encode failed: {e}")))?;
    Ok(Some(encoded))
}

#[cfg(feature = "vector")]
fn collect_qdrant_match_clauses(e: &Expr, out: &mut Vec<QdrantMatchClause>) -> Result<()> {
    match e {
        Expr::And(a, b) => {
            collect_qdrant_match_clauses(a, out)?;
            collect_qdrant_match_clauses(b, out)?;
            Ok(())
        }
        Expr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        } => {
            if let Some((field, value)) = eq_clause_parts(left, right) {
                out.push(QdrantMatchClause { field, value });
                return Ok(());
            }
            Err(ffq_common::FfqError::Planning(
                "unsupported qdrant filter expression; expected `col = literal`".to_string(),
            ))
        }
        _ => Err(ffq_common::FfqError::Planning(
            "unsupported qdrant filter expression; only equality and AND are supported".to_string(),
        )),
    }
}

#[cfg(feature = "vector")]
fn eq_clause_parts(left: &Expr, right: &Expr) -> Option<(String, serde_json::Value)> {
    match (extract_filter_field(left), extract_filter_literal(right)) {
        (Some(field), Some(value)) => Some((field, value)),
        _ => match (extract_filter_field(right), extract_filter_literal(left)) {
            (Some(field), Some(value)) => Some((field, value)),
            _ => None,
        },
    }
}

#[cfg(feature = "vector")]
fn extract_filter_field(e: &Expr) -> Option<String> {
    match e {
        Expr::Column(c) => Some(c.clone()),
        Expr::ColumnRef { name, .. } => Some(name.clone()),
        _ => None,
    }
}

#[cfg(feature = "vector")]
fn extract_filter_literal(e: &Expr) -> Option<serde_json::Value> {
    match e {
        Expr::Literal(LiteralValue::Int64(v)) => Some(serde_json::Value::from(*v)),
        Expr::Literal(LiteralValue::Utf8(v)) => Some(serde_json::Value::from(v.clone())),
        Expr::Literal(LiteralValue::Boolean(v)) => Some(serde_json::Value::from(*v)),
        _ => None,
    }
}

// -----------------------------
// Helpers
// -----------------------------

fn map_children(plan: LogicalPlan, f: impl Fn(LogicalPlan) -> LogicalPlan + Copy) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
            predicate,
            input: Box::new(f(*input)),
        },
        LogicalPlan::InSubqueryFilter {
            input,
            expr,
            subquery,
            negated,
            correlation,
        } => LogicalPlan::InSubqueryFilter {
            input: Box::new(f(*input)),
            expr,
            subquery: Box::new(f(*subquery)),
            negated,
            correlation,
        },
        LogicalPlan::ExistsSubqueryFilter {
            input,
            subquery,
            negated,
            correlation,
        } => LogicalPlan::ExistsSubqueryFilter {
            input: Box::new(f(*input)),
            subquery: Box::new(f(*subquery)),
            negated,
            correlation,
        },
        LogicalPlan::ScalarSubqueryFilter {
            input,
            expr,
            op,
            subquery,
            correlation,
        } => LogicalPlan::ScalarSubqueryFilter {
            input: Box::new(f(*input)),
            expr,
            op,
            subquery: Box::new(f(*subquery)),
            correlation,
        },
        LogicalPlan::Projection { exprs, input } => LogicalPlan::Projection {
            exprs,
            input: Box::new(f(*input)),
        },
        LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input: Box::new(f(*input)),
        },
        LogicalPlan::Window { exprs, input } => LogicalPlan::Window {
            exprs,
            input: Box::new(f(*input)),
        },
        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
            strategy_hint,
        } => LogicalPlan::Join {
            left: Box::new(f(*left)),
            right: Box::new(f(*right)),
            on,
            join_type,
            strategy_hint,
        },
        LogicalPlan::Limit { n, input } => LogicalPlan::Limit {
            n,
            input: Box::new(f(*input)),
        },
        LogicalPlan::TopKByScore {
            score_expr,
            k,
            input,
        } => LogicalPlan::TopKByScore {
            score_expr,
            k,
            input: Box::new(f(*input)),
        },
        LogicalPlan::VectorTopK {
            table,
            query_vector,
            k,
            filter,
        } => LogicalPlan::VectorTopK {
            table,
            query_vector,
            k,
            filter,
        },
        LogicalPlan::InsertInto {
            table,
            columns,
            input,
        } => LogicalPlan::InsertInto {
            table,
            columns,
            input: Box::new(f(*input)),
        },
        LogicalPlan::UnionAll { left, right } => LogicalPlan::UnionAll {
            left: Box::new(f(*left)),
            right: Box::new(f(*right)),
        },
        LogicalPlan::CteRef { name, plan } => LogicalPlan::CteRef {
            name,
            plan: Box::new(f(*plan)),
        },
        s @ LogicalPlan::TableScan { .. } => s,
    }
}

fn try_map_children(
    plan: LogicalPlan,
    f: impl Fn(LogicalPlan) -> Result<LogicalPlan> + Copy,
) -> Result<LogicalPlan> {
    Ok(match plan {
        LogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
            predicate,
            input: Box::new(f(*input)?),
        },
        LogicalPlan::InSubqueryFilter {
            input,
            expr,
            subquery,
            negated,
            correlation,
        } => LogicalPlan::InSubqueryFilter {
            input: Box::new(f(*input)?),
            expr,
            subquery: Box::new(f(*subquery)?),
            negated,
            correlation,
        },
        LogicalPlan::ExistsSubqueryFilter {
            input,
            subquery,
            negated,
            correlation,
        } => LogicalPlan::ExistsSubqueryFilter {
            input: Box::new(f(*input)?),
            subquery: Box::new(f(*subquery)?),
            negated,
            correlation,
        },
        LogicalPlan::ScalarSubqueryFilter {
            input,
            expr,
            op,
            subquery,
            correlation,
        } => LogicalPlan::ScalarSubqueryFilter {
            input: Box::new(f(*input)?),
            expr,
            op,
            subquery: Box::new(f(*subquery)?),
            correlation,
        },
        LogicalPlan::Projection { exprs, input } => LogicalPlan::Projection {
            exprs,
            input: Box::new(f(*input)?),
        },
        LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input: Box::new(f(*input)?),
        },
        LogicalPlan::Window { exprs, input } => LogicalPlan::Window {
            exprs,
            input: Box::new(f(*input)?),
        },
        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
            strategy_hint,
        } => LogicalPlan::Join {
            left: Box::new(f(*left)?),
            right: Box::new(f(*right)?),
            on,
            join_type,
            strategy_hint,
        },
        LogicalPlan::Limit { n, input } => LogicalPlan::Limit {
            n,
            input: Box::new(f(*input)?),
        },
        LogicalPlan::TopKByScore {
            score_expr,
            k,
            input,
        } => LogicalPlan::TopKByScore {
            score_expr,
            k,
            input: Box::new(f(*input)?),
        },
        LogicalPlan::VectorTopK {
            table,
            query_vector,
            k,
            filter,
        } => LogicalPlan::VectorTopK {
            table,
            query_vector,
            k,
            filter,
        },
        LogicalPlan::InsertInto {
            table,
            columns,
            input,
        } => LogicalPlan::InsertInto {
            table,
            columns,
            input: Box::new(f(*input)?),
        },
        LogicalPlan::UnionAll { left, right } => LogicalPlan::UnionAll {
            left: Box::new(f(*left)?),
            right: Box::new(f(*right)?),
        },
        LogicalPlan::CteRef { name, plan } => LogicalPlan::CteRef {
            name,
            plan: Box::new(f(*plan)?),
        },
        s @ LogicalPlan::TableScan { .. } => s,
    })
}

fn rewrite_plan_exprs(plan: LogicalPlan, rewrite: &dyn Fn(Expr) -> Expr) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
            predicate: rewrite_expr(predicate, rewrite),
            input: Box::new(rewrite_plan_exprs(*input, rewrite)),
        },
        LogicalPlan::InSubqueryFilter {
            input,
            expr,
            subquery,
            negated,
            correlation,
        } => LogicalPlan::InSubqueryFilter {
            input: Box::new(rewrite_plan_exprs(*input, rewrite)),
            expr: rewrite_expr(expr, rewrite),
            subquery: Box::new(rewrite_plan_exprs(*subquery, rewrite)),
            negated,
            correlation,
        },
        LogicalPlan::ExistsSubqueryFilter {
            input,
            subquery,
            negated,
            correlation,
        } => LogicalPlan::ExistsSubqueryFilter {
            input: Box::new(rewrite_plan_exprs(*input, rewrite)),
            subquery: Box::new(rewrite_plan_exprs(*subquery, rewrite)),
            negated,
            correlation,
        },
        LogicalPlan::ScalarSubqueryFilter {
            input,
            expr,
            op,
            subquery,
            correlation,
        } => LogicalPlan::ScalarSubqueryFilter {
            input: Box::new(rewrite_plan_exprs(*input, rewrite)),
            expr: rewrite_expr(expr, rewrite),
            op,
            subquery: Box::new(rewrite_plan_exprs(*subquery, rewrite)),
            correlation,
        },
        LogicalPlan::Projection { exprs, input } => LogicalPlan::Projection {
            exprs: exprs
                .into_iter()
                .map(|(e, n)| (rewrite_expr(e, rewrite), n))
                .collect(),
            input: Box::new(rewrite_plan_exprs(*input, rewrite)),
        },
        LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => LogicalPlan::Aggregate {
            group_exprs: group_exprs
                .into_iter()
                .map(|e| rewrite_expr(e, rewrite))
                .collect(),
            aggr_exprs,
            input: Box::new(rewrite_plan_exprs(*input, rewrite)),
        },
        LogicalPlan::Window { exprs, input } => LogicalPlan::Window {
            exprs: exprs
                .into_iter()
                .map(|mut w| {
                    w.partition_by = w
                        .partition_by
                        .into_iter()
                        .map(|e| rewrite_expr(e, rewrite))
                        .collect();
                    w.order_by = w
                        .order_by
                        .into_iter()
                        .map(|mut o| {
                            o.expr = rewrite_expr(o.expr, rewrite);
                            o
                        })
                        .collect();
                    w.func = match w.func {
                        crate::logical_plan::WindowFunction::Count(arg) => {
                            crate::logical_plan::WindowFunction::Count(rewrite_expr(arg, rewrite))
                        }
                        crate::logical_plan::WindowFunction::Sum(arg) => {
                            crate::logical_plan::WindowFunction::Sum(rewrite_expr(arg, rewrite))
                        }
                        crate::logical_plan::WindowFunction::Avg(arg) => {
                            crate::logical_plan::WindowFunction::Avg(rewrite_expr(arg, rewrite))
                        }
                        crate::logical_plan::WindowFunction::Min(arg) => {
                            crate::logical_plan::WindowFunction::Min(rewrite_expr(arg, rewrite))
                        }
                        crate::logical_plan::WindowFunction::Max(arg) => {
                            crate::logical_plan::WindowFunction::Max(rewrite_expr(arg, rewrite))
                        }
                        crate::logical_plan::WindowFunction::Lag {
                            expr,
                            offset,
                            default,
                        } => crate::logical_plan::WindowFunction::Lag {
                            expr: rewrite_expr(expr, rewrite),
                            offset,
                            default: default.map(|d| rewrite_expr(d, rewrite)),
                        },
                        crate::logical_plan::WindowFunction::Lead {
                            expr,
                            offset,
                            default,
                        } => crate::logical_plan::WindowFunction::Lead {
                            expr: rewrite_expr(expr, rewrite),
                            offset,
                            default: default.map(|d| rewrite_expr(d, rewrite)),
                        },
                        crate::logical_plan::WindowFunction::FirstValue(expr) => {
                            crate::logical_plan::WindowFunction::FirstValue(rewrite_expr(
                                expr, rewrite,
                            ))
                        }
                        crate::logical_plan::WindowFunction::LastValue(expr) => {
                            crate::logical_plan::WindowFunction::LastValue(rewrite_expr(
                                expr, rewrite,
                            ))
                        }
                        crate::logical_plan::WindowFunction::NthValue { expr, n } => {
                            crate::logical_plan::WindowFunction::NthValue {
                                expr: rewrite_expr(expr, rewrite),
                                n,
                            }
                        }
                        other => other,
                    };
                    w
                })
                .collect(),
            input: Box::new(rewrite_plan_exprs(*input, rewrite)),
        },
        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
            strategy_hint,
        } => LogicalPlan::Join {
            left: Box::new(rewrite_plan_exprs(*left, rewrite)),
            right: Box::new(rewrite_plan_exprs(*right, rewrite)),
            on,
            join_type,
            strategy_hint,
        },
        LogicalPlan::Limit { n, input } => LogicalPlan::Limit {
            n,
            input: Box::new(rewrite_plan_exprs(*input, rewrite)),
        },
        LogicalPlan::TopKByScore {
            score_expr,
            k,
            input,
        } => LogicalPlan::TopKByScore {
            score_expr: rewrite_expr(score_expr, rewrite),
            k,
            input: Box::new(rewrite_plan_exprs(*input, rewrite)),
        },
        LogicalPlan::VectorTopK {
            table,
            query_vector,
            k,
            filter,
        } => LogicalPlan::VectorTopK {
            table,
            query_vector,
            k,
            filter,
        },
        LogicalPlan::InsertInto {
            table,
            columns,
            input,
        } => LogicalPlan::InsertInto {
            table,
            columns,
            input: Box::new(rewrite_plan_exprs(*input, rewrite)),
        },
        LogicalPlan::UnionAll { left, right } => LogicalPlan::UnionAll {
            left: Box::new(rewrite_plan_exprs(*left, rewrite)),
            right: Box::new(rewrite_plan_exprs(*right, rewrite)),
        },
        LogicalPlan::CteRef { name, plan } => LogicalPlan::CteRef {
            name,
            plan: Box::new(rewrite_plan_exprs(*plan, rewrite)),
        },
        s @ LogicalPlan::TableScan { .. } => s,
    }
}

fn rewrite_expr(e: Expr, rewrite: &dyn Fn(Expr) -> Expr) -> Expr {
    let e = match e {
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(rewrite_expr(*left, rewrite)),
            op,
            right: Box::new(rewrite_expr(*right, rewrite)),
        },
        Expr::And(a, b) => Expr::And(
            Box::new(rewrite_expr(*a, rewrite)),
            Box::new(rewrite_expr(*b, rewrite)),
        ),
        Expr::Or(a, b) => Expr::Or(
            Box::new(rewrite_expr(*a, rewrite)),
            Box::new(rewrite_expr(*b, rewrite)),
        ),
        Expr::Not(x) => Expr::Not(Box::new(rewrite_expr(*x, rewrite))),
        Expr::IsNull(x) => Expr::IsNull(Box::new(rewrite_expr(*x, rewrite))),
        Expr::IsNotNull(x) => Expr::IsNotNull(Box::new(rewrite_expr(*x, rewrite))),
        Expr::Cast { expr, to_type } => Expr::Cast {
            expr: Box::new(rewrite_expr(*expr, rewrite)),
            to_type,
        },
        Expr::CaseWhen { branches, else_expr } => Expr::CaseWhen {
            branches: branches
                .into_iter()
                .map(|(c, v)| (rewrite_expr(c, rewrite), rewrite_expr(v, rewrite)))
                .collect(),
            else_expr: else_expr.map(|e| Box::new(rewrite_expr(*e, rewrite))),
        },
        #[cfg(feature = "vector")]
        Expr::CosineSimilarity { vector, query } => Expr::CosineSimilarity {
            vector: Box::new(rewrite_expr(*vector, rewrite)),
            query: Box::new(rewrite_expr(*query, rewrite)),
        },
        #[cfg(feature = "vector")]
        Expr::L2Distance { vector, query } => Expr::L2Distance {
            vector: Box::new(rewrite_expr(*vector, rewrite)),
            query: Box::new(rewrite_expr(*query, rewrite)),
        },
        #[cfg(feature = "vector")]
        Expr::DotProduct { vector, query } => Expr::DotProduct {
            vector: Box::new(rewrite_expr(*vector, rewrite)),
            query: Box::new(rewrite_expr(*query, rewrite)),
        },
        Expr::ScalarUdf { name, args } => Expr::ScalarUdf {
            name,
            args: args
                .into_iter()
                .map(|arg| rewrite_expr(arg, rewrite))
                .collect(),
        },
        other => other,
    };
    rewrite(e)
}

fn split_conjuncts(e: Expr) -> Vec<Expr> {
    match e {
        Expr::And(a, b) => {
            let mut v = split_conjuncts(*a);
            v.extend(split_conjuncts(*b));
            v
        }
        other => vec![other],
    }
}

fn combine_conjuncts(mut v: Vec<Expr>) -> Expr {
    if v.is_empty() {
        return Expr::Literal(LiteralValue::Boolean(true));
    }
    let first = v.remove(0);
    v.into_iter()
        .fold(first, |acc, e| Expr::And(Box::new(acc), Box::new(e)))
}

fn projection_is_passthrough(exprs: &[(Expr, String)]) -> bool {
    exprs.iter().all(|(e, name)| match e {
        Expr::Column(c) => c == name,
        Expr::ColumnRef { name: c, .. } => c == name,
        _ => false,
    })
}

fn expr_columns(e: &Expr) -> HashSet<String> {
    let mut out = HashSet::new();
    collect_cols(e, &mut out);
    out
}

fn collect_cols(e: &Expr, out: &mut HashSet<String>) {
    match e {
        Expr::Column(c) => {
            out.insert(c.clone());
        }
        Expr::ColumnRef { name, .. } => {
            out.insert(name.clone());
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_cols(left, out);
            collect_cols(right, out);
        }
        Expr::And(a, b) | Expr::Or(a, b) => {
            collect_cols(a, out);
            collect_cols(b, out);
        }
        Expr::Not(x)
        | Expr::IsNull(x)
        | Expr::IsNotNull(x)
        | Expr::Cast { expr: x, .. } => {
            collect_cols(x, out);
        }
        Expr::CaseWhen { branches, else_expr } => {
            for (cond, value) in branches {
                collect_cols(cond, out);
                collect_cols(value, out);
            }
            if let Some(e) = else_expr {
                collect_cols(e, out);
            }
        }
        Expr::Literal(_) => {}
        Expr::ScalarUdf { args, .. } => {
            for arg in args {
                collect_cols(arg, out);
            }
        }
        #[cfg(feature = "vector")]
        Expr::CosineSimilarity { vector, query }
        | Expr::L2Distance { vector, query }
        | Expr::DotProduct { vector, query } => {
            collect_cols(vector, out);
            collect_cols(query, out);
        }
    }
}

fn expr_contains_case(e: &Expr) -> bool {
    match e {
        Expr::CaseWhen { .. } => true,
        Expr::BinaryOp { left, right, .. } => expr_contains_case(left) || expr_contains_case(right),
        Expr::And(a, b) | Expr::Or(a, b) => expr_contains_case(a) || expr_contains_case(b),
        Expr::Not(x)
        | Expr::IsNull(x)
        | Expr::IsNotNull(x)
        | Expr::Cast { expr: x, .. } => expr_contains_case(x),
        Expr::ScalarUdf { args, .. } => args.iter().any(expr_contains_case),
        #[cfg(feature = "vector")]
        Expr::CosineSimilarity { vector, query }
        | Expr::L2Distance { vector, query }
        | Expr::DotProduct { vector, query } => expr_contains_case(vector) || expr_contains_case(query),
        Expr::Column(_) | Expr::ColumnRef { .. } | Expr::Literal(_) => false,
    }
}

fn agg_columns(agg: &crate::logical_plan::AggExpr) -> HashSet<String> {
    match agg {
        crate::logical_plan::AggExpr::Count(e)
        | crate::logical_plan::AggExpr::Sum(e)
        | crate::logical_plan::AggExpr::Min(e)
        | crate::logical_plan::AggExpr::Max(e)
        | crate::logical_plan::AggExpr::Avg(e) => expr_columns(e),
    }
}

fn strip_qual(s: &str) -> String {
    if let Some((_t, c)) = s.split_once('.') {
        c.to_string()
    } else {
        s.to_string()
    }
}

fn plan_output_columns(plan: &LogicalPlan, ctx: &dyn OptimizerContext) -> Result<HashSet<String>> {
    match plan {
        LogicalPlan::TableScan {
            table, projection, ..
        } => {
            let schema = ctx.table_schema(table)?;
            let mut set = HashSet::new();
            if let Some(cols) = projection {
                for c in cols {
                    set.insert(c.clone());
                }
            } else {
                for f in schema.fields() {
                    set.insert(f.name().to_string());
                }
            }
            Ok(set)
        }
        LogicalPlan::Filter { input, .. } => plan_output_columns(input, ctx),
        LogicalPlan::InSubqueryFilter { input, .. } => plan_output_columns(input, ctx),
        LogicalPlan::ExistsSubqueryFilter { input, .. } => plan_output_columns(input, ctx),
        LogicalPlan::ScalarSubqueryFilter { input, .. } => plan_output_columns(input, ctx),
        LogicalPlan::Limit { input, .. } => plan_output_columns(input, ctx),
        LogicalPlan::TopKByScore { input, .. } => plan_output_columns(input, ctx),
        LogicalPlan::Projection { exprs, .. } => Ok(exprs.iter().map(|(_, n)| n.clone()).collect()),
        LogicalPlan::Aggregate { .. } => Ok(HashSet::new()), // v1: conservative
        LogicalPlan::Window { exprs, input } => {
            let mut cols = plan_output_columns(input, ctx)?;
            for w in exprs {
                cols.insert(w.output_name.clone());
            }
            Ok(cols)
        }
        LogicalPlan::VectorTopK { .. } => Ok(["id", "score", "payload"]
            .into_iter()
            .map(std::string::ToString::to_string)
            .collect()),
        LogicalPlan::Join {
            left,
            right,
            join_type,
            ..
        } => {
            let mut l = plan_output_columns(left, ctx)?;
            if !matches!(join_type, JoinType::Semi | JoinType::Anti) {
                let r = plan_output_columns(right, ctx)?;
                l.extend(r);
            }
            Ok(l)
        }
        LogicalPlan::InsertInto { input, .. } => plan_output_columns(input, ctx),
        LogicalPlan::UnionAll { left, .. } => plan_output_columns(left, ctx),
        LogicalPlan::CteRef { plan, .. } => plan_output_columns(plan, ctx),
    }
}

fn estimate_bytes(plan: &LogicalPlan, ctx: &dyn OptimizerContext) -> Result<Option<u64>> {
    match plan {
        LogicalPlan::TableScan { table, .. } => {
            let (bytes, rows) = ctx.table_stats(table)?;
            if let Some(b) = bytes {
                return Ok(Some(b));
            }
            if let Some(r) = rows {
                return Ok(Some(r.saturating_mul(100)));
            } // heuristic row size
            Ok(None)
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::InSubqueryFilter { input, .. }
        | LogicalPlan::ExistsSubqueryFilter { input, .. }
        | LogicalPlan::ScalarSubqueryFilter { input, .. }
        | LogicalPlan::Projection { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::TopKByScore { input, .. }
        | LogicalPlan::UnionAll { left: input, .. }
        | LogicalPlan::CteRef { plan: input, .. }
        | LogicalPlan::InsertInto { input, .. } => estimate_bytes(input, ctx),
        LogicalPlan::VectorTopK { .. } => Ok(None),
        LogicalPlan::Join { .. } => Ok(None),
    }
}

#[cfg(all(test, feature = "vector"))]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema, SchemaRef};

    use super::{Optimizer, OptimizerConfig, OptimizerContext, TableMetadata};
    use crate::analyzer::SchemaProvider;
    use crate::explain::explain_logical;
    use crate::logical_plan::{Expr, JoinStrategyHint, LiteralValue, LogicalPlan};

    struct TestCtx {
        schema: SchemaRef,
        format: String,
    }

    impl SchemaProvider for TestCtx {
        fn table_schema(&self, _table: &str) -> ffq_common::Result<SchemaRef> {
            Ok(self.schema.clone())
        }
    }

    impl OptimizerContext for TestCtx {
        fn table_stats(&self, _table: &str) -> ffq_common::Result<(Option<u64>, Option<u64>)> {
            Ok((None, None))
        }

        fn table_metadata(&self, _table: &str) -> ffq_common::Result<Option<TableMetadata>> {
            Ok(Some(TableMetadata {
                format: self.format.clone(),
                options: HashMap::new(),
            }))
        }
    }

    fn topk_plan(select_cols: &[&str]) -> LogicalPlan {
        LogicalPlan::Projection {
            exprs: select_cols
                .iter()
                .map(|c| (Expr::Column((*c).to_string()), (*c).to_string()))
                .collect(),
            input: Box::new(LogicalPlan::TopKByScore {
                score_expr: Expr::CosineSimilarity {
                    vector: Box::new(Expr::Column("emb".to_string())),
                    query: Box::new(Expr::Literal(LiteralValue::VectorF32(vec![1.0, 0.0, 0.0]))),
                },
                k: 5,
                input: Box::new(LogicalPlan::TableScan {
                    table: "docs_idx".to_string(),
                    projection: None,
                    filters: vec![],
                }),
            }),
        }
    }

    #[test]
    fn rewrites_qdrant_topk_to_vector_topk() {
        let emb_field = Field::new("item", DataType::Float32, true);
        let ctx = TestCtx {
            schema: Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("payload", DataType::Utf8, true),
                Field::new("emb", DataType::FixedSizeList(Arc::new(emb_field), 3), true),
            ])),
            format: "qdrant".to_string(),
        };

        let optimized = Optimizer::new()
            .optimize(
                topk_plan(&["id", "score", "payload"]),
                &ctx,
                OptimizerConfig::default(),
            )
            .expect("optimize");
        match optimized {
            LogicalPlan::Projection { input, .. } => match *input {
                LogicalPlan::VectorTopK {
                    table,
                    query_vector,
                    k,
                    ..
                } => {
                    assert_eq!(table, "docs_idx");
                    assert_eq!(query_vector, vec![1.0, 0.0, 0.0]);
                    assert_eq!(k, 5);
                }
                other => panic!("expected VectorTopK, got {other:?}"),
            },
            other => panic!("expected Projection, got {other:?}"),
        }
    }

    #[test]
    fn does_not_rewrite_non_qdrant_format() {
        let emb_field = Field::new("item", DataType::Float32, true);
        let ctx = TestCtx {
            schema: Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("payload", DataType::Utf8, true),
                Field::new("emb", DataType::FixedSizeList(Arc::new(emb_field), 3), true),
            ])),
            format: "parquet".to_string(),
        };

        let optimized = Optimizer::new()
            .optimize(
                topk_plan(&["id", "score", "payload"]),
                &ctx,
                OptimizerConfig::default(),
            )
            .expect("optimize");
        match optimized {
            LogicalPlan::Projection { input, .. } => match *input {
                LogicalPlan::TopKByScore { .. } => {}
                other => panic!("expected TopKByScore fallback, got {other:?}"),
            },
            other => panic!("expected Projection, got {other:?}"),
        }
    }

    #[test]
    fn does_not_rewrite_when_projection_needs_unsupported_column() {
        let emb_field = Field::new("item", DataType::Float32, true);
        let ctx = TestCtx {
            schema: Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("title", DataType::Utf8, true),
                Field::new("payload", DataType::Utf8, true),
                Field::new("emb", DataType::FixedSizeList(Arc::new(emb_field), 3), true),
            ])),
            format: "qdrant".to_string(),
        };

        let optimized = Optimizer::new()
            .optimize(topk_plan(&["title"]), &ctx, OptimizerConfig::default())
            .expect("optimize");
        match optimized {
            LogicalPlan::Projection { input, .. } => match *input {
                LogicalPlan::TopKByScore { .. } => {}
                other => panic!("expected TopKByScore fallback, got {other:?}"),
            },
            other => panic!("expected Projection, got {other:?}"),
        }
    }

    #[test]
    fn rewrites_with_translated_filter_pushdown() {
        let emb_field = Field::new("item", DataType::Float32, true);
        let ctx = TestCtx {
            schema: Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("payload", DataType::Utf8, true),
                Field::new("emb", DataType::FixedSizeList(Arc::new(emb_field), 3), true),
            ])),
            format: "qdrant".to_string(),
        };

        let plan = LogicalPlan::Projection {
            exprs: vec![
                (Expr::Column("id".to_string()), "id".to_string()),
                (Expr::Column("score".to_string()), "score".to_string()),
                (Expr::Column("payload".to_string()), "payload".to_string()),
            ],
            input: Box::new(LogicalPlan::TopKByScore {
                score_expr: Expr::CosineSimilarity {
                    vector: Box::new(Expr::Column("emb".to_string())),
                    query: Box::new(Expr::Literal(LiteralValue::VectorF32(vec![1.0, 0.0, 0.0]))),
                },
                k: 3,
                input: Box::new(LogicalPlan::TableScan {
                    table: "docs_idx".to_string(),
                    projection: None,
                    filters: vec![Expr::And(
                        Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Column("tenant_id".to_string())),
                            op: crate::logical_plan::BinaryOp::Eq,
                            right: Box::new(Expr::Literal(LiteralValue::Int64(7))),
                        }),
                        Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Column("lang".to_string())),
                            op: crate::logical_plan::BinaryOp::Eq,
                            right: Box::new(Expr::Literal(LiteralValue::Utf8("en".to_string()))),
                        }),
                    )],
                }),
            }),
        };

        let optimized = Optimizer::new()
            .optimize(plan, &ctx, OptimizerConfig::default())
            .expect("optimize");
        match optimized {
            LogicalPlan::Projection { input, .. } => match *input {
                LogicalPlan::VectorTopK { filter, .. } => {
                    let filter = filter.expect("translated filter");
                    let parsed: serde_json::Value =
                        serde_json::from_str(&filter).expect("json filter");
                    assert_eq!(
                        parsed
                            .get("must")
                            .and_then(|v| v.as_array())
                            .map(|a| a.len())
                            .unwrap_or_default(),
                        2
                    );
                }
                other => panic!("expected VectorTopK, got {other:?}"),
            },
            other => panic!("expected Projection, got {other:?}"),
        }
    }

    #[test]
    fn unsupported_filter_shape_falls_back_without_error() {
        let emb_field = Field::new("item", DataType::Float32, true);
        let ctx = TestCtx {
            schema: Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("payload", DataType::Utf8, true),
                Field::new("emb", DataType::FixedSizeList(Arc::new(emb_field), 3), true),
            ])),
            format: "qdrant".to_string(),
        };

        let plan = LogicalPlan::Projection {
            exprs: vec![
                (Expr::Column("id".to_string()), "id".to_string()),
                (Expr::Column("score".to_string()), "score".to_string()),
                (Expr::Column("payload".to_string()), "payload".to_string()),
            ],
            input: Box::new(LogicalPlan::TopKByScore {
                score_expr: Expr::CosineSimilarity {
                    vector: Box::new(Expr::Column("emb".to_string())),
                    query: Box::new(Expr::Literal(LiteralValue::VectorF32(vec![1.0, 0.0, 0.0]))),
                },
                k: 3,
                input: Box::new(LogicalPlan::TableScan {
                    table: "docs_idx".to_string(),
                    projection: None,
                    // unsupported for v1 translator: non-equality predicate
                    filters: vec![Expr::BinaryOp {
                        left: Box::new(Expr::Column("tenant_id".to_string())),
                        op: crate::logical_plan::BinaryOp::Gt,
                        right: Box::new(Expr::Literal(LiteralValue::Int64(7))),
                    }],
                }),
            }),
        };

        let optimized = Optimizer::new()
            .optimize(plan, &ctx, OptimizerConfig::default())
            .expect("optimize should not fail");
        match optimized {
            LogicalPlan::Projection { input, .. } => match *input {
                LogicalPlan::TopKByScore { .. } => {}
                other => panic!("expected TopKByScore fallback, got {other:?}"),
            },
            other => panic!("expected Projection, got {other:?}"),
        }
    }

    #[test]
    fn explain_marks_index_rewrite_state() {
        let emb_field = Field::new("item", DataType::Float32, true);
        let qdrant_ctx = TestCtx {
            schema: Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("payload", DataType::Utf8, true),
                Field::new(
                    "emb",
                    DataType::FixedSizeList(Arc::new(emb_field.clone()), 3),
                    true,
                ),
            ])),
            format: "qdrant".to_string(),
        };
        let parquet_ctx = TestCtx {
            schema: Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("payload", DataType::Utf8, true),
                Field::new("emb", DataType::FixedSizeList(Arc::new(emb_field), 3), true),
            ])),
            format: "parquet".to_string(),
        };

        let applied = Optimizer::new()
            .optimize(
                topk_plan(&["id", "score", "payload"]),
                &qdrant_ctx,
                OptimizerConfig::default(),
            )
            .expect("opt");
        let fallback = Optimizer::new()
            .optimize(
                topk_plan(&["id", "score", "payload"]),
                &parquet_ctx,
                OptimizerConfig::default(),
            )
            .expect("opt");

        let applied_s = explain_logical(&applied);
        let fallback_s = explain_logical(&fallback);
        assert!(applied_s.contains("rewrite=index_applied"));
        assert!(fallback_s.contains("rewrite=index_fallback"));
    }

    #[test]
    fn rewrites_to_two_phase_vector_join_rerank_pipeline() {
        let emb_field = Field::new("item", DataType::Float32, true);
        let docs_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("title", DataType::Utf8, true),
            Field::new("lang", DataType::Utf8, true),
            Field::new(
                "emb",
                DataType::FixedSizeList(Arc::new(emb_field.clone()), 3),
                true,
            ),
        ]));
        let idx_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("score", DataType::Float32, false),
            Field::new("payload", DataType::Utf8, true),
        ]));

        struct Ctx {
            schemas: HashMap<String, SchemaRef>,
            meta: HashMap<String, TableMetadata>,
        }
        impl SchemaProvider for Ctx {
            fn table_schema(&self, table: &str) -> ffq_common::Result<SchemaRef> {
                self.schemas
                    .get(table)
                    .cloned()
                    .ok_or_else(|| ffq_common::FfqError::Planning(format!("unknown table {table}")))
            }
        }
        impl OptimizerContext for Ctx {
            fn table_stats(&self, _table: &str) -> ffq_common::Result<(Option<u64>, Option<u64>)> {
                Ok((None, None))
            }
            fn table_metadata(&self, table: &str) -> ffq_common::Result<Option<TableMetadata>> {
                Ok(self.meta.get(table).cloned())
            }
        }

        let mut docs_opts = HashMap::new();
        docs_opts.insert("vector.index_table".to_string(), "docs_idx".to_string());
        docs_opts.insert("vector.id_column".to_string(), "id".to_string());
        docs_opts.insert("vector.embedding_column".to_string(), "emb".to_string());
        docs_opts.insert("vector.prefetch_multiplier".to_string(), "3".to_string());
        let ctx = Ctx {
            schemas: HashMap::from([
                ("docs".to_string(), docs_schema),
                ("docs_idx".to_string(), idx_schema),
            ]),
            meta: HashMap::from([
                (
                    "docs".to_string(),
                    TableMetadata {
                        format: "parquet".to_string(),
                        options: docs_opts,
                    },
                ),
                (
                    "docs_idx".to_string(),
                    TableMetadata {
                        format: "qdrant".to_string(),
                        options: HashMap::new(),
                    },
                ),
            ]),
        };

        let plan = LogicalPlan::Projection {
            exprs: vec![
                (Expr::Column("id".to_string()), "id".to_string()),
                (Expr::Column("title".to_string()), "title".to_string()),
            ],
            input: Box::new(LogicalPlan::TopKByScore {
                score_expr: Expr::CosineSimilarity {
                    vector: Box::new(Expr::Column("emb".to_string())),
                    query: Box::new(Expr::Literal(LiteralValue::VectorF32(vec![1.0, 0.0, 0.0]))),
                },
                k: 2,
                input: Box::new(LogicalPlan::TableScan {
                    table: "docs".to_string(),
                    projection: None,
                    filters: vec![Expr::BinaryOp {
                        left: Box::new(Expr::Column("lang".to_string())),
                        op: crate::logical_plan::BinaryOp::Eq,
                        right: Box::new(Expr::Literal(LiteralValue::Utf8("en".to_string()))),
                    }],
                }),
            }),
        };

        let optimized = Optimizer::new()
            .optimize(plan, &ctx, OptimizerConfig::default())
            .expect("optimize");
        match optimized {
            LogicalPlan::Projection { input, .. } => match *input {
                LogicalPlan::TopKByScore { input, k, .. } => {
                    assert_eq!(k, 2);
                    match *input {
                        LogicalPlan::Filter { input, .. } => match *input {
                            LogicalPlan::Projection { input, .. } => match *input {
                                LogicalPlan::Join {
                                    left,
                                    right,
                                    on,
                                    strategy_hint,
                                    ..
                                } => {
                                    assert_eq!(on, vec![("id".to_string(), "id".to_string())]);
                                    assert_eq!(strategy_hint, JoinStrategyHint::BroadcastRight);
                                    match *left {
                                        LogicalPlan::TableScan { table, .. } => {
                                            assert_eq!(table, "docs")
                                        }
                                        other => panic!("expected docs TableScan, got {other:?}"),
                                    }
                                    match *right {
                                        LogicalPlan::VectorTopK { table, k, .. } => {
                                            assert_eq!(table, "docs_idx");
                                            assert_eq!(k, 6);
                                        }
                                        other => panic!("expected VectorTopK, got {other:?}"),
                                    }
                                }
                                other => panic!("expected Join, got {other:?}"),
                            },
                            other => panic!("expected post-join projection, got {other:?}"),
                        },
                        other => panic!("expected post-join Filter, got {other:?}"),
                    }
                }
                other => panic!("expected TopKByScore, got {other:?}"),
            },
            other => panic!("expected Projection, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod subquery_integration_tests {
    use std::collections::HashMap;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema, SchemaRef};

    use super::{Optimizer, OptimizerConfig, OptimizerContext, TableMetadata};
    use crate::analyzer::SchemaProvider;
    use crate::logical_plan::{Expr, JoinStrategyHint, JoinType, LogicalPlan, SubqueryCorrelation};

    struct Ctx {
        schemas: HashMap<String, SchemaRef>,
    }

    impl SchemaProvider for Ctx {
        fn table_schema(&self, table: &str) -> ffq_common::Result<SchemaRef> {
            self.schemas
                .get(table)
                .cloned()
                .ok_or_else(|| ffq_common::FfqError::Planning(format!("unknown table {table}")))
        }
    }

    impl OptimizerContext for Ctx {
        fn table_stats(&self, table: &str) -> ffq_common::Result<(Option<u64>, Option<u64>)> {
            if table == "bad_stats" {
                return Err(ffq_common::FfqError::Planning(
                    "table stats unavailable".to_string(),
                ));
            }
            Ok((Some(1024), Some(10)))
        }

        fn table_metadata(&self, _table: &str) -> ffq_common::Result<Option<TableMetadata>> {
            Ok(None)
        }
    }

    fn basic_schema(col: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(col, DataType::Int64, true)]))
    }

    #[test]
    fn predicate_pushdown_through_in_subquery_filter_pushes_left_branch() {
        let ctx = Ctx {
            schemas: HashMap::from([
                ("t".to_string(), basic_schema("a")),
                ("s".to_string(), basic_schema("b")),
            ]),
        };
        let plan = LogicalPlan::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Column("a".to_string())),
                op: crate::logical_plan::BinaryOp::Gt,
                right: Box::new(Expr::Literal(crate::logical_plan::LiteralValue::Int64(1))),
            },
            input: Box::new(LogicalPlan::InSubqueryFilter {
                input: Box::new(LogicalPlan::TableScan {
                    table: "t".to_string(),
                    projection: None,
                    filters: vec![],
                }),
                expr: Expr::Column("a".to_string()),
                subquery: Box::new(LogicalPlan::TableScan {
                    table: "s".to_string(),
                    projection: None,
                    filters: vec![],
                }),
                negated: false,
                correlation: SubqueryCorrelation::Unresolved,
            }),
        };

        let optimized = Optimizer::new()
            .optimize(plan, &ctx, OptimizerConfig::default())
            .expect("optimize");

        match optimized {
            LogicalPlan::InSubqueryFilter { input, .. } => match *input {
                LogicalPlan::TableScan { filters, .. } => {
                    assert_eq!(filters.len(), 1, "expected pushed filter at scan");
                }
                other => panic!("expected left branch TableScan with pushed filter, got {other:?}"),
            },
            other => panic!("expected InSubqueryFilter root after pushdown, got {other:?}"),
        }
    }

    #[test]
    fn optimizer_returns_error_instead_of_panicking_when_child_rewrite_fails() {
        let ctx = Ctx {
            schemas: HashMap::from([
                ("ok".to_string(), basic_schema("k")),
                ("bad_stats".to_string(), basic_schema("k")),
            ]),
        };
        let plan = LogicalPlan::Projection {
            exprs: vec![(Expr::Column("k".to_string()), "k".to_string())],
            input: Box::new(LogicalPlan::Join {
                left: Box::new(LogicalPlan::TableScan {
                    table: "ok".to_string(),
                    projection: None,
                    filters: vec![],
                }),
                right: Box::new(LogicalPlan::TableScan {
                    table: "bad_stats".to_string(),
                    projection: None,
                    filters: vec![],
                }),
                on: vec![("k".to_string(), "k".to_string())],
                join_type: JoinType::Inner,
                strategy_hint: JoinStrategyHint::Auto,
            }),
        };

        let result = catch_unwind(AssertUnwindSafe(|| {
            Optimizer::new().optimize(plan, &ctx, OptimizerConfig::default())
        }));
        assert!(result.is_ok(), "optimizer should not panic");
        let out = result.expect("no panic");
        assert!(out.is_err(), "optimizer should propagate planning error");
    }
}
