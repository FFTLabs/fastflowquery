use ffq_common::Result;
use std::collections::{HashMap, HashSet};

use crate::analyzer::SchemaProvider;
use crate::logical_plan::{BinaryOp, Expr, JoinStrategyHint, JoinType, LiteralValue, LogicalPlan};

#[derive(Debug, Clone, Copy)]
pub struct OptimizerConfig {
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
pub struct TableMetadata {
    pub format: String,
    pub options: HashMap<String, String>,
}

/// Provide table stats for join hinting + schemas for pushdown decisions.
pub trait OptimizerContext: SchemaProvider {
    fn table_stats(&self, table: &str) -> Result<(Option<u64>, Option<u64>)>; // (bytes, rows)

    fn table_metadata(&self, _table: &str) -> Result<Option<TableMetadata>> {
        Ok(None)
    }

    fn table_format(&self, table: &str) -> Result<Option<String>> {
        Ok(self.table_metadata(table)?.map(|m| m.format))
    }

    fn table_options(&self, table: &str) -> Result<Option<HashMap<String, String>>> {
        Ok(self.table_metadata(table)?.map(|m| m.options))
    }
}

#[derive(Debug, Default)]
pub struct Optimizer;

impl Optimizer {
    pub fn new() -> Self {
        Self
    }

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
        let plan = vector_index_rewrite(plan, ctx)?;

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

        other => Ok(map_children(other, |p| predicate_pushdown(p, ctx).unwrap())),
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
        other => Ok(map_children(other, |p| {
            join_strategy_hint(p, ctx, cfg).unwrap()
        })),
    }
}

fn vector_index_rewrite(plan: LogicalPlan, ctx: &dyn OptimizerContext) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Filter { predicate, input } => Ok(LogicalPlan::Filter {
            predicate,
            input: Box::new(vector_index_rewrite(*input, ctx)?),
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
    if !projection_supported_for_vector_topk(exprs) {
        return Ok(None);
    }
    let LogicalPlan::TopKByScore {
        score_expr,
        k,
        input,
    } = input
    else {
        return Ok(None);
    };
    if *k == 0 {
        return Ok(None);
    }
    let LogicalPlan::TableScan { table, filters, .. } = input.as_ref() else {
        return Ok(None);
    };
    if ctx.table_format(table)?.as_deref() != Some("qdrant") {
        return Ok(None);
    }
    let Expr::CosineSimilarity { vector, query } = score_expr else {
        return Ok(None);
    };
    if !matches!(vector.as_ref(), Expr::Column(_) | Expr::ColumnRef { .. }) {
        return Ok(None);
    }
    let Expr::Literal(LiteralValue::VectorF32(query_vector)) = query.as_ref() else {
        return Ok(None);
    };
    let filter = match translate_qdrant_filter(filters) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    Ok(Some(LogicalPlan::VectorTopK {
        table: table.clone(),
        query_vector: query_vector.clone(),
        k: *k,
        filter,
    }))
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
        s @ LogicalPlan::TableScan { .. } => s,
    }
}

fn rewrite_plan_exprs(plan: LogicalPlan, rewrite: &dyn Fn(Expr) -> Expr) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
            predicate: rewrite_expr(predicate, rewrite),
            input: Box::new(rewrite_plan_exprs(*input, rewrite)),
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
        Expr::Cast { expr, to_type } => Expr::Cast {
            expr: Box::new(rewrite_expr(*expr, rewrite)),
            to_type,
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
        Expr::Not(x) | Expr::Cast { expr: x, .. } => {
            collect_cols(x, out);
        }
        Expr::Literal(_) => {}
        #[cfg(feature = "vector")]
        Expr::CosineSimilarity { vector, query }
        | Expr::L2Distance { vector, query }
        | Expr::DotProduct { vector, query } => {
            collect_cols(vector, out);
            collect_cols(query, out);
        }
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
        LogicalPlan::Limit { input, .. } => plan_output_columns(input, ctx),
        LogicalPlan::TopKByScore { input, .. } => plan_output_columns(input, ctx),
        LogicalPlan::Projection { exprs, .. } => Ok(exprs.iter().map(|(_, n)| n.clone()).collect()),
        LogicalPlan::Aggregate { .. } => Ok(HashSet::new()), // v1: conservative
        LogicalPlan::VectorTopK { .. } => Ok(["id", "score", "payload"]
            .into_iter()
            .map(std::string::ToString::to_string)
            .collect()),
        LogicalPlan::Join { left, right, .. } => {
            let mut l = plan_output_columns(left, ctx)?;
            let r = plan_output_columns(right, ctx)?;
            l.extend(r);
            Ok(l)
        }
        LogicalPlan::InsertInto { input, .. } => plan_output_columns(input, ctx),
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
        | LogicalPlan::Projection { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::TopKByScore { input, .. }
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
    use crate::logical_plan::{Expr, LiteralValue, LogicalPlan};

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
}
