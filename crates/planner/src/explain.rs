use crate::logical_plan::{
    Expr, JoinStrategyHint, LogicalPlan, SubqueryCorrelation, WindowExpr, WindowFrameBound,
    WindowFrameExclusion, WindowFrameSpec, WindowFrameUnits, WindowFunction,
};
use crate::physical_plan::{ExchangeExec, PartitioningSpec, PhysicalPlan};
use std::collections::HashMap;

/// Render logical plan as human-readable multiline text.
pub fn explain_logical(plan: &LogicalPlan) -> String {
    let mut s = String::new();
    fmt_plan(plan, 0, &mut s);
    s
}

/// Render physical plan as human-readable multiline text.
pub fn explain_physical(plan: &PhysicalPlan) -> String {
    let mut s = String::new();
    fmt_physical(plan, 0, &mut s);
    s
}

fn fmt_plan(plan: &LogicalPlan, indent: usize, out: &mut String) {
    let pad = "  ".repeat(indent);
    match plan {
        LogicalPlan::TableScan {
            table,
            projection,
            filters,
        } => {
            out.push_str(&format!("{pad}TableScan table={table}\n"));
            out.push_str(&format!("{pad}  projection={:?}\n", projection));
            out.push_str(&format!("{pad}  pushed_filters={}\n", filters.len()));
            for f in filters {
                out.push_str(&format!("{pad}    {}\n", fmt_expr(f)));
            }
        }
        LogicalPlan::Filter { predicate, input } => {
            out.push_str(&format!("{pad}Filter {}\n", fmt_expr(predicate)));
            fmt_plan(input, indent + 1, out);
        }
        LogicalPlan::InSubqueryFilter {
            input,
            expr,
            subquery,
            negated,
            correlation,
        } => {
            out.push_str(&format!(
                "{pad}InSubqueryFilter negated={negated} correlation={} rewrite=none expr={}\n",
                fmt_subquery_correlation(correlation),
                fmt_expr(expr),
            ));
            out.push_str(&format!("{pad}  input:\n"));
            fmt_plan(input, indent + 2, out);
            out.push_str(&format!("{pad}  subquery:\n"));
            fmt_plan(subquery, indent + 2, out);
        }
        LogicalPlan::ExistsSubqueryFilter {
            input,
            subquery,
            negated,
            correlation,
        } => {
            out.push_str(&format!(
                "{pad}ExistsSubqueryFilter negated={negated} correlation={} rewrite=none\n",
                fmt_subquery_correlation(correlation)
            ));
            out.push_str(&format!("{pad}  input:\n"));
            fmt_plan(input, indent + 2, out);
            out.push_str(&format!("{pad}  subquery:\n"));
            fmt_plan(subquery, indent + 2, out);
        }
        LogicalPlan::ScalarSubqueryFilter {
            input,
            expr,
            op,
            subquery,
            correlation,
        } => {
            out.push_str(&format!(
                "{pad}ScalarSubqueryFilter correlation={} rewrite=none expr={} op={op:?}\n",
                fmt_subquery_correlation(correlation),
                fmt_expr(expr),
            ));
            out.push_str(&format!("{pad}  input:\n"));
            fmt_plan(input, indent + 2, out);
            out.push_str(&format!("{pad}  subquery:\n"));
            fmt_plan(subquery, indent + 2, out);
        }
        LogicalPlan::Projection { exprs, input } => {
            out.push_str(&format!("{pad}Projection\n"));
            for (e, name) in exprs {
                out.push_str(&format!("{pad}  {name} := {}\n", fmt_expr(e)));
            }
            fmt_plan(input, indent + 1, out);
        }
        LogicalPlan::Window { exprs, input } => {
            out.push_str(&format!("{pad}Window\n"));
            let window_groups = window_sort_reuse_groups(exprs);
            out.push_str(&format!(
                "{pad}  window_exprs={} sort_reuse_groups={}\n",
                exprs.len(),
                window_groups.len()
            ));
            for (gidx, group) in window_groups.iter().enumerate() {
                out.push_str(&format!(
                    "{pad}  group[{gidx}] partition=[{}] order=[{}] windows=[{}]\n",
                    group.partition_display,
                    group.order_display,
                    group.window_names.join(", ")
                ));
            }
            for w in exprs {
                let func = match &w.func {
                    WindowFunction::RowNumber => "ROW_NUMBER()".to_string(),
                    WindowFunction::Rank => "RANK()".to_string(),
                    WindowFunction::DenseRank => "DENSE_RANK()".to_string(),
                    WindowFunction::PercentRank => "PERCENT_RANK()".to_string(),
                    WindowFunction::CumeDist => "CUME_DIST()".to_string(),
                    WindowFunction::Ntile(n) => format!("NTILE({n})"),
                    WindowFunction::Count(expr) => format!("COUNT({})", fmt_expr(expr)),
                    WindowFunction::Sum(expr) => format!("SUM({})", fmt_expr(expr)),
                    WindowFunction::Avg(expr) => format!("AVG({})", fmt_expr(expr)),
                    WindowFunction::Min(expr) => format!("MIN({})", fmt_expr(expr)),
                    WindowFunction::Max(expr) => format!("MAX({})", fmt_expr(expr)),
                    WindowFunction::Lag {
                        expr,
                        offset,
                        default,
                    } => match default {
                        Some(d) => format!("LAG({}, {}, {})", fmt_expr(expr), offset, fmt_expr(d)),
                        None => format!("LAG({}, {})", fmt_expr(expr), offset),
                    },
                    WindowFunction::Lead {
                        expr,
                        offset,
                        default,
                    } => match default {
                        Some(d) => format!("LEAD({}, {}, {})", fmt_expr(expr), offset, fmt_expr(d)),
                        None => format!("LEAD({}, {})", fmt_expr(expr), offset),
                    },
                    WindowFunction::FirstValue(expr) => {
                        format!("FIRST_VALUE({})", fmt_expr(expr))
                    }
                    WindowFunction::LastValue(expr) => {
                        format!("LAST_VALUE({})", fmt_expr(expr))
                    }
                    WindowFunction::NthValue { expr, n } => {
                        format!("NTH_VALUE({}, {n})", fmt_expr(expr))
                    }
                };
                let part = w
                    .partition_by
                    .iter()
                    .map(fmt_expr)
                    .collect::<Vec<_>>()
                    .join(", ");
                let ord = w
                    .order_by
                    .iter()
                    .map(|o| {
                        format!(
                            "{} {} NULLS {}",
                            fmt_expr(&o.expr),
                            if o.asc { "ASC" } else { "DESC" },
                            if o.nulls_first { "FIRST" } else { "LAST" }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                out.push_str(&format!(
                    "{pad}  {} := {} OVER (PARTITION BY [{}] ORDER BY [{}] FRAME {} )\n",
                    w.output_name,
                    func,
                    part,
                    ord,
                    fmt_window_frame_or_default(w)
                ));
            }
            fmt_plan(input, indent + 1, out);
        }
        LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => {
            out.push_str(&format!("{pad}Aggregate\n"));
            out.push_str(&format!("{pad}  group_by={}\n", group_exprs.len()));
            for g in group_exprs {
                out.push_str(&format!("{pad}    {}\n", fmt_expr(g)));
            }
            out.push_str(&format!("{pad}  aggs={}\n", aggr_exprs.len()));
            for (a, name) in aggr_exprs {
                out.push_str(&format!("{pad}    {name} := {:?}\n", a));
            }
            fmt_plan(input, indent + 1, out);
        }
        LogicalPlan::Join {
            on,
            join_type,
            strategy_hint,
            left,
            right,
        } => {
            let rewrite_suffix = join_rewrite_hint(plan)
                .map(|r| format!(" rewrite={r}"))
                .unwrap_or_default();
            out.push_str(&format!(
                "{pad}Join type={join_type:?} strategy={}{}\n",
                fmt_join_hint(*strategy_hint),
                rewrite_suffix,
            ));
            out.push_str(&format!("{pad}  on={:?}\n", on));
            out.push_str(&format!("{pad}  left:\n"));
            fmt_plan(left, indent + 2, out);
            out.push_str(&format!("{pad}  right:\n"));
            fmt_plan(right, indent + 2, out);
        }
        LogicalPlan::Limit { n, input } => {
            out.push_str(&format!("{pad}Limit n={n}\n"));
            fmt_plan(input, indent + 1, out);
        }
        LogicalPlan::TopKByScore {
            score_expr,
            k,
            input,
        } => {
            out.push_str(&format!(
                "{pad}TopKByScore k={k} score={} rewrite=index_fallback\n",
                fmt_expr(score_expr)
            ));
            fmt_plan(input, indent + 1, out);
        }
        LogicalPlan::UnionAll { left, right } => {
            out.push_str(&format!("{pad}UnionAll\n"));
            out.push_str(&format!("{pad}  left:\n"));
            fmt_plan(left, indent + 2, out);
            out.push_str(&format!("{pad}  right:\n"));
            fmt_plan(right, indent + 2, out);
        }
        LogicalPlan::CteRef { name, plan } => {
            out.push_str(&format!("{pad}CteRef name={name}\n"));
            fmt_plan(plan, indent + 1, out);
        }
        LogicalPlan::VectorTopK {
            table,
            query_vector,
            k,
            filter,
        } => {
            out.push_str(&format!(
                "{pad}VectorTopK table={table} k={k} query_dim={} filter={filter:?} rewrite=index_applied\n",
                query_vector.len()
            ));
        }
        LogicalPlan::HybridVectorScan {
            source,
            query_vectors,
            k,
            ef_search,
            prefilter,
            metric,
            provider,
        } => {
            let qdim = query_vectors.first().map_or(0, Vec::len);
            out.push_str(&format!(
                "{pad}HybridVectorScan source={source} k={k} ef_search={ef_search:?} query_dim={qdim} metric={metric} provider={provider} prefilter={prefilter:?} columns=[id,_score,payload] rewrite=index_applied\n"
            ));
        }
        LogicalPlan::InsertInto {
            table,
            columns,
            input,
        } => {
            out.push_str(&format!(
                "{pad}InsertInto table={table} columns={columns:?}\n"
            ));
            fmt_plan(input, indent + 1, out);
        }
    }
}

fn fmt_physical(plan: &PhysicalPlan, indent: usize, out: &mut String) {
    let pad = "  ".repeat(indent);
    match plan {
        PhysicalPlan::ParquetScan(scan) => {
            out.push_str(&format!("{pad}ParquetScan table={}\n", scan.table));
            out.push_str(&format!("{pad}  projection={:?}\n", scan.projection));
            out.push_str(&format!("{pad}  pushed_filters={}\n", scan.filters.len()));
        }
        PhysicalPlan::ParquetWrite(write) => {
            out.push_str(&format!("{pad}ParquetWrite table={}\n", write.table));
            fmt_physical(&write.input, indent + 1, out);
        }
        PhysicalPlan::Filter(filter) => {
            out.push_str(&format!("{pad}Filter {}\n", fmt_expr(&filter.predicate)));
            fmt_physical(&filter.input, indent + 1, out);
        }
        PhysicalPlan::InSubqueryFilter(exec) => {
            out.push_str(&format!("{pad}InSubqueryFilter negated={}\n", exec.negated));
            out.push_str(&format!("{pad}  expr={}\n", fmt_expr(&exec.expr)));
            out.push_str(&format!("{pad}  input:\n"));
            fmt_physical(&exec.input, indent + 2, out);
            out.push_str(&format!("{pad}  subquery:\n"));
            fmt_physical(&exec.subquery, indent + 2, out);
        }
        PhysicalPlan::ExistsSubqueryFilter(exec) => {
            out.push_str(&format!(
                "{pad}ExistsSubqueryFilter negated={}\n",
                exec.negated
            ));
            out.push_str(&format!("{pad}  input:\n"));
            fmt_physical(&exec.input, indent + 2, out);
            out.push_str(&format!("{pad}  subquery:\n"));
            fmt_physical(&exec.subquery, indent + 2, out);
        }
        PhysicalPlan::ScalarSubqueryFilter(exec) => {
            out.push_str(&format!(
                "{pad}ScalarSubqueryFilter expr={} op={:?}\n",
                fmt_expr(&exec.expr),
                exec.op
            ));
            out.push_str(&format!("{pad}  input:\n"));
            fmt_physical(&exec.input, indent + 2, out);
            out.push_str(&format!("{pad}  subquery:\n"));
            fmt_physical(&exec.subquery, indent + 2, out);
        }
        PhysicalPlan::Project(project) => {
            out.push_str(&format!("{pad}Project exprs={}\n", project.exprs.len()));
            for (expr, name) in &project.exprs {
                out.push_str(&format!("{pad}  {name} := {}\n", fmt_expr(expr)));
            }
            fmt_physical(&project.input, indent + 1, out);
        }
        PhysicalPlan::Window(window) => {
            out.push_str(&format!("{pad}WindowExec\n"));
            let window_groups = window_sort_reuse_groups(&window.exprs);
            out.push_str(&format!(
                "{pad}  window_exprs={} sort_reuse_groups={}\n",
                window.exprs.len(),
                window_groups.len()
            ));
            for (gidx, group) in window_groups.iter().enumerate() {
                out.push_str(&format!(
                    "{pad}  group[{gidx}] partition=[{}] order=[{}] windows=[{}]\n",
                    group.partition_display,
                    group.order_display,
                    group.window_names.join(", ")
                ));
            }
            out.push_str(&format!(
                "{pad}  distribution_strategy={}\n",
                window_distribution_strategy(&window.input)
            ));
            for w in &window.exprs {
                out.push_str(&format!(
                    "{pad}  {} frame={}\n",
                    w.output_name,
                    fmt_window_frame_or_default(w)
                ));
            }
            fmt_physical(&window.input, indent + 1, out);
        }
        PhysicalPlan::CoalesceBatches(exec) => {
            out.push_str(&format!(
                "{pad}CoalesceBatches target_batch_rows={}\n",
                exec.target_batch_rows
            ));
            fmt_physical(&exec.input, indent + 1, out);
        }
        PhysicalPlan::PartialHashAggregate(agg) => {
            out.push_str(&format!(
                "{pad}PartialHashAggregate group_by={} aggs={}\n",
                agg.group_exprs.len(),
                agg.aggr_exprs.len()
            ));
            fmt_physical(&agg.input, indent + 1, out);
        }
        PhysicalPlan::FinalHashAggregate(agg) => {
            out.push_str(&format!(
                "{pad}FinalHashAggregate group_by={} aggs={}\n",
                agg.group_exprs.len(),
                agg.aggr_exprs.len()
            ));
            fmt_physical(&agg.input, indent + 1, out);
        }
        PhysicalPlan::HashJoin(join) => {
            out.push_str(&format!(
                "{pad}HashJoin type={:?} strategy={}\n",
                join.join_type,
                fmt_join_hint(join.strategy_hint)
            ));
            out.push_str(&format!("{pad}  on={:?}\n", join.on));
            if !join.alternatives.is_empty() {
                out.push_str(&format!(
                    "{pad}  adaptive_alternatives={}\n",
                    join.alternatives.len()
                ));
                for (idx, alt) in join.alternatives.iter().enumerate() {
                    out.push_str(&format!(
                        "{pad}    alt[{idx}] strategy={} build_side={:?}\n",
                        fmt_join_hint(alt.strategy_hint),
                        alt.build_side
                    ));
                }
            }
            out.push_str(&format!("{pad}  left:\n"));
            fmt_physical(&join.left, indent + 2, out);
            out.push_str(&format!("{pad}  right:\n"));
            fmt_physical(&join.right, indent + 2, out);
        }
        PhysicalPlan::Exchange(exchange) => match exchange {
            ExchangeExec::ShuffleWrite(e) => {
                out.push_str(&format!(
                    "{pad}ShuffleWrite partitioning={}\n",
                    fmt_partitioning_spec(&e.partitioning)
                ));
                fmt_physical(&e.input, indent + 1, out);
            }
            ExchangeExec::ShuffleRead(e) => {
                out.push_str(&format!(
                    "{pad}ShuffleRead partitioning={}\n",
                    fmt_partitioning_spec(&e.partitioning)
                ));
                fmt_physical(&e.input, indent + 1, out);
            }
            ExchangeExec::Broadcast(e) => {
                out.push_str(&format!("{pad}Broadcast\n"));
                fmt_physical(&e.input, indent + 1, out);
            }
        },
        PhysicalPlan::Limit(limit) => {
            out.push_str(&format!("{pad}Limit n={}\n", limit.n));
            fmt_physical(&limit.input, indent + 1, out);
        }
        PhysicalPlan::TopKByScore(topk) => {
            out.push_str(&format!(
                "{pad}TopKByScore k={} score={}\n",
                topk.k,
                fmt_expr(&topk.score_expr)
            ));
            fmt_physical(&topk.input, indent + 1, out);
        }
        PhysicalPlan::UnionAll(union) => {
            out.push_str(&format!("{pad}UnionAll\n"));
            out.push_str(&format!("{pad}  left:\n"));
            fmt_physical(&union.left, indent + 2, out);
            out.push_str(&format!("{pad}  right:\n"));
            fmt_physical(&union.right, indent + 2, out);
        }
        PhysicalPlan::CteRef(cte) => {
            out.push_str(&format!("{pad}CteRef name={}\n", cte.name));
            fmt_physical(&cte.plan, indent + 1, out);
        }
        PhysicalPlan::VectorTopK(exec) => {
            out.push_str(&format!(
                "{pad}VectorTopK table={} k={} query_dim={}\n",
                exec.table,
                exec.k,
                exec.query_vector.len()
            ));
        }
        PhysicalPlan::VectorKnn(exec) => {
            out.push_str(&format!(
                "{pad}VectorKnn source={} k={} ef_search={:?} query_dim={} metric={} provider={} columns=[id,_score,payload]\n",
                exec.source,
                exec.k,
                exec.ef_search,
                exec.query_vector.len(),
                exec.metric,
                exec.provider
            ));
        }
        PhysicalPlan::Custom(custom) => {
            out.push_str(&format!("{pad}Custom op_name={}\n", custom.op_name));
            fmt_physical(&custom.input, indent + 1, out);
        }
    }
}

fn fmt_join_hint(h: JoinStrategyHint) -> &'static str {
    match h {
        JoinStrategyHint::Auto => "auto",
        JoinStrategyHint::BroadcastLeft => "broadcast_left",
        JoinStrategyHint::BroadcastRight => "broadcast_right",
        JoinStrategyHint::Shuffle => "shuffle",
        JoinStrategyHint::SortMerge => "sort_merge",
    }
}

fn join_rewrite_hint(plan: &LogicalPlan) -> Option<&'static str> {
    let LogicalPlan::Join {
        join_type,
        left,
        right,
        ..
    } = plan
    else {
        return None;
    };
    match join_type {
        crate::logical_plan::JoinType::Semi => {
            if plan_has_is_not_null_filter(right) {
                Some("decorrelated_in_subquery")
            } else {
                Some("decorrelated_exists_subquery")
            }
        }
        crate::logical_plan::JoinType::Anti => {
            if matches!(
                left.as_ref(),
                LogicalPlan::Join {
                    join_type: crate::logical_plan::JoinType::Anti,
                    ..
                }
            ) {
                Some("decorrelated_not_in_subquery")
            } else {
                Some("decorrelated_not_exists_subquery")
            }
        }
        _ => None,
    }
}

fn plan_has_is_not_null_filter(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Filter { predicate, input } => {
            matches!(predicate, Expr::IsNotNull(_)) || plan_has_is_not_null_filter(input)
        }
        LogicalPlan::Projection { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::TopKByScore { input, .. } => plan_has_is_not_null_filter(input),
        LogicalPlan::InSubqueryFilter {
            input, subquery, ..
        }
        | LogicalPlan::ExistsSubqueryFilter {
            input, subquery, ..
        } => plan_has_is_not_null_filter(input) || plan_has_is_not_null_filter(subquery),
        LogicalPlan::ScalarSubqueryFilter {
            input, subquery, ..
        } => plan_has_is_not_null_filter(input) || plan_has_is_not_null_filter(subquery),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::UnionAll { left, right } => {
            plan_has_is_not_null_filter(left) || plan_has_is_not_null_filter(right)
        }
        LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::InsertInto { input, .. }
        | LogicalPlan::CteRef { plan: input, .. } => plan_has_is_not_null_filter(input),
        _ => false,
    }
}

fn fmt_subquery_correlation(c: &SubqueryCorrelation) -> String {
    match c {
        SubqueryCorrelation::Unresolved => "unresolved".to_string(),
        SubqueryCorrelation::Uncorrelated => "uncorrelated".to_string(),
        SubqueryCorrelation::Correlated { outer_refs } => {
            format!("correlated({})", outer_refs.join(","))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{explain_logical, explain_physical};
    use crate::logical_plan::{
        Expr, JoinStrategyHint, JoinType, LogicalPlan, WindowExpr, WindowFunction, WindowOrderExpr,
    };
    use crate::physical_plan::{
        ExchangeExec, ParquetScanExec, PartitioningSpec, PhysicalPlan, ProjectExec,
        ShuffleReadExchange, ShuffleWriteExchange, WindowExec,
    };

    fn scan(name: &str) -> LogicalPlan {
        LogicalPlan::TableScan {
            table: name.to_string(),
            projection: None,
            filters: vec![],
        }
    }

    #[test]
    fn explain_marks_decorrelated_exists_join() {
        let plan = LogicalPlan::Join {
            left: Box::new(scan("t")),
            right: Box::new(scan("s")),
            on: vec![("t.a".to_string(), "s.b".to_string())],
            join_type: JoinType::Semi,
            strategy_hint: JoinStrategyHint::Auto,
        };
        let ex = explain_logical(&plan);
        assert!(ex.contains("rewrite=decorrelated_exists_subquery"), "{ex}");
    }

    #[test]
    fn explain_marks_decorrelated_in_join() {
        let right = LogicalPlan::Filter {
            predicate: Expr::IsNotNull(Box::new(Expr::Column("s.k".to_string()))),
            input: Box::new(scan("s")),
        };
        let plan = LogicalPlan::Join {
            left: Box::new(scan("t")),
            right: Box::new(right),
            on: vec![("t.k".to_string(), "s.k".to_string())],
            join_type: JoinType::Semi,
            strategy_hint: JoinStrategyHint::Auto,
        };
        let ex = explain_logical(&plan);
        assert!(ex.contains("rewrite=decorrelated_in_subquery"), "{ex}");
    }

    #[test]
    fn explain_window_prints_sort_reuse_groups() {
        let plan = LogicalPlan::Window {
            exprs: vec![
                WindowExpr {
                    func: WindowFunction::RowNumber,
                    partition_by: vec![Expr::Column("grp".to_string())],
                    order_by: vec![WindowOrderExpr {
                        expr: Expr::Column("score".to_string()),
                        asc: true,
                        nulls_first: false,
                    }],
                    frame: None,
                    output_name: "rn".to_string(),
                },
                WindowExpr {
                    func: WindowFunction::Rank,
                    partition_by: vec![Expr::Column("grp".to_string())],
                    order_by: vec![WindowOrderExpr {
                        expr: Expr::Column("score".to_string()),
                        asc: true,
                        nulls_first: false,
                    }],
                    frame: None,
                    output_name: "rnk".to_string(),
                },
                WindowExpr {
                    func: WindowFunction::DenseRank,
                    partition_by: vec![Expr::Column("grp".to_string())],
                    order_by: vec![WindowOrderExpr {
                        expr: Expr::Column("score".to_string()),
                        asc: false,
                        nulls_first: true,
                    }],
                    frame: None,
                    output_name: "dr".to_string(),
                },
            ],
            input: Box::new(scan("t")),
        };
        let ex = explain_logical(&plan);
        assert!(ex.contains("window_exprs=3 sort_reuse_groups=2"), "{ex}");
        assert!(ex.contains("windows=[rn, rnk]"), "{ex}");
        assert!(ex.contains("windows=[dr]"), "{ex}");
        assert!(
            ex.contains("FRAME RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
            "{ex}"
        );
    }

    #[test]
    fn explain_physical_window_prints_distribution_strategy_and_frames() {
        let plan = PhysicalPlan::Window(WindowExec {
            exprs: vec![WindowExpr {
                func: WindowFunction::RowNumber,
                partition_by: vec![Expr::Column("grp".to_string())],
                order_by: vec![WindowOrderExpr {
                    expr: Expr::Column("ord".to_string()),
                    asc: true,
                    nulls_first: false,
                }],
                frame: None,
                output_name: "rn".to_string(),
            }],
            input: Box::new(PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(
                ShuffleReadExchange {
                    partitioning: PartitioningSpec::HashKeys {
                        keys: vec!["grp".to_string()],
                        partitions: 8,
                    },
                    input: Box::new(PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(
                        ShuffleWriteExchange {
                            partitioning: PartitioningSpec::HashKeys {
                                keys: vec!["grp".to_string()],
                                partitions: 8,
                            },
                            input: Box::new(PhysicalPlan::Project(ProjectExec {
                                exprs: vec![(Expr::Column("grp".to_string()), "grp".to_string())],
                                input: Box::new(PhysicalPlan::ParquetScan(ParquetScanExec {
                                    table: "t".to_string(),
                                    schema: None,
                                    projection: None,
                                    filters: vec![],
                                })),
                            })),
                        },
                    ))),
                },
            ))),
        });
        let ex = explain_physical(&plan);
        assert!(ex.contains("WindowExec"), "{ex}");
        assert!(
            ex.contains("distribution_strategy=shuffle hash(keys=[grp], partitions=8)"),
            "{ex}"
        );
        assert!(
            ex.contains("frame=RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
            "{ex}"
        );
        assert!(ex.contains("sort_reuse_groups=1"), "{ex}");
    }
}

fn fmt_expr(e: &Expr) -> String {
    match e {
        Expr::Column(c) => c.clone(),
        Expr::ColumnRef { name, index } => format!("{name}#{index}"),
        Expr::Literal(v) => format!("{v:?}"),
        Expr::Cast { expr, to_type } => format!("cast({} as {to_type:?})", fmt_expr(expr)),
        Expr::Not(x) => format!("NOT ({})", fmt_expr(x)),
        Expr::IsNull(x) => format!("({}) IS NULL", fmt_expr(x)),
        Expr::IsNotNull(x) => format!("({}) IS NOT NULL", fmt_expr(x)),
        Expr::And(a, b) => format!("({}) AND ({})", fmt_expr(a), fmt_expr(b)),
        Expr::Or(a, b) => format!("({}) OR ({})", fmt_expr(a), fmt_expr(b)),
        Expr::CaseWhen {
            branches,
            else_expr,
        } => {
            let mut parts = vec!["CASE".to_string()];
            for (cond, value) in branches {
                parts.push(format!("WHEN {} THEN {}", fmt_expr(cond), fmt_expr(value)));
            }
            if let Some(e) = else_expr {
                parts.push(format!("ELSE {}", fmt_expr(e)));
            }
            parts.push("END".to_string());
            parts.join(" ")
        }
        Expr::BinaryOp { left, op, right } => {
            format!("({}) {:?} ({})", fmt_expr(left), op, fmt_expr(right))
        }
        #[cfg(feature = "vector")]
        Expr::CosineSimilarity { vector, query } => format!(
            "cosine_similarity({}, {})",
            fmt_expr(vector),
            fmt_expr(query)
        ),
        #[cfg(feature = "vector")]
        Expr::L2Distance { vector, query } => {
            format!("l2_distance({}, {})", fmt_expr(vector), fmt_expr(query))
        }
        #[cfg(feature = "vector")]
        Expr::DotProduct { vector, query } => {
            format!("dot_product({}, {})", fmt_expr(vector), fmt_expr(query))
        }
        Expr::ScalarUdf { name, args } => format!(
            "{}({})",
            name,
            args.iter().map(fmt_expr).collect::<Vec<_>>().join(", ")
        ),
    }
}

fn fmt_window_frame(f: &WindowFrameSpec) -> String {
    format!(
        "{} BETWEEN {} AND {} EXCLUDE {}",
        match f.units {
            WindowFrameUnits::Rows => "ROWS",
            WindowFrameUnits::Range => "RANGE",
            WindowFrameUnits::Groups => "GROUPS",
        },
        fmt_window_bound(&f.start_bound),
        fmt_window_bound(&f.end_bound),
        match f.exclusion {
            WindowFrameExclusion::NoOthers => "NO OTHERS",
            WindowFrameExclusion::CurrentRow => "CURRENT ROW",
            WindowFrameExclusion::Group => "GROUP",
            WindowFrameExclusion::Ties => "TIES",
        }
    )
}

fn fmt_window_frame_or_default(w: &WindowExpr) -> String {
    if let Some(frame) = &w.frame {
        return fmt_window_frame(frame);
    }
    if w.order_by.is_empty() {
        "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE NO OTHERS (implicit)"
            .to_string()
    } else {
        "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE NO OTHERS (implicit)".to_string()
    }
}

fn fmt_window_bound(b: &WindowFrameBound) -> String {
    match b {
        WindowFrameBound::UnboundedPreceding => "UNBOUNDED PRECEDING".to_string(),
        WindowFrameBound::Preceding(n) => format!("{n} PRECEDING"),
        WindowFrameBound::CurrentRow => "CURRENT ROW".to_string(),
        WindowFrameBound::Following(n) => format!("{n} FOLLOWING"),
        WindowFrameBound::UnboundedFollowing => "UNBOUNDED FOLLOWING".to_string(),
    }
}

fn fmt_partitioning_spec(spec: &PartitioningSpec) -> String {
    match spec {
        PartitioningSpec::Single => "single".to_string(),
        PartitioningSpec::HashKeys { keys, partitions } => {
            format!("hash(keys=[{}], partitions={partitions})", keys.join(", "))
        }
    }
}

fn window_distribution_strategy(input: &PhysicalPlan) -> String {
    match input {
        PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(read)) => match read.input.as_ref() {
            PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(write)) => {
                format!("shuffle {}", fmt_partitioning_spec(&write.partitioning))
            }
            _ => format!("shuffle {}", fmt_partitioning_spec(&read.partitioning)),
        },
        _ => "local(no_exchange)".to_string(),
    }
}

#[derive(Debug, Clone)]
struct WindowSortReuseGroup {
    partition_display: String,
    order_display: String,
    window_names: Vec<String>,
}

fn window_sort_reuse_groups(exprs: &[WindowExpr]) -> Vec<WindowSortReuseGroup> {
    let mut groups: Vec<WindowSortReuseGroup> = Vec::new();
    let mut by_key: HashMap<String, usize> = HashMap::new();
    for w in exprs {
        let partition_display = w
            .partition_by
            .iter()
            .map(fmt_expr)
            .collect::<Vec<_>>()
            .join(", ");
        let order_display = w
            .order_by
            .iter()
            .map(|o| {
                format!(
                    "{} {} NULLS {}",
                    fmt_expr(&o.expr),
                    if o.asc { "ASC" } else { "DESC" },
                    if o.nulls_first { "FIRST" } else { "LAST" }
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let key = format!("{partition_display}|{order_display}");
        let group_idx = if let Some(idx) = by_key.get(&key).copied() {
            idx
        } else {
            let idx = groups.len();
            groups.push(WindowSortReuseGroup {
                partition_display: partition_display.clone(),
                order_display: order_display.clone(),
                window_names: Vec::new(),
            });
            by_key.insert(key, idx);
            idx
        };
        groups[group_idx].window_names.push(w.output_name.clone());
    }
    groups
}
