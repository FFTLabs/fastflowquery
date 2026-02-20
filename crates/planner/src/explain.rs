use crate::logical_plan::{
    Expr, JoinStrategyHint, LogicalPlan, SubqueryCorrelation, WindowFrameBound, WindowFrameSpec,
    WindowFrameUnits, WindowFunction,
};

/// Render logical plan as human-readable multiline text.
pub fn explain_logical(plan: &LogicalPlan) -> String {
    let mut s = String::new();
    fmt_plan(plan, 0, &mut s);
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
                        Some(d) => format!(
                            "LAG({}, {}, {})",
                            fmt_expr(expr),
                            offset,
                            fmt_expr(d)
                        ),
                        None => format!("LAG({}, {})", fmt_expr(expr), offset),
                    },
                    WindowFunction::Lead {
                        expr,
                        offset,
                        default,
                    } => match default {
                        Some(d) => format!(
                            "LEAD({}, {}, {})",
                            fmt_expr(expr),
                            offset,
                            fmt_expr(d)
                        ),
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
                    "{pad}  {} := {} OVER (PARTITION BY [{}] ORDER BY [{}]{} )\n",
                    w.output_name,
                    func,
                    part,
                    ord,
                    w.frame
                        .as_ref()
                        .map(|f| format!(" FRAME {}", fmt_window_frame(f)))
                        .unwrap_or_default()
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

fn fmt_join_hint(h: JoinStrategyHint) -> &'static str {
    match h {
        JoinStrategyHint::Auto => "auto",
        JoinStrategyHint::BroadcastLeft => "broadcast_left",
        JoinStrategyHint::BroadcastRight => "broadcast_right",
        JoinStrategyHint::Shuffle => "shuffle",
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
            if matches!(left.as_ref(), LogicalPlan::Join { join_type: crate::logical_plan::JoinType::Anti, .. }) {
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
        LogicalPlan::InSubqueryFilter { input, subquery, .. }
        | LogicalPlan::ExistsSubqueryFilter { input, subquery, .. } => {
            plan_has_is_not_null_filter(input) || plan_has_is_not_null_filter(subquery)
        }
        LogicalPlan::ScalarSubqueryFilter { input, subquery, .. } => {
            plan_has_is_not_null_filter(input) || plan_has_is_not_null_filter(subquery)
        }
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
    use super::explain_logical;
    use crate::logical_plan::{Expr, JoinStrategyHint, JoinType, LogicalPlan};

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
        Expr::CaseWhen { branches, else_expr } => {
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
        "{} BETWEEN {} AND {}",
        match f.units {
            WindowFrameUnits::Rows => "ROWS",
            WindowFrameUnits::Range => "RANGE",
            WindowFrameUnits::Groups => "GROUPS",
        },
        fmt_window_bound(&f.start_bound),
        fmt_window_bound(&f.end_bound)
    )
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
