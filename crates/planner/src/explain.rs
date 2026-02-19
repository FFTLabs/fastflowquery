use crate::logical_plan::{Expr, JoinStrategyHint, LogicalPlan};

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
        LogicalPlan::Projection { exprs, input } => {
            out.push_str(&format!("{pad}Projection\n"));
            for (e, name) in exprs {
                out.push_str(&format!("{pad}  {name} := {}\n", fmt_expr(e)));
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
            out.push_str(&format!(
                "{pad}Join type={join_type:?} strategy={}\n",
                fmt_join_hint(*strategy_hint)
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

fn fmt_expr(e: &Expr) -> String {
    match e {
        Expr::Column(c) => c.clone(),
        Expr::ColumnRef { name, index } => format!("{name}#{index}"),
        Expr::Literal(v) => format!("{v:?}"),
        Expr::Cast { expr, to_type } => format!("cast({} as {to_type:?})", fmt_expr(expr)),
        Expr::Not(x) => format!("NOT ({})", fmt_expr(x)),
        Expr::And(a, b) => format!("({}) AND ({})", fmt_expr(a), fmt_expr(b)),
        Expr::Or(a, b) => format!("({}) OR ({})", fmt_expr(a), fmt_expr(b)),
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
