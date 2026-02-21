use std::collections::HashMap;

use ffq_common::{FfqError, Result};
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOp, CteAsMaterialized, DuplicateTreatment, Expr as SqlExpr,
    FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident, JoinConstraint,
    JoinOperator, ObjectName, Query, SelectItem, SetExpr, SetOperator, SetQuantifier, Statement,
    TableFactor, TableWithJoins, Value,
};

use crate::logical_plan::{
    AggExpr, BinaryOp, Expr, JoinStrategyHint, LiteralValue, LogicalPlan, SubqueryCorrelation,
    WindowExpr, WindowFrameBound, WindowFrameExclusion, WindowFrameSpec, WindowFrameUnits,
    WindowFunction, WindowOrderExpr,
};

const E_RECURSIVE_CTE_OVERFLOW: &str = "E_RECURSIVE_CTE_OVERFLOW";

/// SQL frontend planning options.
#[derive(Debug, Clone, Copy)]
pub struct SqlFrontendOptions {
    /// Maximum recursive CTE expansion depth for `WITH RECURSIVE`.
    pub recursive_cte_max_depth: usize,
    /// CTE reuse strategy.
    pub cte_reuse_mode: CteReuseMode,
}

/// CTE reuse strategy used while lowering SQL to logical plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CteReuseMode {
    /// Always inline CTE plan at each reference.
    Inline,
    /// Materialize reused CTEs and share references.
    Materialize,
}

impl Default for SqlFrontendOptions {
    fn default() -> Self {
        Self {
            recursive_cte_max_depth: 32,
            cte_reuse_mode: CteReuseMode::Inline,
        }
    }
}

#[derive(Debug, Clone)]
struct CteBinding {
    plan: LogicalPlan,
    materialize: bool,
}

/// Convert a SQL string into a [`LogicalPlan`], binding named parameters (for
/// example `:k`, `:query`).
///
/// Contract:
/// - exactly one statement must be present;
/// - supported statements are delegated to [`statement_to_logical`].
///
/// Error taxonomy:
/// - `Unsupported`: SQL construct is outside v1 supported subset
/// - `Planning`: parse/parameter literal shape issues (for example bad LIMIT literal)
pub fn sql_to_logical(sql: &str, params: &HashMap<String, LiteralValue>) -> Result<LogicalPlan> {
    sql_to_logical_with_options(sql, params, SqlFrontendOptions::default())
}

/// Convert a SQL string into a [`LogicalPlan`] using explicit frontend options.
pub fn sql_to_logical_with_options(
    sql: &str,
    params: &HashMap<String, LiteralValue>,
    opts: SqlFrontendOptions,
) -> Result<LogicalPlan> {
    let stmts = ffq_sql::parse_sql(sql)?;
    if stmts.len() != 1 {
        return Err(FfqError::Unsupported(
            "only single-statement SQL is supported in v1".to_string(),
        ));
    }
    statement_to_logical_with_options(&stmts[0], params, opts)
}

/// Convert one parsed SQL statement into a [`LogicalPlan`].
///
/// v1 supports `SELECT` and `INSERT INTO ... SELECT ...` only.
///
/// Error taxonomy:
/// - `Unsupported`: statement kind not supported in v1
/// - `Planning`: invalid statement arguments/literals where applicable
pub fn statement_to_logical(
    stmt: &Statement,
    params: &HashMap<String, LiteralValue>,
) -> Result<LogicalPlan> {
    statement_to_logical_with_options(stmt, params, SqlFrontendOptions::default())
}

fn statement_to_logical_with_options(
    stmt: &Statement,
    params: &HashMap<String, LiteralValue>,
    opts: SqlFrontendOptions,
) -> Result<LogicalPlan> {
    match stmt {
        Statement::Query(q) => query_to_logical(q, params, opts),
        Statement::Insert(insert) => insert_to_logical(insert, params, opts),
        _ => Err(FfqError::Unsupported(
            "only SELECT and INSERT INTO ... SELECT are supported in v1".to_string(),
        )),
    }
}

fn insert_to_logical(
    insert: &sqlparser::ast::Insert,
    params: &HashMap<String, LiteralValue>,
    opts: SqlFrontendOptions,
) -> Result<LogicalPlan> {
    let table = object_name_to_string(&insert.table_name);
    let columns = insert
        .columns
        .iter()
        .map(|c| c.value.clone())
        .collect::<Vec<_>>();

    let source = insert.source.as_ref().ok_or_else(|| {
        FfqError::Unsupported("INSERT must have a SELECT source in v1".to_string())
    })?;
    let select_plan = query_to_logical(source, params, opts)?;
    Ok(LogicalPlan::InsertInto {
        table,
        columns,
        input: Box::new(select_plan),
    })
}

fn query_to_logical(
    q: &Query,
    params: &HashMap<String, LiteralValue>,
    opts: SqlFrontendOptions,
) -> Result<LogicalPlan> {
    query_to_logical_with_ctes(q, params, &HashMap::new(), opts)
}

fn query_to_logical_with_ctes(
    q: &Query,
    params: &HashMap<String, LiteralValue>,
    parent_ctes: &HashMap<String, CteBinding>,
    opts: SqlFrontendOptions,
) -> Result<LogicalPlan> {
    // We only support plain SELECT in v1.
    let select = match &*q.body {
        SetExpr::Select(s) => s.as_ref(),
        _ => {
            return Err(FfqError::Unsupported(
                "only simple SELECT is supported (no UNION/EXCEPT/INTERSECT)".to_string(),
            ));
        }
    };

    let mut cte_map = parent_ctes.clone();
    if let Some(with) = &q.with {
        let ordered = ordered_cte_indices(with, parent_ctes)?;
        let recursive_self = recursive_self_ctes(with);
        let cte_names = with
            .cte_tables
            .iter()
            .map(|c| c.alias.name.value.clone())
            .collect::<std::collections::HashSet<_>>();
        let cte_ref_counts = cte_reference_counts_in_query(q, &cte_names);
        for idx in ordered {
            let cte = &with.cte_tables[idx];
            let name = cte.alias.name.value.clone();
            let cte_plan = if recursive_self.contains(&name) {
                if !with.recursive {
                    return Err(FfqError::Planning(format!(
                        "CTE '{name}' references itself; use WITH RECURSIVE"
                    )));
                }
                build_recursive_cte_plan(cte, &name, params, &cte_map, opts)?
            } else {
                query_to_logical_with_ctes(&cte.query, params, &cte_map, opts)?
            };
            let materialize = match cte.materialized {
                Some(CteAsMaterialized::Materialized) => true,
                Some(CteAsMaterialized::NotMaterialized) => false,
                None => {
                    opts.cte_reuse_mode == CteReuseMode::Materialize
                        && cte_ref_counts.get(&name).copied().unwrap_or(0) > 1
                }
            };
            cte_map.insert(
                name,
                CteBinding {
                    plan: cte_plan,
                    materialize,
                },
            );
        }
    }

    // FROM + JOINs
    let mut plan = from_to_plan(&select.from, params, &cte_map)?;

    // WHERE
    if let Some(selection) = &select.selection {
        plan = where_to_plan(plan, selection, params, &cte_map, opts)?;
    }

    // GROUP BY
    let group_exprs = group_by_exprs(&select.group_by, params)?;
    let mut agg_exprs: Vec<(AggExpr, String)> = vec![];
    let mut proj_exprs: Vec<(Expr, String)> = vec![];
    let mut window_exprs: Vec<WindowExpr> = vec![];

    // Parse SELECT list.
    // If we see aggregate functions or GROUP BY exists, we build Aggregate + Projection.
    let mut saw_agg = false;
    let named_windows = parse_named_windows(select, params)?;
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(e) => {
                if let Some((wexpr, out_name)) =
                    try_parse_window_expr(e, params, &named_windows, None)?
                {
                    window_exprs.push(wexpr);
                    proj_exprs.push((Expr::Column(out_name.clone()), out_name));
                    continue;
                }
                if let Some((agg, name)) = try_parse_agg(e, params)? {
                    saw_agg = true;
                    agg_exprs.push((agg, name.clone()));
                    proj_exprs.push((Expr::Column(name.clone()), name));
                } else {
                    let expr = sql_expr_to_expr(e, params)?;
                    let name = expr_to_name_fallback(&expr);
                    proj_exprs.push((expr, name));
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let alias_name = alias.value.clone();
                if let Some((wexpr, out_name)) =
                    try_parse_window_expr(expr, params, &named_windows, Some(alias_name.clone()))?
                {
                    window_exprs.push(wexpr);
                    proj_exprs.push((Expr::Column(out_name.clone()), out_name));
                    continue;
                }
                if let Some((agg, _)) = try_parse_agg(expr, params)? {
                    saw_agg = true;
                    agg_exprs.push((agg, alias_name.clone()));
                    proj_exprs.push((Expr::Column(alias_name.clone()), alias_name));
                } else {
                    let expr = sql_expr_to_expr(expr, params)?;
                    proj_exprs.push((expr, alias_name));
                }
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                return Err(FfqError::Unsupported(
                    "SELECT * is not supported in v1 subset (use explicit columns)".to_string(),
                ));
            }
        }
    }

    let needs_agg = saw_agg || !group_exprs.is_empty();
    if needs_agg && !window_exprs.is_empty() {
        return Err(FfqError::Unsupported(
            "mixing GROUP BY aggregates and window functions is not supported in v1".to_string(),
        ));
    }
    let output_proj_exprs = proj_exprs.clone();
    let pre_projection_input = plan.clone();
    if needs_agg {
        plan = LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs: agg_exprs,
            input: Box::new(plan),
        };
        // After Aggregate, do a projection to shape output.
        plan = LogicalPlan::Projection {
            exprs: proj_exprs,
            input: Box::new(plan),
        };
    } else if !window_exprs.is_empty() {
        plan = LogicalPlan::Window {
            exprs: window_exprs,
            input: Box::new(plan),
        };
        plan = LogicalPlan::Projection {
            exprs: proj_exprs,
            input: Box::new(plan),
        };
    } else {
        // No aggregate: projection directly on input.
        plan = LogicalPlan::Projection {
            exprs: proj_exprs,
            input: Box::new(plan),
        };
    }

    // ORDER BY + LIMIT
    if let Some(order_by) = &q.order_by {
        if order_by.interpolate.is_some() {
            return Err(FfqError::Unsupported(
                "ORDER BY INTERPOLATE is not supported in v1".to_string(),
            ));
        }
        if order_by.exprs.len() != 1 {
            return Err(FfqError::Unsupported(
                "only a single ORDER BY expression is supported in v1".to_string(),
            ));
        }
        let item = &order_by.exprs[0];
        if item.asc != Some(false) {
            return Err(FfqError::Unsupported(
                "only ORDER BY ... DESC is supported in v1 top-k mode".to_string(),
            ));
        }
        let score_expr = sql_expr_to_expr(&item.expr, params)?;
        if !is_topk_score_expr(&score_expr) {
            return Err(FfqError::Unsupported(
                "global ORDER BY is not supported in v1; only ORDER BY cosine_similarity(...) DESC LIMIT k is supported".to_string(),
            ));
        }
        if needs_agg {
            return Err(FfqError::Unsupported(
                "ORDER BY cosine_similarity with aggregates is not supported in v1".to_string(),
            ));
        }
        let limit_expr = q.limit.as_ref().ok_or_else(|| {
            FfqError::Unsupported("ORDER BY cosine_similarity requires LIMIT k in v1".to_string())
        })?;
        let limit_val = sql_limit_to_usize(limit_expr, params)?;
        plan = LogicalPlan::Projection {
            exprs: output_proj_exprs,
            input: Box::new(LogicalPlan::TopKByScore {
                score_expr,
                k: limit_val,
                input: Box::new(pre_projection_input),
            }),
        };
    } else if let Some(limit_expr) = &q.limit {
        let limit_val = sql_limit_to_usize(limit_expr, params)?;
        plan = LogicalPlan::Limit {
            n: limit_val,
            input: Box::new(plan),
        };
    }

    Ok(plan)
}

fn ordered_cte_indices(
    with: &sqlparser::ast::With,
    parent_ctes: &HashMap<String, CteBinding>,
) -> Result<Vec<usize>> {
    let mut name_to_idx: HashMap<String, usize> = HashMap::new();
    for (idx, cte) in with.cte_tables.iter().enumerate() {
        let name = cte.alias.name.value.clone();
        if parent_ctes.contains_key(&name) {
            return Err(FfqError::Planning(format!(
                "CTE '{name}' shadows an outer CTE; shadowing is not allowed"
            )));
        }
        if name_to_idx.insert(name.clone(), idx).is_some() {
            return Err(FfqError::Planning(format!(
                "duplicate CTE name in WITH clause: '{name}'"
            )));
        }
    }

    let cte_names = name_to_idx
        .keys()
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    let mut deps_by_idx: Vec<std::collections::HashSet<usize>> =
        vec![std::collections::HashSet::new(); with.cte_tables.len()];
    let mut outgoing_by_idx: Vec<Vec<usize>> = vec![Vec::new(); with.cte_tables.len()];

    let self_recursive = recursive_self_ctes(with);
    for (idx, cte) in with.cte_tables.iter().enumerate() {
        let deps = referenced_local_ctes_in_query(&cte.query, &cte_names);
        for dep_name in deps {
            if dep_name == cte.alias.name.value && self_recursive.contains(&dep_name) {
                // Allow legal self-edge; this is handled by recursive CTE expansion.
                continue;
            }
            if let Some(dep_idx) = name_to_idx.get(&dep_name).copied() {
                deps_by_idx[idx].insert(dep_idx);
            }
        }
    }
    for (idx, deps) in deps_by_idx.iter().enumerate() {
        for dep in deps {
            outgoing_by_idx[*dep].push(idx);
        }
    }

    let mut indegree = deps_by_idx.iter().map(|d| d.len()).collect::<Vec<_>>();
    let mut ready = indegree
        .iter()
        .enumerate()
        .filter_map(|(idx, deg)| (*deg == 0).then_some(idx))
        .collect::<Vec<_>>();
    // Deterministic ordering: declaration order when multiple CTEs are ready.
    ready.sort_unstable();

    let mut out = Vec::with_capacity(with.cte_tables.len());
    while let Some(idx) = ready.first().copied() {
        ready.remove(0);
        out.push(idx);
        for succ in &outgoing_by_idx[idx] {
            indegree[*succ] -= 1;
            if indegree[*succ] == 0 {
                ready.push(*succ);
                ready.sort_unstable();
            }
        }
    }

    if out.len() != with.cte_tables.len() {
        let cycle_nodes = indegree
            .iter()
            .enumerate()
            .filter_map(|(idx, deg)| {
                (*deg > 0).then_some(with.cte_tables[idx].alias.name.value.clone())
            })
            .collect::<Vec<_>>();
        return Err(FfqError::Planning(format!(
            "CTE dependency cycle detected involving: {}",
            cycle_nodes.join(", ")
        )));
    }
    Ok(out)
}

fn recursive_self_ctes(with: &sqlparser::ast::With) -> std::collections::HashSet<String> {
    let cte_names = with
        .cte_tables
        .iter()
        .map(|c| c.alias.name.value.clone())
        .collect::<std::collections::HashSet<_>>();
    with.cte_tables
        .iter()
        .filter_map(|cte| {
            let name = cte.alias.name.value.clone();
            let refs = referenced_local_ctes_in_query(&cte.query, &cte_names);
            refs.contains(&name).then_some(name)
        })
        .collect()
}

fn build_recursive_cte_plan(
    cte: &sqlparser::ast::Cte,
    cte_name: &str,
    params: &HashMap<String, LiteralValue>,
    cte_map: &HashMap<String, CteBinding>,
    opts: SqlFrontendOptions,
) -> Result<LogicalPlan> {
    if opts.recursive_cte_max_depth == 0 {
        return Err(FfqError::Planning(format!(
            "{E_RECURSIVE_CTE_OVERFLOW}: recursive CTE '{cte_name}' cannot be planned with recursive_cte_max_depth=0"
        )));
    }
    let SetExpr::SetOperation {
        op,
        set_quantifier,
        left,
        right,
    } = cte.query.body.as_ref()
    else {
        return Err(FfqError::Unsupported(format!(
            "recursive CTE '{cte_name}' must use UNION ALL between seed and recursive term"
        )));
    };
    if *op != SetOperator::Union || *set_quantifier != SetQuantifier::All {
        return Err(FfqError::Unsupported(format!(
            "recursive CTE '{cte_name}' only supports UNION ALL in phase-1"
        )));
    }

    let left_refs_self = setexpr_references_cte(left, cte_name);
    let right_refs_self = setexpr_references_cte(right, cte_name);
    let (seed_body, rec_body) = match (left_refs_self, right_refs_self) {
        (false, true) => (left.as_ref().clone(), right.as_ref().clone()),
        (true, false) => (right.as_ref().clone(), left.as_ref().clone()),
        (false, false) => {
            return Err(FfqError::Planning(format!(
                "recursive CTE '{cte_name}' has no self-reference in recursive term"
            )));
        }
        (true, true) => {
            return Err(FfqError::Unsupported(format!(
                "recursive CTE '{cte_name}' has multiple self-references; phase-1 supports one recursive term reference"
            )));
        }
    };

    let mut seed_query = (*cte.query).clone();
    seed_query.body = Box::new(seed_body);
    let seed = query_to_logical_with_ctes(&seed_query, params, cte_map, opts)?;

    let mut acc = seed.clone();
    let mut delta = seed;
    for _ in 0..opts.recursive_cte_max_depth {
        let mut rec_query = (*cte.query).clone();
        rec_query.body = Box::new(rec_body.clone());
        let mut loop_ctes = cte_map.clone();
        loop_ctes.insert(
            cte_name.to_string(),
            CteBinding {
                plan: delta.clone(),
                materialize: false,
            },
        );
        let step = query_to_logical_with_ctes(&rec_query, params, &loop_ctes, opts)?;
        acc = LogicalPlan::UnionAll {
            left: Box::new(acc),
            right: Box::new(step.clone()),
        };
        delta = step;
    }
    Ok(acc)
}

fn setexpr_references_cte(expr: &SetExpr, cte_name: &str) -> bool {
    match expr {
        SetExpr::Select(sel) => select_references_cte(sel, cte_name),
        SetExpr::Query(q) => setexpr_references_cte(&q.body, cte_name),
        SetExpr::SetOperation { left, right, .. } => {
            setexpr_references_cte(left, cte_name) || setexpr_references_cte(right, cte_name)
        }
        _ => false,
    }
}

fn select_references_cte(select: &sqlparser::ast::Select, cte_name: &str) -> bool {
    select.from.iter().any(|twj| {
        table_factor_references_cte(&twj.relation, cte_name)
            || twj
                .joins
                .iter()
                .any(|j| table_factor_references_cte(&j.relation, cte_name))
    })
}

fn table_factor_references_cte(tf: &TableFactor, cte_name: &str) -> bool {
    match tf {
        TableFactor::Table { name, .. } => object_name_to_string(name) == cte_name,
        TableFactor::Derived { subquery, .. } => setexpr_references_cte(&subquery.body, cte_name),
        _ => false,
    }
}

fn cte_reference_counts_in_query(
    q: &Query,
    cte_names: &std::collections::HashSet<String>,
) -> HashMap<String, usize> {
    let mut out = HashMap::new();
    collect_cte_ref_counts_from_setexpr(&q.body, cte_names, &mut out);
    out
}

fn collect_cte_ref_counts_from_setexpr(
    body: &SetExpr,
    cte_names: &std::collections::HashSet<String>,
    out: &mut HashMap<String, usize>,
) {
    match body {
        SetExpr::Select(sel) => collect_cte_ref_counts_from_select(sel.as_ref(), cte_names, out),
        SetExpr::Query(q) => collect_cte_ref_counts_from_setexpr(&q.body, cte_names, out),
        SetExpr::SetOperation { left, right, .. } => {
            collect_cte_ref_counts_from_setexpr(left, cte_names, out);
            collect_cte_ref_counts_from_setexpr(right, cte_names, out);
        }
        _ => {}
    }
}

fn collect_cte_ref_counts_from_select(
    select: &sqlparser::ast::Select,
    cte_names: &std::collections::HashSet<String>,
    out: &mut HashMap<String, usize>,
) {
    for twj in &select.from {
        collect_cte_ref_counts_from_table_factor(&twj.relation, cte_names, out);
        for j in &twj.joins {
            collect_cte_ref_counts_from_table_factor(&j.relation, cte_names, out);
        }
    }
    if let Some(selection) = &select.selection {
        collect_cte_ref_counts_from_expr(selection, cte_names, out);
    }
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(e) => collect_cte_ref_counts_from_expr(e, cte_names, out),
            SelectItem::ExprWithAlias { expr, .. } => {
                collect_cte_ref_counts_from_expr(expr, cte_names, out)
            }
            _ => {}
        }
    }
}

fn collect_cte_ref_counts_from_table_factor(
    tf: &TableFactor,
    cte_names: &std::collections::HashSet<String>,
    out: &mut HashMap<String, usize>,
) {
    match tf {
        TableFactor::Table { name, .. } => {
            let t = object_name_to_string(name);
            if cte_names.contains(&t) {
                *out.entry(t).or_insert(0) += 1;
            }
        }
        TableFactor::Derived { subquery, .. } => {
            collect_cte_ref_counts_from_setexpr(&subquery.body, cte_names, out);
        }
        _ => {}
    }
}

fn collect_cte_ref_counts_from_expr(
    expr: &SqlExpr,
    cte_names: &std::collections::HashSet<String>,
    out: &mut HashMap<String, usize>,
) {
    match expr {
        SqlExpr::Subquery(q) => collect_cte_ref_counts_from_setexpr(&q.body, cte_names, out),
        SqlExpr::Exists { subquery, .. } => {
            collect_cte_ref_counts_from_setexpr(&subquery.body, cte_names, out)
        }
        SqlExpr::InSubquery { expr, subquery, .. } => {
            collect_cte_ref_counts_from_expr(expr, cte_names, out);
            collect_cte_ref_counts_from_setexpr(&subquery.body, cte_names, out);
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            collect_cte_ref_counts_from_expr(left, cte_names, out);
            collect_cte_ref_counts_from_expr(right, cte_names, out);
        }
        SqlExpr::UnaryOp { expr, .. } => collect_cte_ref_counts_from_expr(expr, cte_names, out),
        SqlExpr::Nested(e) => collect_cte_ref_counts_from_expr(e, cte_names, out),
        SqlExpr::IsNull(e) | SqlExpr::IsNotNull(e) => {
            collect_cte_ref_counts_from_expr(e, cte_names, out)
        }
        SqlExpr::Function(f) => {
            if let FunctionArguments::List(list) = &f.args {
                for arg in &list.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                        collect_cte_ref_counts_from_expr(e, cte_names, out);
                    }
                }
            }
        }
        _ => {}
    }
}

fn referenced_local_ctes_in_query(
    q: &Query,
    cte_names: &std::collections::HashSet<String>,
) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    collect_cte_refs_from_setexpr(&q.body, cte_names, &mut out);
    out
}

fn collect_cte_refs_from_setexpr(
    body: &Box<SetExpr>,
    cte_names: &std::collections::HashSet<String>,
    out: &mut std::collections::HashSet<String>,
) {
    match body.as_ref() {
        SetExpr::Select(sel) => {
            collect_cte_refs_from_select(sel.as_ref(), cte_names, out);
        }
        SetExpr::Query(q) => {
            collect_cte_refs_from_setexpr(&q.body, cte_names, out);
        }
        SetExpr::SetOperation { left, right, .. } => {
            collect_cte_refs_from_setexpr(left, cte_names, out);
            collect_cte_refs_from_setexpr(right, cte_names, out);
        }
        _ => {}
    }
}

fn collect_cte_refs_from_select(
    select: &sqlparser::ast::Select,
    cte_names: &std::collections::HashSet<String>,
    out: &mut std::collections::HashSet<String>,
) {
    for twj in &select.from {
        collect_cte_refs_from_table_factor(&twj.relation, cte_names, out);
        for j in &twj.joins {
            collect_cte_refs_from_table_factor(&j.relation, cte_names, out);
        }
    }
    if let Some(selection) = &select.selection {
        collect_cte_refs_from_expr(selection, cte_names, out);
    }
    for proj in &select.projection {
        match proj {
            SelectItem::UnnamedExpr(e) => collect_cte_refs_from_expr(e, cte_names, out),
            SelectItem::ExprWithAlias { expr, .. } => {
                collect_cte_refs_from_expr(expr, cte_names, out)
            }
            _ => {}
        }
    }
}

fn collect_cte_refs_from_table_factor(
    tf: &TableFactor,
    cte_names: &std::collections::HashSet<String>,
    out: &mut std::collections::HashSet<String>,
) {
    match tf {
        TableFactor::Table { name, .. } => {
            let t = object_name_to_string(name);
            if cte_names.contains(&t) {
                out.insert(t);
            }
        }
        TableFactor::Derived { subquery, .. } => {
            collect_cte_refs_from_setexpr(&subquery.body, cte_names, out);
        }
        _ => {}
    }
}

fn collect_cte_refs_from_expr(
    expr: &SqlExpr,
    cte_names: &std::collections::HashSet<String>,
    out: &mut std::collections::HashSet<String>,
) {
    match expr {
        SqlExpr::Subquery(q) => collect_cte_refs_from_setexpr(&q.body, cte_names, out),
        SqlExpr::Exists { subquery, .. } => {
            collect_cte_refs_from_setexpr(&subquery.body, cte_names, out)
        }
        SqlExpr::InSubquery { subquery, expr, .. } => {
            collect_cte_refs_from_expr(expr, cte_names, out);
            collect_cte_refs_from_setexpr(&subquery.body, cte_names, out);
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            collect_cte_refs_from_expr(left, cte_names, out);
            collect_cte_refs_from_expr(right, cte_names, out);
        }
        SqlExpr::UnaryOp { expr, .. } => collect_cte_refs_from_expr(expr, cte_names, out),
        SqlExpr::Nested(e) => collect_cte_refs_from_expr(e, cte_names, out),
        SqlExpr::IsNull(e) | SqlExpr::IsNotNull(e) => collect_cte_refs_from_expr(e, cte_names, out),
        SqlExpr::Function(f) => {
            if let FunctionArguments::List(list) = &f.args {
                for arg in &list.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                        collect_cte_refs_from_expr(e, cte_names, out);
                    }
                }
            }
        }
        _ => {}
    }
}

fn from_to_plan(
    from: &[TableWithJoins],
    params: &HashMap<String, LiteralValue>,
    ctes: &HashMap<String, CteBinding>,
) -> Result<LogicalPlan> {
    if from.len() != 1 {
        return Err(FfqError::Unsupported(
            "only one FROM source is supported in v1".to_string(),
        ));
    }
    let twj = &from[0];

    let mut left = table_factor_to_scan(&twj.relation, ctes)?;

    for j in &twj.joins {
        let right = table_factor_to_scan(&j.relation, ctes)?;
        let (constraint, join_type) = match &j.join_operator {
            JoinOperator::Inner(c) => (c, crate::logical_plan::JoinType::Inner),
            JoinOperator::LeftOuter(c) => (c, crate::logical_plan::JoinType::Left),
            JoinOperator::RightOuter(c) => (c, crate::logical_plan::JoinType::Right),
            JoinOperator::FullOuter(c) => (c, crate::logical_plan::JoinType::Full),
            _ => {
                return Err(FfqError::Unsupported(
                    "only INNER/LEFT/RIGHT/FULL OUTER JOIN are supported in v1".to_string(),
                ));
            }
        };
        let on_pairs = join_constraint_to_on_pairs(constraint)?;
        left = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            on: on_pairs,
            join_type,
            strategy_hint: JoinStrategyHint::Auto,
        };
    }

    // (Note: params are not used here yet; kept for future join filters, etc.)
    let _ = params;
    Ok(left)
}

fn table_factor_to_scan(
    tf: &TableFactor,
    ctes: &HashMap<String, CteBinding>,
) -> Result<LogicalPlan> {
    match tf {
        TableFactor::Table { name, .. } => {
            let t = object_name_to_string(name);
            if let Some(cte) = ctes.get(&t) {
                if cte.materialize {
                    return Ok(LogicalPlan::CteRef {
                        name: t,
                        plan: Box::new(cte.plan.clone()),
                    });
                }
                return Ok(cte.plan.clone());
            }
            Ok(LogicalPlan::TableScan {
                table: t,
                projection: None,
                filters: vec![],
            })
        }
        _ => Err(FfqError::Unsupported(
            "only simple table names in FROM are supported in v1".to_string(),
        )),
    }
}

fn where_to_plan(
    input: LogicalPlan,
    selection: &SqlExpr,
    params: &HashMap<String, LiteralValue>,
    ctes: &HashMap<String, CteBinding>,
    opts: SqlFrontendOptions,
) -> Result<LogicalPlan> {
    match selection {
        SqlExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => Ok(LogicalPlan::InSubqueryFilter {
            input: Box::new(input),
            expr: sql_expr_to_expr(expr, params)?,
            subquery: Box::new(query_to_logical_with_ctes(subquery, params, ctes, opts)?),
            negated: *negated,
            correlation: SubqueryCorrelation::Unresolved,
        }),
        SqlExpr::Exists { subquery, negated } => Ok(LogicalPlan::ExistsSubqueryFilter {
            input: Box::new(input),
            subquery: Box::new(query_to_logical_with_ctes(subquery, params, ctes, opts)?),
            negated: *negated,
            correlation: SubqueryCorrelation::Unresolved,
        }),
        SqlExpr::BinaryOp { left, op, right } => match (&**left, &**right) {
            (SqlExpr::Subquery(sub), rhs_expr) => {
                let mapped_op = sql_binop_to_binop(op)?;
                let reversed = reverse_comparison_op(mapped_op).ok_or_else(|| {
                        FfqError::Unsupported(format!(
                            "scalar subquery only supports comparison operators (=, !=, <, <=, >, >=), got {op}"
                        ))
                    })?;
                Ok(LogicalPlan::ScalarSubqueryFilter {
                    input: Box::new(input),
                    expr: sql_expr_to_expr(rhs_expr, params)?,
                    op: reversed,
                    subquery: Box::new(query_to_logical_with_ctes(sub, params, ctes, opts)?),
                    correlation: SubqueryCorrelation::Unresolved,
                })
            }
            (lhs_expr, SqlExpr::Subquery(sub)) => {
                let mapped_op = sql_binop_to_binop(op)?;
                match mapped_op {
                    BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq => Ok(LogicalPlan::ScalarSubqueryFilter {
                        input: Box::new(input),
                        expr: sql_expr_to_expr(lhs_expr, params)?,
                        op: mapped_op,
                        subquery: Box::new(query_to_logical_with_ctes(sub, params, ctes, opts)?),
                        correlation: SubqueryCorrelation::Unresolved,
                    }),
                    _ => Err(FfqError::Unsupported(format!(
                        "scalar subquery only supports comparison operators (=, !=, <, <=, >, >=), got {op}"
                    ))),
                }
            }
            _ => {
                let pred = sql_expr_to_expr(selection, params)?;
                Ok(LogicalPlan::Filter {
                    predicate: pred,
                    input: Box::new(input),
                })
            }
        },
        _ => {
            let pred = sql_expr_to_expr(selection, params)?;
            Ok(LogicalPlan::Filter {
                predicate: pred,
                input: Box::new(input),
            })
        }
    }
}

fn reverse_comparison_op(op: BinaryOp) -> Option<BinaryOp> {
    Some(match op {
        BinaryOp::Eq => BinaryOp::Eq,
        BinaryOp::NotEq => BinaryOp::NotEq,
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        _ => return None,
    })
}

fn join_constraint_to_on_pairs(constraint: &JoinConstraint) -> Result<Vec<(String, String)>> {
    match constraint {
        JoinConstraint::On(expr) => {
            let mut pairs = vec![];
            collect_equi_join_pairs(expr, &mut pairs)?;
            if pairs.is_empty() {
                return Err(FfqError::Unsupported(
                    "JOIN ... ON must be equi-join (a=b) in v1".to_string(),
                ));
            }
            Ok(pairs)
        }
        _ => Err(FfqError::Unsupported(
            "JOIN requires ON ... in v1".to_string(),
        )),
    }
}

fn collect_equi_join_pairs(expr: &SqlExpr, out: &mut Vec<(String, String)>) -> Result<()> {
    match expr {
        SqlExpr::BinaryOp { left, op, right } => {
            if *op == SqlBinaryOp::Eq {
                let l = sql_ident_expr_to_col(left)?;
                let r = sql_ident_expr_to_col(right)?;
                out.push((l, r));
                return Ok(());
            }
            // allow AND of equi conditions: a=b AND c=d
            if *op == SqlBinaryOp::And {
                collect_equi_join_pairs(left, out)?;
                collect_equi_join_pairs(right, out)?;
                return Ok(());
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn group_by_exprs(g: &GroupByExpr, params: &HashMap<String, LiteralValue>) -> Result<Vec<Expr>> {
    match g {
        GroupByExpr::Expressions(es, _mods) => {
            es.iter().map(|e| sql_expr_to_expr(e, params)).collect()
        }
        GroupByExpr::All(_mods) => Err(FfqError::Unsupported(
            "GROUP BY ALL is not supported in v1".to_string(),
        )),
    }
}

fn first_function_arg(func: &sqlparser::ast::Function) -> Option<&FunctionArg> {
    match &func.args {
        FunctionArguments::List(list) => list.args.get(0),
        _ => None,
    }
}

fn function_args(func: &sqlparser::ast::Function) -> Result<Vec<&FunctionArg>> {
    match &func.args {
        FunctionArguments::List(list) => Ok(list.args.iter().collect()),
        _ => Err(FfqError::Unsupported(
            "unsupported function argument form in v1".to_string(),
        )),
    }
}

fn try_parse_agg(
    e: &SqlExpr,
    params: &HashMap<String, LiteralValue>,
) -> Result<Option<(AggExpr, String)>> {
    let (func, alias) = match e {
        SqlExpr::Function(f) => (f, None),
        _ => return Ok(None),
    };

    let fname = object_name_to_string(&func.name).to_uppercase();
    let arg0 = first_function_arg(func);
    let is_distinct = match &func.args {
        FunctionArguments::List(list) => {
            matches!(list.duplicate_treatment, Some(DuplicateTreatment::Distinct))
        }
        _ => false,
    };

    let make_name = |prefix: &str| -> String {
        // v1: simple generated name; later use schema-aware naming rules
        format!("{prefix}()")
    };

    let agg = match fname.as_str() {
        "COUNT" => {
            if let Some(a0) = arg0 {
                let ex = function_arg_to_expr(a0, params)?;
                if is_distinct {
                    AggExpr::CountDistinct(ex)
                } else {
                    AggExpr::Count(ex)
                }
            } else {
                return Err(FfqError::Unsupported(
                    "COUNT() requires an argument in v1".to_string(),
                ));
            }
        }
        _ if is_distinct => {
            return Err(FfqError::Unsupported(format!(
                "{fname}(DISTINCT ...) is not supported in v1 (only COUNT(DISTINCT ...) is supported)"
            )));
        }
        "SUM" => AggExpr::Sum(function_arg_to_expr(required_arg(arg0, "SUM")?, params)?),
        "MIN" => AggExpr::Min(function_arg_to_expr(required_arg(arg0, "MIN")?, params)?),
        "MAX" => AggExpr::Max(function_arg_to_expr(required_arg(arg0, "MAX")?, params)?),
        "AVG" => AggExpr::Avg(function_arg_to_expr(required_arg(arg0, "AVG")?, params)?),
        _ => return Ok(None),
    };

    let name = alias.unwrap_or_else(|| make_name(&fname));
    Ok(Some((agg, name)))
}

fn try_parse_window_expr(
    e: &SqlExpr,
    params: &HashMap<String, LiteralValue>,
    named_windows: &HashMap<String, (Vec<Expr>, Vec<WindowOrderExpr>, Option<WindowFrameSpec>)>,
    explicit_alias: Option<String>,
) -> Result<Option<(WindowExpr, String)>> {
    let SqlExpr::Function(func) = e else {
        return Ok(None);
    };
    let Some(over) = &func.over else {
        return Ok(None);
    };
    let fname = object_name_to_string(&func.name).to_uppercase();
    let output_name = explicit_alias.unwrap_or_else(|| match fname.as_str() {
        "ROW_NUMBER" => "row_number()".to_string(),
        "RANK" => "rank()".to_string(),
        "DENSE_RANK" => "dense_rank()".to_string(),
        "PERCENT_RANK" => "percent_rank()".to_string(),
        "CUME_DIST" => "cume_dist()".to_string(),
        "NTILE" => "ntile()".to_string(),
        "COUNT" => "count_over()".to_string(),
        "SUM" => "sum_over()".to_string(),
        "AVG" => "avg_over()".to_string(),
        "MIN" => "min_over()".to_string(),
        "MAX" => "max_over()".to_string(),
        "LAG" => "lag()".to_string(),
        "LEAD" => "lead()".to_string(),
        "FIRST_VALUE" => "first_value()".to_string(),
        "LAST_VALUE" => "last_value()".to_string(),
        "NTH_VALUE" => "nth_value()".to_string(),
        _ => format!("window_{}", fname.to_lowercase()),
    });

    let (partition_by, order_by, frame) = match over {
        sqlparser::ast::WindowType::WindowSpec(spec) => {
            parse_window_spec(spec, params, named_windows)?
        }
        sqlparser::ast::WindowType::NamedWindow(name) => {
            named_windows.get(&name.value).cloned().ok_or_else(|| {
                FfqError::Planning(format!("unknown named window in OVER clause: '{}'", name))
            })?
        }
    };

    let args = function_args(func)?;
    let func_kind = match fname.as_str() {
        "ROW_NUMBER" => {
            if !args.is_empty() {
                return Err(FfqError::Unsupported(
                    "ROW_NUMBER() does not accept arguments".to_string(),
                ));
            }
            WindowFunction::RowNumber
        }
        "RANK" => {
            if !args.is_empty() {
                return Err(FfqError::Unsupported(
                    "RANK() does not accept arguments".to_string(),
                ));
            }
            WindowFunction::Rank
        }
        "DENSE_RANK" => {
            if !args.is_empty() {
                return Err(FfqError::Unsupported(
                    "DENSE_RANK() does not accept arguments".to_string(),
                ));
            }
            WindowFunction::DenseRank
        }
        "PERCENT_RANK" => {
            if !args.is_empty() {
                return Err(FfqError::Unsupported(
                    "PERCENT_RANK() does not accept arguments".to_string(),
                ));
            }
            WindowFunction::PercentRank
        }
        "CUME_DIST" => {
            if !args.is_empty() {
                return Err(FfqError::Unsupported(
                    "CUME_DIST() does not accept arguments".to_string(),
                ));
            }
            WindowFunction::CumeDist
        }
        "NTILE" => {
            if args.len() != 1 {
                return Err(FfqError::Unsupported(
                    "NTILE() requires one positive integer argument".to_string(),
                ));
            }
            let buckets = parse_positive_usize_arg(args[0], params, "NTILE")?;
            WindowFunction::Ntile(buckets)
        }
        "COUNT" => {
            if args.len() != 1 {
                return Err(FfqError::Unsupported(
                    "COUNT() OVER requires one argument in v1".to_string(),
                ));
            }
            let arg_expr = match args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                    Expr::Literal(LiteralValue::Int64(1))
                }
                other => function_arg_to_expr(other, params)?,
            };
            WindowFunction::Count(arg_expr)
        }
        "SUM" => WindowFunction::Sum(function_arg_to_expr(
            required_arg(args.first().copied(), "SUM")?,
            params,
        )?),
        "AVG" => WindowFunction::Avg(function_arg_to_expr(
            required_arg(args.first().copied(), "AVG")?,
            params,
        )?),
        "MIN" => WindowFunction::Min(function_arg_to_expr(
            required_arg(args.first().copied(), "MIN")?,
            params,
        )?),
        "MAX" => WindowFunction::Max(function_arg_to_expr(
            required_arg(args.first().copied(), "MAX")?,
            params,
        )?),
        "LAG" => {
            if args.is_empty() || args.len() > 3 {
                return Err(FfqError::Unsupported(
                    "LAG() supports 1 to 3 arguments in v1".to_string(),
                ));
            }
            let expr = function_arg_to_expr(args[0], params)?;
            let offset = if args.len() >= 2 {
                parse_positive_usize_arg(args[1], params, "LAG")?
            } else {
                1
            };
            let default = if args.len() >= 3 {
                Some(function_arg_to_expr(args[2], params)?)
            } else {
                None
            };
            WindowFunction::Lag {
                expr,
                offset,
                default,
            }
        }
        "LEAD" => {
            if args.is_empty() || args.len() > 3 {
                return Err(FfqError::Unsupported(
                    "LEAD() supports 1 to 3 arguments in v1".to_string(),
                ));
            }
            let expr = function_arg_to_expr(args[0], params)?;
            let offset = if args.len() >= 2 {
                parse_positive_usize_arg(args[1], params, "LEAD")?
            } else {
                1
            };
            let default = if args.len() >= 3 {
                Some(function_arg_to_expr(args[2], params)?)
            } else {
                None
            };
            WindowFunction::Lead {
                expr,
                offset,
                default,
            }
        }
        "FIRST_VALUE" => {
            if args.len() != 1 {
                return Err(FfqError::Unsupported(
                    "FIRST_VALUE() requires one argument in v1".to_string(),
                ));
            }
            WindowFunction::FirstValue(function_arg_to_expr(args[0], params)?)
        }
        "LAST_VALUE" => {
            if args.len() != 1 {
                return Err(FfqError::Unsupported(
                    "LAST_VALUE() requires one argument in v1".to_string(),
                ));
            }
            WindowFunction::LastValue(function_arg_to_expr(args[0], params)?)
        }
        "NTH_VALUE" => {
            if args.len() != 2 {
                return Err(FfqError::Unsupported(
                    "NTH_VALUE() requires two arguments in v1".to_string(),
                ));
            }
            let expr = function_arg_to_expr(args[0], params)?;
            let n = parse_positive_usize_arg(args[1], params, "NTH_VALUE")?;
            WindowFunction::NthValue { expr, n }
        }
        _ => {
            return Err(FfqError::Unsupported(format!(
                "unsupported window function in v1: {fname}"
            )));
        }
    };
    if order_by.is_empty() {
        return Err(FfqError::Unsupported(
            "window functions in v1 require ORDER BY in OVER(...)".to_string(),
        ));
    }
    Ok(Some((
        WindowExpr {
            func: func_kind,
            partition_by,
            order_by,
            frame,
            output_name: output_name.clone(),
        },
        output_name,
    )))
}

fn parse_named_windows(
    select: &sqlparser::ast::Select,
    params: &HashMap<String, LiteralValue>,
) -> Result<HashMap<String, (Vec<Expr>, Vec<WindowOrderExpr>, Option<WindowFrameSpec>)>> {
    let mut defs = HashMap::new();
    for def in &select.named_window {
        let name = def.0.value.clone();
        if defs.insert(name.clone(), def.1.clone()).is_some() {
            return Err(FfqError::Planning(format!(
                "duplicate named window definition: '{name}'"
            )));
        }
    }

    let mut resolved = HashMap::new();
    let mut resolving = std::collections::HashSet::new();
    let names = defs.keys().cloned().collect::<Vec<_>>();
    for name in names {
        resolve_named_window_spec(&name, &defs, params, &mut resolving, &mut resolved)?;
    }
    Ok(resolved)
}

fn resolve_named_window_spec(
    name: &str,
    defs: &HashMap<String, sqlparser::ast::NamedWindowExpr>,
    params: &HashMap<String, LiteralValue>,
    resolving: &mut std::collections::HashSet<String>,
    resolved: &mut HashMap<String, (Vec<Expr>, Vec<WindowOrderExpr>, Option<WindowFrameSpec>)>,
) -> Result<(Vec<Expr>, Vec<WindowOrderExpr>, Option<WindowFrameSpec>)> {
    if let Some(v) = resolved.get(name) {
        return Ok(v.clone());
    }
    if !resolving.insert(name.to_string()) {
        return Err(FfqError::Planning(format!(
            "named window reference cycle detected at '{name}'"
        )));
    }
    let named_expr = defs
        .get(name)
        .ok_or_else(|| FfqError::Planning(format!("unknown named window reference: '{name}'")))?;
    let resolved_spec = match named_expr {
        sqlparser::ast::NamedWindowExpr::NamedWindow(parent) => {
            resolve_named_window_spec(&parent.value, defs, params, resolving, resolved)?
        }
        sqlparser::ast::NamedWindowExpr::WindowSpec(spec) => {
            parse_window_spec_with_refs(spec, params, defs, resolving, resolved)?
        }
    };
    resolving.remove(name);
    resolved.insert(name.to_string(), resolved_spec.clone());
    Ok(resolved_spec)
}

fn parse_window_spec(
    spec: &sqlparser::ast::WindowSpec,
    params: &HashMap<String, LiteralValue>,
    named_windows: &HashMap<String, (Vec<Expr>, Vec<WindowOrderExpr>, Option<WindowFrameSpec>)>,
) -> Result<(Vec<Expr>, Vec<WindowOrderExpr>, Option<WindowFrameSpec>)> {
    let base = if let Some(base_name) = &spec.window_name {
        named_windows
            .get(&base_name.value)
            .cloned()
            .ok_or_else(|| {
                FfqError::Planning(format!(
                    "unknown named window referenced in OVER spec: '{}'",
                    base_name
                ))
            })?
    } else {
        (Vec::new(), Vec::new(), None)
    };
    let local_partition_by = spec
        .partition_by
        .iter()
        .map(|e| sql_expr_to_expr(e, params))
        .collect::<Result<Vec<_>>>()?;
    let local_order_by = parse_window_order_by(&spec.order_by, params)?;
    let local_frame = spec
        .window_frame
        .as_ref()
        .map(|f| parse_window_frame(f, params))
        .transpose()?;
    if !local_partition_by.is_empty() && !base.0.is_empty() {
        return Err(FfqError::Planning(
            "window spec cannot override PARTITION BY of referenced named window".to_string(),
        ));
    }
    if !local_order_by.is_empty() && !base.1.is_empty() {
        return Err(FfqError::Planning(
            "window spec cannot override ORDER BY of referenced named window".to_string(),
        ));
    }
    if local_frame.is_some() && base.2.is_some() {
        return Err(FfqError::Planning(
            "window spec cannot override frame of referenced named window".to_string(),
        ));
    }
    Ok((
        if local_partition_by.is_empty() {
            base.0
        } else {
            local_partition_by
        },
        if local_order_by.is_empty() {
            base.1
        } else {
            local_order_by
        },
        if local_frame.is_none() {
            base.2
        } else {
            local_frame
        },
    ))
}

fn parse_window_spec_with_refs(
    spec: &sqlparser::ast::WindowSpec,
    params: &HashMap<String, LiteralValue>,
    defs: &HashMap<String, sqlparser::ast::NamedWindowExpr>,
    resolving: &mut std::collections::HashSet<String>,
    resolved: &mut HashMap<String, (Vec<Expr>, Vec<WindowOrderExpr>, Option<WindowFrameSpec>)>,
) -> Result<(Vec<Expr>, Vec<WindowOrderExpr>, Option<WindowFrameSpec>)> {
    let base = if let Some(base_name) = &spec.window_name {
        resolve_named_window_spec(&base_name.value, defs, params, resolving, resolved)?
    } else {
        (Vec::new(), Vec::new(), None)
    };
    let local_partition_by = spec
        .partition_by
        .iter()
        .map(|e| sql_expr_to_expr(e, params))
        .collect::<Result<Vec<_>>>()?;
    let local_order_by = parse_window_order_by(&spec.order_by, params)?;
    let local_frame = spec
        .window_frame
        .as_ref()
        .map(|f| parse_window_frame(f, params))
        .transpose()?;
    if !local_partition_by.is_empty() && !base.0.is_empty() {
        return Err(FfqError::Planning(
            "named window cannot override PARTITION BY of referenced named window".to_string(),
        ));
    }
    if !local_order_by.is_empty() && !base.1.is_empty() {
        return Err(FfqError::Planning(
            "named window cannot override ORDER BY of referenced named window".to_string(),
        ));
    }
    if local_frame.is_some() && base.2.is_some() {
        return Err(FfqError::Planning(
            "named window cannot override frame of referenced named window".to_string(),
        ));
    }
    Ok((
        if local_partition_by.is_empty() {
            base.0
        } else {
            local_partition_by
        },
        if local_order_by.is_empty() {
            base.1
        } else {
            local_order_by
        },
        if local_frame.is_none() {
            base.2
        } else {
            local_frame
        },
    ))
}

fn parse_window_frame(
    frame: &sqlparser::ast::WindowFrame,
    params: &HashMap<String, LiteralValue>,
) -> Result<WindowFrameSpec> {
    let units = match frame.units {
        sqlparser::ast::WindowFrameUnits::Rows => WindowFrameUnits::Rows,
        sqlparser::ast::WindowFrameUnits::Range => WindowFrameUnits::Range,
        sqlparser::ast::WindowFrameUnits::Groups => WindowFrameUnits::Groups,
    };
    let start_bound = parse_window_frame_bound(&frame.start_bound, params)?;
    let end_bound = parse_window_frame_bound(
        frame
            .end_bound
            .as_ref()
            .unwrap_or(&sqlparser::ast::WindowFrameBound::CurrentRow),
        params,
    )?;
    let exclusion = match frame.exclusion {
        Some(sqlparser::ast::WindowFrameExclusion::NoOthers) | None => {
            WindowFrameExclusion::NoOthers
        }
        Some(sqlparser::ast::WindowFrameExclusion::CurrentRow) => WindowFrameExclusion::CurrentRow,
        Some(sqlparser::ast::WindowFrameExclusion::Group) => WindowFrameExclusion::Group,
        Some(sqlparser::ast::WindowFrameExclusion::Ties) => WindowFrameExclusion::Ties,
    };
    validate_window_frame_bounds(&start_bound, &end_bound)?;
    Ok(WindowFrameSpec {
        units,
        start_bound,
        end_bound,
        exclusion,
    })
}

fn parse_window_frame_bound(
    bound: &sqlparser::ast::WindowFrameBound,
    params: &HashMap<String, LiteralValue>,
) -> Result<WindowFrameBound> {
    match bound {
        sqlparser::ast::WindowFrameBound::CurrentRow => Ok(WindowFrameBound::CurrentRow),
        sqlparser::ast::WindowFrameBound::Preceding(None) => {
            Ok(WindowFrameBound::UnboundedPreceding)
        }
        sqlparser::ast::WindowFrameBound::Following(None) => {
            Ok(WindowFrameBound::UnboundedFollowing)
        }
        sqlparser::ast::WindowFrameBound::Preceding(Some(expr)) => Ok(WindowFrameBound::Preceding(
            parse_positive_usize_expr(expr, params, "window frame")?,
        )),
        sqlparser::ast::WindowFrameBound::Following(Some(expr)) => Ok(WindowFrameBound::Following(
            parse_positive_usize_expr(expr, params, "window frame")?,
        )),
    }
}

fn parse_positive_usize_expr(
    expr: &SqlExpr,
    params: &HashMap<String, LiteralValue>,
    ctx: &str,
) -> Result<usize> {
    let parsed = sql_expr_to_expr(expr, params)?;
    let Expr::Literal(LiteralValue::Int64(v)) = parsed else {
        return Err(FfqError::Planning(format!(
            "{ctx} bound requires positive integer literal in v1"
        )));
    };
    if v < 0 {
        return Err(FfqError::Planning(format!("{ctx} bound must be >= 0")));
    }
    Ok(v as usize)
}

fn validate_window_frame_bounds(start: &WindowFrameBound, end: &WindowFrameBound) -> Result<()> {
    if matches!(start, WindowFrameBound::UnboundedFollowing) {
        return Err(FfqError::Planning(
            "window frame start cannot be UNBOUNDED FOLLOWING".to_string(),
        ));
    }
    if matches!(end, WindowFrameBound::UnboundedPreceding) {
        return Err(FfqError::Planning(
            "window frame end cannot be UNBOUNDED PRECEDING".to_string(),
        ));
    }
    if frame_bound_order(start) > frame_bound_order(end) {
        return Err(FfqError::Planning(
            "window frame start bound must be <= end bound".to_string(),
        ));
    }
    Ok(())
}

fn frame_bound_order(bound: &WindowFrameBound) -> i32 {
    match bound {
        WindowFrameBound::UnboundedPreceding => -10_000,
        WindowFrameBound::Preceding(v) => -(*v as i32) - 1,
        WindowFrameBound::CurrentRow => 0,
        WindowFrameBound::Following(v) => *v as i32 + 1,
        WindowFrameBound::UnboundedFollowing => 10_000,
    }
}

fn parse_window_order_by(
    order_by: &[sqlparser::ast::OrderByExpr],
    params: &HashMap<String, LiteralValue>,
) -> Result<Vec<WindowOrderExpr>> {
    let mut out = Vec::with_capacity(order_by.len());
    for ob in order_by {
        let asc = ob.asc.unwrap_or(true);
        let nulls_first = ob.nulls_first.unwrap_or(!asc);
        out.push(WindowOrderExpr {
            expr: sql_expr_to_expr(&ob.expr, params)?,
            asc,
            nulls_first,
        });
    }
    Ok(out)
}

fn required_arg<'a>(a: Option<&'a FunctionArg>, name: &str) -> Result<&'a FunctionArg> {
    a.ok_or_else(|| FfqError::Unsupported(format!("{name}() requires one argument in v1")))
}

fn function_arg_to_expr(a: &FunctionArg, params: &HashMap<String, LiteralValue>) -> Result<Expr> {
    match a {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => sql_expr_to_expr(e, params),
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Err(FfqError::Unsupported(
            "COUNT(*) is not supported in v1 (use COUNT(1) or COUNT(col))".to_string(),
        )),
        _ => Err(FfqError::Unsupported(
            "unsupported function argument form in v1".to_string(),
        )),
    }
}

fn parse_positive_usize_arg(
    arg: &FunctionArg,
    params: &HashMap<String, LiteralValue>,
    fn_name: &str,
) -> Result<usize> {
    let expr = function_arg_to_expr(arg, params)?;
    let Expr::Literal(LiteralValue::Int64(v)) = expr else {
        return Err(FfqError::Planning(format!(
            "{fn_name}() requires a positive integer literal argument in v1"
        )));
    };
    if v <= 0 {
        return Err(FfqError::Planning(format!(
            "{fn_name}() argument must be > 0"
        )));
    }
    Ok(v as usize)
}

fn sql_expr_to_expr(e: &SqlExpr, params: &HashMap<String, LiteralValue>) -> Result<Expr> {
    match e {
        SqlExpr::Identifier(id) => Ok(Expr::Column(id.value.clone())),
        SqlExpr::CompoundIdentifier(parts) => Ok(Expr::Column(compound_ident_to_string(parts))),
        SqlExpr::Value(v) => sql_value_to_literal(v, params),
        SqlExpr::Function(func) => parse_scalar_function(func, params),
        SqlExpr::BinaryOp { left, op, right } => {
            // AND/OR are represented as BinaryOp too
            if *op == SqlBinaryOp::And {
                return Ok(Expr::And(
                    Box::new(sql_expr_to_expr(left, params)?),
                    Box::new(sql_expr_to_expr(right, params)?),
                ));
            }
            if *op == SqlBinaryOp::Or {
                return Ok(Expr::Or(
                    Box::new(sql_expr_to_expr(left, params)?),
                    Box::new(sql_expr_to_expr(right, params)?),
                ));
            }

            let bop = sql_binop_to_binop(op)?;
            Ok(Expr::BinaryOp {
                left: Box::new(sql_expr_to_expr(left, params)?),
                op: bop,
                right: Box::new(sql_expr_to_expr(right, params)?),
            })
        }
        SqlExpr::UnaryOp { op, expr } => {
            // Only support NOT for v1
            if op.to_string().to_uppercase() == "NOT" {
                Ok(Expr::Not(Box::new(sql_expr_to_expr(expr, params)?)))
            } else {
                Err(FfqError::Unsupported(format!(
                    "unsupported unary op in v1: {op}"
                )))
            }
        }
        SqlExpr::IsNull(expr) => Ok(Expr::IsNull(Box::new(sql_expr_to_expr(expr, params)?))),
        SqlExpr::IsNotNull(expr) => Ok(Expr::IsNotNull(Box::new(sql_expr_to_expr(expr, params)?))),
        SqlExpr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if operand.is_some() {
                return Err(FfqError::Unsupported(
                    "CASE <expr> WHEN ... form is not supported in v1; use CASE WHEN ..."
                        .to_string(),
                ));
            }
            if conditions.len() != results.len() {
                return Err(FfqError::Planning(
                    "CASE has mismatched WHEN/THEN branch count".to_string(),
                ));
            }
            let mut branches = Vec::with_capacity(conditions.len());
            for (cond, result) in conditions.iter().zip(results.iter()) {
                branches.push((
                    sql_expr_to_expr(cond, params)?,
                    sql_expr_to_expr(result, params)?,
                ));
            }
            let else_expr = else_result
                .as_ref()
                .map(|e| sql_expr_to_expr(e, params))
                .transpose()?
                .map(Box::new);
            Ok(Expr::CaseWhen {
                branches,
                else_expr,
            })
        }
        _ => Err(FfqError::Unsupported(format!(
            "unsupported SQL expression in v1: {e}"
        ))),
    }
}

fn parse_scalar_function(
    func: &sqlparser::ast::Function,
    params: &HashMap<String, LiteralValue>,
) -> Result<Expr> {
    let fname = object_name_to_string(&func.name).to_lowercase();
    let args = function_expr_args(func, params)?;

    #[cfg(feature = "vector")]
    {
        if fname == "cosine_similarity" {
            if args.len() != 2 {
                return Err(FfqError::Unsupported(
                    "cosine_similarity requires exactly 2 arguments in v1".to_string(),
                ));
            }
            return Ok(Expr::CosineSimilarity {
                vector: Box::new(args[0].clone()),
                query: Box::new(args[1].clone()),
            });
        }
    }

    Ok(Expr::ScalarUdf { name: fname, args })
}

fn function_expr_args(
    func: &sqlparser::ast::Function,
    params: &HashMap<String, LiteralValue>,
) -> Result<Vec<Expr>> {
    match &func.args {
        FunctionArguments::List(list) => list
            .args
            .iter()
            .map(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => sql_expr_to_expr(e, params),
                _ => Err(FfqError::Unsupported(
                    "unsupported function argument form in v1".to_string(),
                )),
            })
            .collect(),
        _ => Err(FfqError::Unsupported(
            "unsupported function argument form in v1".to_string(),
        )),
    }
}

fn sql_value_to_literal(v: &Value, params: &HashMap<String, LiteralValue>) -> Result<Expr> {
    match v {
        Value::Number(s, _) => {
            if s.contains('.') {
                let f: f64 = s
                    .parse()
                    .map_err(|_| FfqError::Planning(format!("bad number: {s}")))?;
                Ok(Expr::Literal(LiteralValue::Float64(f)))
            } else {
                let i: i64 = s
                    .parse()
                    .map_err(|_| FfqError::Planning(format!("bad number: {s}")))?;
                Ok(Expr::Literal(LiteralValue::Int64(i)))
            }
        }
        Value::SingleQuotedString(s) => Ok(Expr::Literal(LiteralValue::Utf8(s.clone()))),
        Value::Boolean(b) => Ok(Expr::Literal(LiteralValue::Boolean(*b))),
        Value::Null => Ok(Expr::Literal(LiteralValue::Null)),
        Value::Placeholder(ph) => {
            let key = normalize_placeholder_key(ph);
            match params.get(&key) {
                Some(v) => Ok(Expr::Literal(v.clone())),
                None => Err(FfqError::Planning(format!(
                    "missing SQL parameter :{key} (placeholder={ph})"
                ))),
            }
        }
        _ => Err(FfqError::Unsupported(format!(
            "unsupported SQL literal in v1: {v}"
        ))),
    }
}

fn sql_limit_to_usize(e: &SqlExpr, params: &HashMap<String, LiteralValue>) -> Result<usize> {
    let expr = sql_expr_to_expr(e, params)?;
    match expr {
        Expr::Literal(LiteralValue::Int64(i)) => {
            if i < 0 {
                Err(FfqError::Planning("LIMIT must be non-negative".to_string()))
            } else {
                Ok(i as usize)
            }
        }
        Expr::Literal(LiteralValue::Float64(_)) => {
            Err(FfqError::Planning("LIMIT must be an integer".to_string()))
        }
        _ => Err(FfqError::Planning(
            "LIMIT must be a literal integer or bound parameter".to_string(),
        )),
    }
}

fn sql_binop_to_binop(op: &SqlBinaryOp) -> Result<BinaryOp> {
    Ok(match op {
        SqlBinaryOp::Eq => BinaryOp::Eq,
        SqlBinaryOp::NotEq => BinaryOp::NotEq,
        SqlBinaryOp::Lt => BinaryOp::Lt,
        SqlBinaryOp::LtEq => BinaryOp::LtEq,
        SqlBinaryOp::Gt => BinaryOp::Gt,
        SqlBinaryOp::GtEq => BinaryOp::GtEq,
        SqlBinaryOp::Plus => BinaryOp::Plus,
        SqlBinaryOp::Minus => BinaryOp::Minus,
        SqlBinaryOp::Multiply => BinaryOp::Multiply,
        SqlBinaryOp::Divide => BinaryOp::Divide,
        _ => {
            return Err(FfqError::Unsupported(format!(
                "unsupported binary operator in v1: {op}"
            )));
        }
    })
}

fn object_name_to_string(n: &ObjectName) -> String {
    n.0.iter()
        .map(|i| i.value.clone())
        .collect::<Vec<_>>()
        .join(".")
}

fn compound_ident_to_string(parts: &[Ident]) -> String {
    parts
        .iter()
        .map(|i| i.value.clone())
        .collect::<Vec<_>>()
        .join(".")
}

fn sql_ident_expr_to_col(e: &SqlExpr) -> Result<String> {
    match e {
        SqlExpr::Identifier(id) => Ok(id.value.clone()),
        SqlExpr::CompoundIdentifier(parts) => Ok(compound_ident_to_string(parts)),
        _ => Err(FfqError::Unsupported(
            "JOIN keys must be column identifiers in v1".to_string(),
        )),
    }
}

fn normalize_placeholder_key(ph: &str) -> String {
    // Supports ":k", "k", "$1" (we won't implement positional binding yet, but normalize anyway)
    let s = ph.trim();
    let s = s.strip_prefix(':').unwrap_or(s);
    s.to_string()
}

fn expr_to_name_fallback(e: &Expr) -> String {
    match e {
        Expr::Column(c) => c.clone(),
        Expr::Literal(_) => "lit".to_string(),
        _ => "expr".to_string(),
    }
}

fn is_topk_score_expr(_e: &Expr) -> bool {
    #[cfg(feature = "vector")]
    if matches!(_e, Expr::CosineSimilarity { .. }) {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{CteReuseMode, SqlFrontendOptions, sql_to_logical, sql_to_logical_with_options};
    use crate::logical_plan::LiteralValue;
    use crate::logical_plan::{LogicalPlan, WindowFrameExclusion};

    #[test]
    fn parses_insert_into_select() {
        let plan = sql_to_logical("INSERT INTO t SELECT a FROM s", &HashMap::new()).expect("parse");
        match plan {
            LogicalPlan::InsertInto { table, columns, .. } => {
                assert_eq!(table, "t");
                assert!(columns.is_empty());
            }
            other => panic!("expected InsertInto, got {other:?}"),
        }
    }

    #[test]
    fn parses_count_distinct_aggregate() {
        let plan = sql_to_logical(
            "SELECT k, COUNT(DISTINCT v) AS cd FROM t GROUP BY k",
            &HashMap::new(),
        )
        .expect("parse");
        match plan {
            LogicalPlan::Projection { input, .. } => match input.as_ref() {
                LogicalPlan::Aggregate { aggr_exprs, .. } => {
                    assert_eq!(aggr_exprs.len(), 1);
                    assert!(matches!(
                        aggr_exprs[0].0,
                        crate::logical_plan::AggExpr::CountDistinct(_)
                    ));
                }
                other => panic!("expected Aggregate, got {other:?}"),
            },
            other => panic!("expected Projection, got {other:?}"),
        }
    }

    #[cfg(feature = "vector")]
    #[test]
    fn parses_cosine_similarity_expression() {
        let mut params = HashMap::new();
        params.insert(
            "q".to_string(),
            LiteralValue::VectorF32(vec![1.0, 2.0, 3.0]),
        );
        let plan = sql_to_logical(
            "SELECT cosine_similarity(emb, :q) AS score FROM docs",
            &params,
        )
        .expect("parse");
        match plan {
            LogicalPlan::Projection { exprs, .. } => {
                assert_eq!(exprs.len(), 1);
                match &exprs[0].0 {
                    crate::logical_plan::Expr::CosineSimilarity { .. } => {}
                    other => panic!("expected cosine expr, got {other:?}"),
                }
            }
            other => panic!("expected projection, got {other:?}"),
        }
    }

    #[cfg(feature = "vector")]
    #[test]
    fn rewrites_order_by_cosine_limit_into_topk() {
        let mut params = HashMap::new();
        params.insert(
            "q".to_string(),
            LiteralValue::VectorF32(vec![1.0, 2.0, 3.0]),
        );
        let plan = sql_to_logical(
            "SELECT id, title FROM docs ORDER BY cosine_similarity(emb, :q) DESC LIMIT 5",
            &params,
        )
        .expect("parse");
        match plan {
            LogicalPlan::Projection { input, .. } => match *input {
                LogicalPlan::TopKByScore { k, .. } => assert_eq!(k, 5),
                other => panic!("expected TopKByScore input, got {other:?}"),
            },
            other => panic!("expected Projection, got {other:?}"),
        }
    }

    #[test]
    fn parses_case_when_expression() {
        let plan = sql_to_logical(
            "SELECT CASE WHEN a > 1 THEN a ELSE 0 END AS c FROM t",
            &HashMap::new(),
        )
        .expect("parse");
        match plan {
            LogicalPlan::Projection { exprs, .. } => {
                assert_eq!(exprs.len(), 1);
                match &exprs[0].0 {
                    crate::logical_plan::Expr::CaseWhen {
                        branches,
                        else_expr,
                    } => {
                        assert_eq!(branches.len(), 1);
                        assert!(else_expr.is_some());
                    }
                    other => panic!("expected CASE expression, got {other:?}"),
                }
            }
            other => panic!("expected Projection, got {other:?}"),
        }
    }

    #[test]
    fn parses_case_when_in_where_expression_shape() {
        let plan = sql_to_logical(
            "SELECT k FROM t WHERE CASE WHEN k > 1 THEN true ELSE false END",
            &HashMap::new(),
        )
        .expect("parse");
        match plan {
            LogicalPlan::Projection { input, .. } => match input.as_ref() {
                LogicalPlan::Filter { predicate, .. } => match predicate {
                    crate::logical_plan::Expr::CaseWhen {
                        branches,
                        else_expr,
                    } => {
                        assert_eq!(branches.len(), 1);
                        match &branches[0].0 {
                            crate::logical_plan::Expr::BinaryOp { op, .. } => {
                                assert_eq!(*op, crate::logical_plan::BinaryOp::Gt);
                            }
                            other => panic!("expected WHEN condition binary gt, got {other:?}"),
                        }
                        match &branches[0].1 {
                            crate::logical_plan::Expr::Literal(LiteralValue::Boolean(true)) => {}
                            other => panic!("expected THEN true, got {other:?}"),
                        }
                        match else_expr.as_deref() {
                            Some(crate::logical_plan::Expr::Literal(LiteralValue::Boolean(
                                false,
                            ))) => {}
                            other => panic!("expected ELSE false, got {other:?}"),
                        }
                    }
                    other => panic!("expected CASE predicate, got {other:?}"),
                },
                other => panic!("expected Filter input, got {other:?}"),
            },
            other => panic!("expected Projection, got {other:?}"),
        }
    }

    #[test]
    fn parses_cte_query() {
        let plan = sql_to_logical(
            "WITH c AS (SELECT a FROM t) SELECT a FROM c",
            &HashMap::new(),
        )
        .expect("parse");
        match plan {
            LogicalPlan::Projection { input, .. } => match input.as_ref() {
                LogicalPlan::Projection {
                    input: cte_input, ..
                } => match cte_input.as_ref() {
                    LogicalPlan::TableScan { table, .. } => assert_eq!(table, "t"),
                    other => panic!("expected expanded CTE table scan, got {other:?}"),
                },
                other => panic!("expected cte projection, got {other:?}"),
            },
            other => panic!("expected Projection, got {other:?}"),
        }
    }

    #[test]
    fn parses_multi_cte_with_dependency_ordering() {
        let plan = sql_to_logical(
            "WITH b AS (SELECT a FROM c), c AS (SELECT a FROM t) SELECT a FROM b",
            &HashMap::new(),
        )
        .expect("parse");

        fn contains_tablescan(plan: &LogicalPlan, target: &str) -> bool {
            match plan {
                LogicalPlan::TableScan { table, .. } => table == target,
                LogicalPlan::Projection { input, .. }
                | LogicalPlan::Filter { input, .. }
                | LogicalPlan::Window { input, .. }
                | LogicalPlan::Limit { input, .. }
                | LogicalPlan::TopKByScore { input, .. }
                | LogicalPlan::InsertInto { input, .. } => contains_tablescan(input, target),
                LogicalPlan::InSubqueryFilter {
                    input, subquery, ..
                }
                | LogicalPlan::ExistsSubqueryFilter {
                    input, subquery, ..
                }
                | LogicalPlan::ScalarSubqueryFilter {
                    input, subquery, ..
                } => contains_tablescan(input, target) || contains_tablescan(subquery, target),
                LogicalPlan::Join { left, right, .. } => {
                    contains_tablescan(left, target) || contains_tablescan(right, target)
                }
                LogicalPlan::UnionAll { left, right } => {
                    contains_tablescan(left, target) || contains_tablescan(right, target)
                }
                LogicalPlan::Aggregate { input, .. } => contains_tablescan(input, target),
                LogicalPlan::CteRef { plan, .. } => contains_tablescan(plan, target),
                LogicalPlan::VectorTopK { .. } => false,
            }
        }

        assert!(
            contains_tablescan(&plan, "t"),
            "expected dependency-ordered expansion to include base table t: {plan:?}"
        );
    }

    fn count_cte_refs(plan: &LogicalPlan) -> usize {
        match plan {
            LogicalPlan::CteRef { plan, .. } => 1 + count_cte_refs(plan),
            LogicalPlan::Projection { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::TopKByScore { input, .. }
            | LogicalPlan::InsertInto { input, .. } => count_cte_refs(input),
            LogicalPlan::InSubqueryFilter {
                input, subquery, ..
            }
            | LogicalPlan::ExistsSubqueryFilter {
                input, subquery, ..
            }
            | LogicalPlan::ScalarSubqueryFilter {
                input, subquery, ..
            } => count_cte_refs(input) + count_cte_refs(subquery),
            LogicalPlan::Join { left, right, .. } | LogicalPlan::UnionAll { left, right } => {
                count_cte_refs(left) + count_cte_refs(right)
            }
            LogicalPlan::Aggregate { input, .. } => count_cte_refs(input),
            LogicalPlan::TableScan { .. } | LogicalPlan::VectorTopK { .. } => 0,
        }
    }

    #[test]
    fn cte_reuse_policy_materialize_emits_cte_refs_for_reused_cte() {
        let sql = "WITH c AS (SELECT a FROM t) SELECT l.a FROM c l JOIN c r ON l.a = r.a";
        let plan = sql_to_logical_with_options(
            sql,
            &HashMap::new(),
            SqlFrontendOptions {
                recursive_cte_max_depth: 32,
                cte_reuse_mode: CteReuseMode::Materialize,
            },
        )
        .expect("materialize cte parse");
        assert!(
            count_cte_refs(&plan) >= 2,
            "expected reused CTE references to emit CteRef nodes: {plan:?}"
        );
    }

    #[test]
    fn cte_reuse_policy_inline_does_not_emit_cte_refs() {
        let sql = "WITH c AS (SELECT a FROM t) SELECT l.a FROM c l JOIN c r ON l.a = r.a";
        let plan = sql_to_logical_with_options(
            sql,
            &HashMap::new(),
            SqlFrontendOptions {
                recursive_cte_max_depth: 32,
                cte_reuse_mode: CteReuseMode::Inline,
            },
        )
        .expect("inline cte parse");
        assert_eq!(count_cte_refs(&plan), 0, "expected inline plan: {plan:?}");
    }

    #[test]
    fn rejects_cte_dependency_cycle() {
        let err = sql_to_logical(
            "WITH a AS (SELECT x FROM b), b AS (SELECT y FROM a) SELECT x FROM a",
            &HashMap::new(),
        )
        .expect_err("cycle should fail");
        assert!(
            err.to_string()
                .contains("CTE dependency cycle detected involving"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_duplicate_cte_name() {
        let err = sql_to_logical(
            "WITH c AS (SELECT a FROM t), c AS (SELECT a FROM t2) SELECT a FROM c",
            &HashMap::new(),
        )
        .expect_err("duplicate CTE name should fail");
        assert!(
            err.to_string()
                .contains("duplicate CTE name in WITH clause"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_cte_shadowing_outer_scope() {
        let err = sql_to_logical(
            "WITH c AS (SELECT a FROM t), d AS (WITH c AS (SELECT a FROM t) SELECT a FROM c) SELECT a FROM d",
            &HashMap::new(),
        )
        .expect_err("shadowing should fail");
        assert!(
            err.to_string().contains("shadows an outer CTE"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn parses_recursive_cte_union_all() {
        let plan = sql_to_logical(
            "WITH RECURSIVE r AS (
                SELECT 1 AS node FROM t
                UNION ALL
                SELECT node + 1 AS node FROM r WHERE node < 3
            )
            SELECT node FROM r",
            &HashMap::new(),
        )
        .expect("recursive parse");

        fn has_union_all(plan: &LogicalPlan) -> bool {
            match plan {
                LogicalPlan::UnionAll { .. } => true,
                LogicalPlan::Projection { input, .. }
                | LogicalPlan::Filter { input, .. }
                | LogicalPlan::Window { input, .. }
                | LogicalPlan::Limit { input, .. }
                | LogicalPlan::TopKByScore { input, .. }
                | LogicalPlan::InsertInto { input, .. } => has_union_all(input),
                LogicalPlan::InSubqueryFilter {
                    input, subquery, ..
                }
                | LogicalPlan::ExistsSubqueryFilter {
                    input, subquery, ..
                }
                | LogicalPlan::ScalarSubqueryFilter {
                    input, subquery, ..
                } => has_union_all(input) || has_union_all(subquery),
                LogicalPlan::Join { left, right, .. } => {
                    has_union_all(left) || has_union_all(right)
                }
                LogicalPlan::Aggregate { input, .. } => has_union_all(input),
                LogicalPlan::CteRef { plan, .. } => has_union_all(plan),
                LogicalPlan::TableScan { .. } | LogicalPlan::VectorTopK { .. } => false,
            }
        }

        assert!(
            has_union_all(&plan),
            "expected recursive CTE to expand into UnionAll: {plan:?}"
        );
    }

    #[test]
    fn rejects_self_referencing_cte_without_recursive_keyword() {
        let err = sql_to_logical(
            "WITH r AS (
                SELECT 1 AS node FROM t
                UNION ALL
                SELECT node + 1 AS node FROM r WHERE node < 3
            )
            SELECT node FROM r",
            &HashMap::new(),
        )
        .expect_err("self-reference without WITH RECURSIVE should fail");

        assert!(
            err.to_string().contains("use WITH RECURSIVE"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_recursive_cte_when_depth_limit_is_zero() {
        let err = sql_to_logical_with_options(
            "WITH RECURSIVE r AS (
                SELECT 1 AS node FROM t
                UNION ALL
                SELECT node + 1 AS node FROM r WHERE node < 3
            )
            SELECT node FROM r",
            &HashMap::new(),
            SqlFrontendOptions {
                recursive_cte_max_depth: 0,
                cte_reuse_mode: CteReuseMode::Inline,
            },
        )
        .expect_err("depth=0 should reject recursive CTE");

        assert!(
            err.to_string().contains("recursive_cte_max_depth=0"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn parses_in_subquery_filter() {
        let plan = sql_to_logical(
            "SELECT a FROM t WHERE a IN (SELECT b FROM s)",
            &HashMap::new(),
        )
        .expect("parse");
        match plan {
            LogicalPlan::Projection { input, .. } => match input.as_ref() {
                LogicalPlan::InSubqueryFilter { .. } => {}
                other => panic!("expected InSubqueryFilter, got {other:?}"),
            },
            other => panic!("expected Projection, got {other:?}"),
        }
    }

    #[test]
    fn parses_exists_subquery_filter() {
        let plan = sql_to_logical(
            "SELECT a FROM t WHERE EXISTS (SELECT b FROM s)",
            &HashMap::new(),
        )
        .expect("parse");
        match plan {
            LogicalPlan::Projection { input, .. } => match input.as_ref() {
                LogicalPlan::ExistsSubqueryFilter { negated, .. } => assert!(!negated),
                other => panic!("expected ExistsSubqueryFilter, got {other:?}"),
            },
            other => panic!("expected Projection, got {other:?}"),
        }
    }

    #[test]
    fn parses_not_exists_subquery_filter() {
        let plan = sql_to_logical(
            "SELECT a FROM t WHERE NOT EXISTS (SELECT b FROM s)",
            &HashMap::new(),
        )
        .expect("parse");
        match plan {
            LogicalPlan::Projection { input, .. } => match input.as_ref() {
                LogicalPlan::ExistsSubqueryFilter { negated, .. } => assert!(*negated),
                other => panic!("expected ExistsSubqueryFilter, got {other:?}"),
            },
            other => panic!("expected Projection, got {other:?}"),
        }
    }

    #[test]
    fn parses_scalar_subquery_filter() {
        let plan = sql_to_logical(
            "SELECT a FROM t WHERE a = (SELECT max(b) FROM s)",
            &HashMap::new(),
        )
        .expect("parse");
        match plan {
            LogicalPlan::Projection { input, .. } => match input.as_ref() {
                LogicalPlan::ScalarSubqueryFilter { .. } => {}
                other => panic!("expected ScalarSubqueryFilter, got {other:?}"),
            },
            other => panic!("expected Projection, got {other:?}"),
        }
    }

    #[test]
    fn parses_window_order_desc_nulls_last() {
        let plan = sql_to_logical(
            "SELECT ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC NULLS LAST) AS rn FROM t",
            &HashMap::new(),
        )
        .expect("parse");
        match plan {
            LogicalPlan::Projection { input, .. } => match input.as_ref() {
                LogicalPlan::Window { exprs, .. } => {
                    assert_eq!(exprs.len(), 1);
                    assert_eq!(exprs[0].order_by.len(), 1);
                    assert!(!exprs[0].order_by[0].asc);
                    assert!(!exprs[0].order_by[0].nulls_first);
                }
                other => panic!("expected Window, got {other:?}"),
            },
            other => panic!("expected Projection, got {other:?}"),
        }
    }

    #[test]
    fn parses_named_window_reference_over_name() {
        let plan = sql_to_logical(
            "SELECT ROW_NUMBER() OVER w AS rn FROM t WINDOW w AS (PARTITION BY a ORDER BY b DESC NULLS FIRST)",
            &HashMap::new(),
        )
        .expect("parse");
        match plan {
            LogicalPlan::Projection { input, .. } => match input.as_ref() {
                LogicalPlan::Window { exprs, .. } => {
                    assert_eq!(exprs.len(), 1);
                    assert_eq!(exprs[0].partition_by.len(), 1);
                    assert_eq!(exprs[0].order_by.len(), 1);
                    assert!(!exprs[0].order_by[0].asc);
                    assert!(exprs[0].order_by[0].nulls_first);
                }
                other => panic!("expected Window, got {other:?}"),
            },
            other => panic!("expected Projection, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_named_window_reference() {
        let err = sql_to_logical("SELECT ROW_NUMBER() OVER w FROM t", &HashMap::new())
            .expect_err("unknown window should fail");
        assert!(
            err.to_string()
                .contains("unknown named window in OVER clause"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_window_spec_overriding_named_window_order_by() {
        let err = sql_to_logical(
            "SELECT ROW_NUMBER() OVER (w ORDER BY c) FROM t WINDOW w AS (ORDER BY b)",
            &HashMap::new(),
        )
        .expect_err("override should fail");
        assert!(
            err.to_string().contains("cannot override ORDER BY"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn parses_expanded_window_functions() {
        let plan = sql_to_logical(
            "SELECT \
               DENSE_RANK() OVER (PARTITION BY a ORDER BY b) AS dr, \
               PERCENT_RANK() OVER (PARTITION BY a ORDER BY b) AS pr, \
               CUME_DIST() OVER (PARTITION BY a ORDER BY b) AS cd, \
               NTILE(3) OVER (PARTITION BY a ORDER BY b) AS nt, \
               COUNT(b) OVER (PARTITION BY a ORDER BY b) AS ct, \
               AVG(b) OVER (PARTITION BY a ORDER BY b) AS av, \
               MIN(b) OVER (PARTITION BY a ORDER BY b) AS mn, \
               MAX(b) OVER (PARTITION BY a ORDER BY b) AS mx, \
               LAG(b, 2, 0) OVER (PARTITION BY a ORDER BY b) AS lg, \
               LEAD(b, 1, 0) OVER (PARTITION BY a ORDER BY b) AS ld, \
               FIRST_VALUE(b) OVER (PARTITION BY a ORDER BY b) AS fv, \
               LAST_VALUE(b) OVER (PARTITION BY a ORDER BY b) AS lv, \
               NTH_VALUE(b, 2) OVER (PARTITION BY a ORDER BY b) AS nv \
             FROM t",
            &HashMap::new(),
        )
        .expect("parse");
        match plan {
            LogicalPlan::Projection { input, .. } => match input.as_ref() {
                LogicalPlan::Window { exprs, .. } => assert_eq!(exprs.len(), 13),
                other => panic!("expected Window, got {other:?}"),
            },
            other => panic!("expected Projection, got {other:?}"),
        }
    }

    #[test]
    fn rejects_invalid_window_frame_bounds() {
        let err = sql_to_logical(
            "SELECT SUM(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) FROM t",
            &HashMap::new(),
        )
        .expect_err("invalid frame should fail");
        assert!(
            err.to_string().contains("UNBOUNDED FOLLOWING"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn parses_rows_range_groups_frames() {
        let plan = sql_to_logical(
            "SELECT \
                SUM(a) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS r1, \
                SUM(a) OVER (ORDER BY a RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS r2, \
                SUM(a) OVER (ORDER BY a GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS r3 \
             FROM t",
            &HashMap::new(),
        )
        .expect("parse");
        match plan {
            LogicalPlan::Projection { input, .. } => match input.as_ref() {
                LogicalPlan::Window { exprs, .. } => {
                    assert_eq!(exprs.len(), 3);
                    assert!(exprs.iter().all(|w| w.frame.is_some()));
                }
                other => panic!("expected Window, got {other:?}"),
            },
            other => panic!("expected Projection, got {other:?}"),
        }
    }

    #[test]
    fn parses_window_frame_exclusions() {
        let plan = sql_to_logical(
            "SELECT \
                SUM(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) AS c, \
                SUM(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS g, \
                SUM(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) AS t, \
                SUM(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE NO OTHERS) AS n \
             FROM t",
            &HashMap::new(),
        )
        .expect("parse");
        match plan {
            LogicalPlan::Projection { input, .. } => match input.as_ref() {
                LogicalPlan::Window { exprs, .. } => {
                    assert_eq!(exprs.len(), 4);
                    assert_eq!(
                        exprs[0].frame.as_ref().expect("frame").exclusion,
                        WindowFrameExclusion::CurrentRow
                    );
                    assert_eq!(
                        exprs[1].frame.as_ref().expect("frame").exclusion,
                        WindowFrameExclusion::Group
                    );
                    assert_eq!(
                        exprs[2].frame.as_ref().expect("frame").exclusion,
                        WindowFrameExclusion::Ties
                    );
                    assert_eq!(
                        exprs[3].frame.as_ref().expect("frame").exclusion,
                        WindowFrameExclusion::NoOthers
                    );
                }
                other => panic!("expected Window, got {other:?}"),
            },
            other => panic!("expected Projection, got {other:?}"),
        }
    }
}
