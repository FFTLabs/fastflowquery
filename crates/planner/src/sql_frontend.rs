use std::collections::HashMap;

use ffq_common::{FfqError, Result};
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOp, Expr as SqlExpr, FunctionArg, FunctionArgExpr,
    FunctionArguments, GroupByExpr, Ident, JoinConstraint, JoinOperator, ObjectName, Query,
    SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value,
};

use crate::logical_plan::{AggExpr, BinaryOp, Expr, JoinStrategyHint, LiteralValue, LogicalPlan};

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
    let stmts = ffq_sql::parse_sql(sql)?;
    if stmts.len() != 1 {
        return Err(FfqError::Unsupported(
            "only single-statement SQL is supported in v1".to_string(),
        ));
    }
    statement_to_logical(&stmts[0], params)
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
    match stmt {
        Statement::Query(q) => query_to_logical(q, params),
        Statement::Insert(insert) => insert_to_logical(insert, params),
        _ => Err(FfqError::Unsupported(
            "only SELECT and INSERT INTO ... SELECT are supported in v1".to_string(),
        )),
    }
}

fn insert_to_logical(
    insert: &sqlparser::ast::Insert,
    params: &HashMap<String, LiteralValue>,
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
    let select_plan = query_to_logical(source, params)?;
    Ok(LogicalPlan::InsertInto {
        table,
        columns,
        input: Box::new(select_plan),
    })
}

fn query_to_logical(q: &Query, params: &HashMap<String, LiteralValue>) -> Result<LogicalPlan> {
    query_to_logical_with_ctes(q, params, &HashMap::new())
}

fn query_to_logical_with_ctes(
    q: &Query,
    params: &HashMap<String, LiteralValue>,
    parent_ctes: &HashMap<String, LogicalPlan>,
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
        for cte in &with.cte_tables {
            let name = cte.alias.name.value.clone();
            let cte_plan = query_to_logical_with_ctes(&cte.query, params, &cte_map)?;
            cte_map.insert(name, cte_plan);
        }
    }

    // FROM + JOINs
    let mut plan = from_to_plan(&select.from, params, &cte_map)?;

    // WHERE
    if let Some(selection) = &select.selection {
        plan = where_to_plan(plan, selection, params, &cte_map)?;
    }

    // GROUP BY
    let group_exprs = group_by_exprs(&select.group_by, params)?;
    let mut agg_exprs: Vec<(AggExpr, String)> = vec![];
    let mut proj_exprs: Vec<(Expr, String)> = vec![];

    // Parse SELECT list.
    // If we see aggregate functions or GROUP BY exists, we build Aggregate + Projection.
    let mut saw_agg = false;
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(e) => {
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

fn from_to_plan(
    from: &[TableWithJoins],
    params: &HashMap<String, LiteralValue>,
    ctes: &HashMap<String, LogicalPlan>,
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

fn table_factor_to_scan(tf: &TableFactor, ctes: &HashMap<String, LogicalPlan>) -> Result<LogicalPlan> {
    match tf {
        TableFactor::Table { name, .. } => {
            let t = object_name_to_string(name);
            if let Some(cte_plan) = ctes.get(&t) {
                return Ok(cte_plan.clone());
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
    ctes: &HashMap<String, LogicalPlan>,
) -> Result<LogicalPlan> {
    match selection {
        SqlExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => Ok(LogicalPlan::InSubqueryFilter {
            input: Box::new(input),
            expr: sql_expr_to_expr(expr, params)?,
            subquery: Box::new(query_to_logical_with_ctes(subquery, params, ctes)?),
            negated: *negated,
        }),
        SqlExpr::Exists { subquery, negated } => Ok(LogicalPlan::ExistsSubqueryFilter {
            input: Box::new(input),
            subquery: Box::new(query_to_logical_with_ctes(subquery, params, ctes)?),
            negated: *negated,
        }),
        SqlExpr::BinaryOp { left, op, right } => {
            match (&**left, &**right) {
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
                        subquery: Box::new(query_to_logical_with_ctes(sub, params, ctes)?),
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
                            subquery: Box::new(query_to_logical_with_ctes(sub, params, ctes)?),
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
            }
        }
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

    let make_name = |prefix: &str| -> String {
        // v1: simple generated name; later use schema-aware naming rules
        format!("{prefix}()")
    };

    let agg = match fname.as_str() {
        "COUNT" => {
            if let Some(a0) = arg0 {
                let ex = function_arg_to_expr(a0, params)?;
                AggExpr::Count(ex)
            } else {
                return Err(FfqError::Unsupported(
                    "COUNT() requires an argument in v1".to_string(),
                ));
            }
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
        SqlExpr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if operand.is_some() {
                return Err(FfqError::Unsupported(
                    "CASE <expr> WHEN ... form is not supported in v1; use CASE WHEN ...".to_string(),
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

    use super::sql_to_logical;
    use crate::logical_plan::LiteralValue;
    use crate::logical_plan::LogicalPlan;

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
                    crate::logical_plan::Expr::CaseWhen { branches, else_expr } => {
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
                    crate::logical_plan::Expr::CaseWhen { branches, else_expr } => {
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
        let plan = sql_to_logical("WITH c AS (SELECT a FROM t) SELECT a FROM c", &HashMap::new())
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
    fn parses_in_subquery_filter() {
        let plan = sql_to_logical("SELECT a FROM t WHERE a IN (SELECT b FROM s)", &HashMap::new())
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
        let plan =
            sql_to_logical("SELECT a FROM t WHERE EXISTS (SELECT b FROM s)", &HashMap::new())
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
        let plan =
            sql_to_logical("SELECT a FROM t WHERE a = (SELECT max(b) FROM s)", &HashMap::new())
                .expect("parse");
        match plan {
            LogicalPlan::Projection { input, .. } => match input.as_ref() {
                LogicalPlan::ScalarSubqueryFilter { .. } => {}
                other => panic!("expected ScalarSubqueryFilter, got {other:?}"),
            },
            other => panic!("expected Projection, got {other:?}"),
        }
    }
}
