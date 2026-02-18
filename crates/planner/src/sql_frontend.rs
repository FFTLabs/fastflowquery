use std::collections::HashMap;

use ffq_common::{FfqError, Result};
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOp, Expr as SqlExpr, FunctionArg, FunctionArgExpr,
    FunctionArguments, GroupByExpr, Ident, JoinConstraint, JoinOperator, ObjectName, Query,
    SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value,
};

use crate::logical_plan::{AggExpr, BinaryOp, Expr, JoinStrategyHint, LiteralValue, LogicalPlan};

/// Convert a SQL string into a LogicalPlan, binding named parameters (like :k, :query).
pub fn sql_to_logical(sql: &str, params: &HashMap<String, LiteralValue>) -> Result<LogicalPlan> {
    let stmts = ffq_sql::parse_sql(sql)?;
    if stmts.len() != 1 {
        return Err(FfqError::Unsupported(
            "only single-statement SQL is supported in v1".to_string(),
        ));
    }
    statement_to_logical(&stmts[0], params)
}

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
    // We only support plain SELECT in v1.
    let select = match &*q.body {
        SetExpr::Select(s) => s.as_ref(),
        _ => {
            return Err(FfqError::Unsupported(
                "only simple SELECT is supported (no UNION/EXCEPT/INTERSECT)".to_string(),
            ))
        }
    };

    // FROM + JOINs
    let mut plan = from_to_plan(&select.from, params)?;

    // WHERE
    if let Some(selection) = &select.selection {
        let pred = sql_expr_to_expr(selection, params)?;
        plan = LogicalPlan::Filter {
            predicate: pred,
            input: Box::new(plan),
        };
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
                ))
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
) -> Result<LogicalPlan> {
    if from.len() != 1 {
        return Err(FfqError::Unsupported(
            "only one FROM source is supported in v1".to_string(),
        ));
    }
    let twj = &from[0];

    let mut left = table_factor_to_scan(&twj.relation)?;

    for j in &twj.joins {
        let right = table_factor_to_scan(&j.relation)?;
        match &j.join_operator {
            JoinOperator::Inner(constraint) => {
                let on_pairs = join_constraint_to_on_pairs(constraint)?;
                left = LogicalPlan::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    on: on_pairs,
                    join_type: crate::logical_plan::JoinType::Inner,
                    strategy_hint: JoinStrategyHint::Auto,
                };
            }
            _ => {
                return Err(FfqError::Unsupported(
                    "only INNER JOIN is supported in v1".to_string(),
                ))
            }
        }
    }

    // (Note: params are not used here yet; kept for future join filters, etc.)
    let _ = params;
    Ok(left)
}

fn table_factor_to_scan(tf: &TableFactor) -> Result<LogicalPlan> {
    match tf {
        TableFactor::Table { name, .. } => {
            let t = object_name_to_string(name);
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
    #[cfg(not(feature = "vector"))]
    let _ = params;

    #[cfg(feature = "vector")]
    {
        if fname == "cosine_similarity" {
            let args = function_expr_args(func)?;
            if args.len() != 2 {
                return Err(FfqError::Unsupported(
                    "cosine_similarity requires exactly 2 arguments in v1".to_string(),
                ));
            }
            return Ok(Expr::CosineSimilarity {
                vector: Box::new(sql_expr_to_expr(args[0], params)?),
                query: Box::new(sql_expr_to_expr(args[1], params)?),
            });
        }
    }

    Err(FfqError::Unsupported(format!(
        "unsupported scalar function in v1: {fname}"
    )))
}

#[cfg(feature = "vector")]
fn function_expr_args<'a>(func: &'a sqlparser::ast::Function) -> Result<Vec<&'a SqlExpr>> {
    match &func.args {
        FunctionArguments::List(list) => list
            .args
            .iter()
            .map(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok(e),
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
            )))
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
    #[cfg(feature = "vector")]
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
}
