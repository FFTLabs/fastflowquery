use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use ffq_common::{FfqError, Result};

use crate::logical_plan::{AggExpr, BinaryOp, Expr, LiteralValue, LogicalPlan};

/// The analyzer needs schemas to resolve columns.
/// The client (Engine) will provide this from its Catalog.
pub trait SchemaProvider {
    /// Return schema for a table by name.
    fn table_schema(&self, table: &str) -> Result<SchemaRef>;
}

/// Logical-plan semantic analyzer.
pub struct Analyzer {
    udf_type_resolvers: RwLock<HashMap<String, ScalarUdfTypeResolver>>,
}

/// Type resolver callback for scalar UDFs.
pub type ScalarUdfTypeResolver =
    Arc<dyn Fn(&[DataType]) -> Result<DataType> + Send + Sync + 'static>;

impl std::fmt::Debug for Analyzer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self
            .udf_type_resolvers
            .read()
            .map(|m| m.len())
            .unwrap_or_default();
        f.debug_struct("Analyzer")
            .field("udf_type_resolvers", &count)
            .finish()
    }
}

impl Default for Analyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl Analyzer {
    /// Create a new analyzer.
    pub fn new() -> Self {
        Self {
            udf_type_resolvers: RwLock::new(HashMap::new()),
        }
    }

    /// Register or replace a scalar UDF type resolver.
    ///
    /// Returns `true` when an existing resolver with the same name was replaced.
    pub fn register_scalar_udf_type(
        &self,
        name: impl Into<String>,
        resolver: ScalarUdfTypeResolver,
    ) -> bool {
        self.udf_type_resolvers
            .write()
            .expect("udf resolver lock poisoned")
            .insert(name.into().to_ascii_lowercase(), resolver)
            .is_some()
    }

    /// Deregister a scalar UDF type resolver by name.
    ///
    /// Returns `true` when an existing resolver was removed.
    pub fn deregister_scalar_udf_type(&self, name: &str) -> bool {
        self.udf_type_resolvers
            .write()
            .expect("udf resolver lock poisoned")
            .remove(&name.to_ascii_lowercase())
            .is_some()
    }

    /// Analyze a logical plan and return a semantically validated plan.
    ///
    /// Guarantees:
    /// - unresolved `Expr::Column` references become `Expr::ColumnRef`;
    /// - expression/aggregate types are inferred and checked;
    /// - required casts are inserted for supported coercions;
    /// - join and insert contracts are validated early.
    ///
    /// Error taxonomy:
    /// - `Planning`: semantic/type/name resolution failures
    /// - `Unsupported`: valid SQL shape that analyzer intentionally does not support in v1
    pub fn analyze(&self, plan: LogicalPlan, provider: &dyn SchemaProvider) -> Result<LogicalPlan> {
        let (p, _schema, _resolver) = self.analyze_plan(plan, provider)?;
        Ok(p)
    }

    // -------------------------
    // Internal analysis plumbing
    // -------------------------

    fn analyze_plan(
        &self,
        plan: LogicalPlan,
        provider: &dyn SchemaProvider,
    ) -> Result<(LogicalPlan, SchemaRef, Resolver)> {
        match plan {
            LogicalPlan::TableScan {
                table,
                projection,
                filters,
            } => {
                let schema = provider.table_schema(&table)?;
                let mut resolver = Resolver::from_table(&table, schema.clone());

                // Analyze any scan-level filters (pushdown filters).
                let mut analyzed_filters = vec![];
                for f in filters {
                    let (af, t) = self.analyze_expr(f, &resolver)?;
                    if t != DataType::Boolean {
                        return Err(FfqError::Planning(
                            "table scan filter must be boolean".to_string(),
                        ));
                    }
                    analyzed_filters.push(af);
                }

                // Apply projection to schema/resolver if present.
                if let Some(cols) = &projection {
                    let (proj_schema, proj_resolver) = resolver.project(cols)?;
                    resolver = proj_resolver;
                    Ok((
                        LogicalPlan::TableScan {
                            table,
                            projection,
                            filters: analyzed_filters,
                        },
                        proj_schema,
                        resolver,
                    ))
                } else {
                    Ok((
                        LogicalPlan::TableScan {
                            table,
                            projection,
                            filters: analyzed_filters,
                        },
                        resolver.schema(),
                        resolver,
                    ))
                }
            }

            LogicalPlan::Filter { predicate, input } => {
                let (ain, schema, resolver) = self.analyze_plan(*input, provider)?;
                let (pred, t) = self.analyze_expr(predicate, &resolver)?;
                if t != DataType::Boolean {
                    return Err(FfqError::Planning(
                        "WHERE predicate must be boolean".to_string(),
                    ));
                }
                Ok((
                    LogicalPlan::Filter {
                        predicate: pred,
                        input: Box::new(ain),
                    },
                    schema,
                    resolver,
                ))
            }
            LogicalPlan::InSubqueryFilter {
                input,
                expr,
                subquery,
                negated,
            } => {
                let (ain, in_schema, in_resolver) = self.analyze_plan(*input, provider)?;
                let (asub, sub_schema, _sub_resolver) = self.analyze_plan(*subquery, provider)?;
                if sub_schema.fields().len() != 1 {
                    return Err(FfqError::Planning(
                        "IN subquery must return exactly one column".to_string(),
                    ));
                }
                let sub_col_name = sub_schema.field(0).name().clone();
                let sub_col_dt = sub_schema.field(0).data_type().clone();
                let (aexpr, expr_dt) = self.analyze_expr(expr, &in_resolver)?;
                let sub_expr = Expr::ColumnRef {
                    name: sub_col_name.clone(),
                    index: 0,
                };
                let (coerced_left, coerced_sub, target_dt) =
                    coerce_for_compare(aexpr, expr_dt, sub_expr, sub_col_dt)?;
                let coerced_subquery = LogicalPlan::Projection {
                    exprs: vec![(coerced_sub, "__in_key".to_string())],
                    input: Box::new(asub),
                };
                let out_schema = in_schema.clone();
                let out_resolver = Resolver::anonymous(out_schema.clone());
                let _ = target_dt;
                Ok((
                    LogicalPlan::InSubqueryFilter {
                        input: Box::new(ain),
                        expr: coerced_left,
                        subquery: Box::new(coerced_subquery),
                        negated,
                    },
                    out_schema,
                    out_resolver,
                ))
            }
            LogicalPlan::ExistsSubqueryFilter {
                input,
                subquery,
                negated,
            } => {
                let (ain, in_schema, _in_resolver) = self.analyze_plan(*input, provider)?;
                let (asub, _sub_schema, _sub_resolver) = self.analyze_plan(*subquery, provider)?;
                let out_schema = in_schema.clone();
                let out_resolver = Resolver::anonymous(out_schema.clone());
                Ok((
                    LogicalPlan::ExistsSubqueryFilter {
                        input: Box::new(ain),
                        subquery: Box::new(asub),
                        negated,
                    },
                    out_schema,
                    out_resolver,
                ))
            }
            LogicalPlan::ScalarSubqueryFilter {
                input,
                expr,
                op,
                subquery,
            } => {
                let (ain, in_schema, in_resolver) = self.analyze_plan(*input, provider)?;
                let (asub, sub_schema, _sub_resolver) = self.analyze_plan(*subquery, provider)?;
                if sub_schema.fields().len() != 1 {
                    return Err(FfqError::Planning(
                        "scalar subquery must return exactly one column".to_string(),
                    ));
                }
                let sub_col_name = sub_schema.field(0).name().clone();
                let sub_col_dt = sub_schema.field(0).data_type().clone();
                let (aexpr, expr_dt) = self.analyze_expr(expr, &in_resolver)?;
                let sub_expr = Expr::ColumnRef {
                    name: sub_col_name,
                    index: 0,
                };
                let (coerced_left, coerced_sub, _target) =
                    coerce_for_compare(aexpr, expr_dt, sub_expr, sub_col_dt)?;
                let coerced_subquery = LogicalPlan::Projection {
                    exprs: vec![(coerced_sub, "__scalar".to_string())],
                    input: Box::new(asub),
                };
                let out_schema = in_schema.clone();
                let out_resolver = Resolver::anonymous(out_schema.clone());
                Ok((
                    LogicalPlan::ScalarSubqueryFilter {
                        input: Box::new(ain),
                        expr: coerced_left,
                        op,
                        subquery: Box::new(coerced_subquery),
                    },
                    out_schema,
                    out_resolver,
                ))
            }

            LogicalPlan::Projection { exprs, input } => {
                let (ain, _in_schema, in_resolver) = self.analyze_plan(*input, provider)?;

                let mut out_fields: Vec<Field> = vec![];
                let mut out_exprs: Vec<(Expr, String)> = vec![];

                for (e, name) in exprs {
                    let (ae, dt) = self.analyze_expr(e, &in_resolver)?;
                    out_fields.push(Field::new(&name, dt.clone(), true));
                    out_exprs.push((ae, name));
                }

                let out_schema = Arc::new(Schema::new(out_fields));
                let out_resolver = Resolver::anonymous(out_schema.clone());

                Ok((
                    LogicalPlan::Projection {
                        exprs: out_exprs,
                        input: Box::new(ain),
                    },
                    out_schema,
                    out_resolver,
                ))
            }

            LogicalPlan::Aggregate {
                group_exprs,
                aggr_exprs,
                input,
            } => {
                let (ain, _in_schema, in_resolver) = self.analyze_plan(*input, provider)?;

                let mut out_fields: Vec<Field> = vec![];
                let mut out_group: Vec<Expr> = vec![];
                for g in group_exprs {
                    let (ag, dt) = self.analyze_expr(g, &in_resolver)?;
                    out_fields.push(Field::new(expr_name(&ag), dt, true));
                    out_group.push(ag);
                }

                let mut out_aggs: Vec<(AggExpr, String)> = vec![];
                for (agg, name) in aggr_exprs {
                    let (aagg, dt) = self.analyze_agg(agg, &in_resolver)?;
                    out_fields.push(Field::new(&name, dt, true));
                    out_aggs.push((aagg, name));
                }

                let out_schema = Arc::new(Schema::new(out_fields));
                let out_resolver = Resolver::anonymous(out_schema.clone());

                Ok((
                    LogicalPlan::Aggregate {
                        group_exprs: out_group,
                        aggr_exprs: out_aggs,
                        input: Box::new(ain),
                    },
                    out_schema,
                    out_resolver,
                ))
            }

            LogicalPlan::Join {
                left,
                right,
                on,
                join_type,
                strategy_hint,
            } => {
                let (al, _ls, lres) = self.analyze_plan(*left, provider)?;
                let (ar, _rs, rres) = self.analyze_plan(*right, provider)?;

                // Validate join keys exist and have compatible types.
                for (lk, rk) in &on {
                    let (_li, ldt) = lres.resolve(lk)?;
                    let (_ri, rdt) = rres.resolve(rk)?;
                    if !types_compatible_for_equality(&ldt, &rdt) {
                        return Err(FfqError::Planning(format!(
                            "join key type mismatch: {lk}({ldt:?}) vs {rk}({rdt:?})"
                        )));
                    }
                }

                let out_resolver = Resolver::join(lres, rres);
                let out_schema = out_resolver.schema();

                Ok((
                    LogicalPlan::Join {
                        left: Box::new(al),
                        right: Box::new(ar),
                        on,
                        join_type,
                        strategy_hint,
                    },
                    out_schema,
                    out_resolver,
                ))
            }

            LogicalPlan::Limit { n, input } => {
                let (ain, schema, resolver) = self.analyze_plan(*input, provider)?;
                Ok((
                    LogicalPlan::Limit {
                        n,
                        input: Box::new(ain),
                    },
                    schema,
                    resolver,
                ))
            }
            LogicalPlan::TopKByScore {
                score_expr,
                k,
                input,
            } => {
                let (ain, schema, resolver) = self.analyze_plan(*input, provider)?;
                if k == 0 {
                    return Err(FfqError::Planning("TOP-K value must be > 0".to_string()));
                }
                let (score_expr, score_dt) = self.analyze_expr(score_expr, &resolver)?;
                if !matches!(score_dt, DataType::Float32 | DataType::Float64) {
                    return Err(FfqError::Planning(format!(
                        "top-k score expression must be Float32/Float64, got {score_dt:?}"
                    )));
                }
                Ok((
                    LogicalPlan::TopKByScore {
                        score_expr,
                        k,
                        input: Box::new(ain),
                    },
                    schema,
                    resolver,
                ))
            }
            LogicalPlan::VectorTopK {
                table,
                query_vector,
                k,
                filter,
            } => {
                if k == 0 {
                    return Err(FfqError::Planning("TOP-K value must be > 0".to_string()));
                }
                if query_vector.is_empty() {
                    return Err(FfqError::Planning(
                        "vector top-k query vector cannot be empty".to_string(),
                    ));
                }
                // Validate table exists.
                let _ = provider.table_schema(&table)?;
                let out_schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("score", DataType::Float32, false),
                    Field::new("payload", DataType::Utf8, true),
                ]));
                let out_resolver = Resolver::anonymous(out_schema.clone());
                Ok((
                    LogicalPlan::VectorTopK {
                        table,
                        query_vector,
                        k,
                        filter,
                    },
                    out_schema,
                    out_resolver,
                ))
            }
            LogicalPlan::InsertInto {
                table,
                columns,
                input,
            } => {
                let (ain, input_schema, _input_resolver) = self.analyze_plan(*input, provider)?;
                let target_schema = provider.table_schema(&table)?;

                let mut target_fields: Vec<Field> = Vec::new();
                if columns.is_empty() {
                    for idx in 0..target_schema.fields().len() {
                        target_fields.push(target_schema.field(idx).clone());
                    }
                } else {
                    target_fields.reserve(columns.len());
                    for col in &columns {
                        let idx = target_schema.index_of(col).map_err(|_| {
                            FfqError::Planning(format!(
                                "INSERT target column '{col}' not found in table '{table}'"
                            ))
                        })?;
                        target_fields.push(target_schema.field(idx).clone());
                    }
                }

                if input_schema.fields().len() != target_fields.len() {
                    return Err(FfqError::Planning(format!(
                        "INSERT column count mismatch: target has {}, SELECT has {}",
                        target_fields.len(),
                        input_schema.fields().len()
                    )));
                }

                for (idx, (src, dst)) in input_schema
                    .fields()
                    .iter()
                    .zip(target_fields.iter())
                    .enumerate()
                {
                    if !insert_type_compatible(src.data_type(), dst.data_type()) {
                        return Err(FfqError::Planning(format!(
                            "INSERT type mismatch at column {}: target '{}' is {:?}, SELECT is {:?}",
                            idx,
                            dst.name(),
                            dst.data_type(),
                            src.data_type()
                        )));
                    }
                }

                let out_schema = Arc::new(Schema::new(target_fields));
                let out_resolver = Resolver::anonymous(out_schema.clone());
                Ok((
                    LogicalPlan::InsertInto {
                        table,
                        columns,
                        input: Box::new(ain),
                    },
                    out_schema,
                    out_resolver,
                ))
            }
        }
    }

    fn analyze_agg(&self, agg: AggExpr, resolver: &Resolver) -> Result<(AggExpr, DataType)> {
        match agg {
            AggExpr::Count(e) => {
                let (ae, _dt) = self.analyze_expr(e, resolver)?;
                Ok((AggExpr::Count(ae), DataType::Int64))
            }
            AggExpr::Sum(e) => {
                let (ae, dt) = self.analyze_expr(e, resolver)?;
                if !is_numeric(&dt) {
                    return Err(FfqError::Planning("SUM() requires numeric".to_string()));
                }
                Ok((AggExpr::Sum(ae), dt))
            }
            AggExpr::Min(e) => {
                let (ae, dt) = self.analyze_expr(e, resolver)?;
                Ok((AggExpr::Min(ae), dt))
            }
            AggExpr::Max(e) => {
                let (ae, dt) = self.analyze_expr(e, resolver)?;
                Ok((AggExpr::Max(ae), dt))
            }
            AggExpr::Avg(e) => {
                let (ae, dt) = self.analyze_expr(e, resolver)?;
                if !is_numeric(&dt) {
                    return Err(FfqError::Planning("AVG() requires numeric".to_string()));
                }
                // v1: avg returns float
                Ok((AggExpr::Avg(ae), DataType::Float64))
            }
        }
    }

    fn analyze_expr(&self, expr: Expr, resolver: &Resolver) -> Result<(Expr, DataType)> {
        match expr {
            Expr::Column(name) => {
                let (idx, dt) = resolver.resolve(&name)?;
                Ok((Expr::ColumnRef { name, index: idx }, dt))
            }
            Expr::ColumnRef { name, index } => {
                let dt = resolver.data_type_at(index)?;
                Ok((Expr::ColumnRef { name, index }, dt))
            }
            Expr::Literal(v) => Ok((Expr::Literal(v.clone()), literal_type(&v))),
            Expr::Cast { expr, to_type } => {
                let (ae, _dt) = self.analyze_expr(*expr, resolver)?;
                Ok((
                    Expr::Cast {
                        expr: Box::new(ae),
                        to_type: to_type.clone(),
                    },
                    to_type,
                ))
            }
            Expr::And(l, r) => {
                let (al, ldt) = self.analyze_expr(*l, resolver)?;
                let (ar, rdt) = self.analyze_expr(*r, resolver)?;
                if ldt != DataType::Boolean || rdt != DataType::Boolean {
                    return Err(FfqError::Planning(
                        "AND requires boolean operands".to_string(),
                    ));
                }
                Ok((Expr::And(Box::new(al), Box::new(ar)), DataType::Boolean))
            }
            Expr::Or(l, r) => {
                let (al, ldt) = self.analyze_expr(*l, resolver)?;
                let (ar, rdt) = self.analyze_expr(*r, resolver)?;
                if ldt != DataType::Boolean || rdt != DataType::Boolean {
                    return Err(FfqError::Planning(
                        "OR requires boolean operands".to_string(),
                    ));
                }
                Ok((Expr::Or(Box::new(al), Box::new(ar)), DataType::Boolean))
            }
            Expr::Not(e) => {
                let (ae, dt) = self.analyze_expr(*e, resolver)?;
                if dt != DataType::Boolean {
                    return Err(FfqError::Planning(
                        "NOT requires boolean operand".to_string(),
                    ));
                }
                Ok((Expr::Not(Box::new(ae)), DataType::Boolean))
            }
            Expr::CaseWhen {
                branches,
                else_expr,
            } => {
                if branches.is_empty() {
                    return Err(FfqError::Planning(
                        "CASE requires at least one WHEN/THEN branch".to_string(),
                    ));
                }
                let mut analyzed_branches = Vec::with_capacity(branches.len());
                let mut result_types = Vec::with_capacity(branches.len() + 1);
                for (cond, result) in branches {
                    let (acond, cdt) = self.analyze_expr(cond, resolver)?;
                    if cdt != DataType::Boolean {
                        return Err(FfqError::Planning(
                            "CASE WHEN condition must be boolean".to_string(),
                        ));
                    }
                    let (aresult, rdt) = self.analyze_expr(result, resolver)?;
                    analyzed_branches.push((acond, aresult));
                    result_types.push(rdt);
                }

                let (analyzed_else, else_dt) = if let Some(e) = else_expr {
                    self.analyze_expr(*e, resolver)?
                } else {
                    (Expr::Literal(LiteralValue::Null), DataType::Null)
                };
                result_types.push(else_dt.clone());
                let target_dt = coerce_case_result_type(&result_types)?;

                let coerced_branches = analyzed_branches
                    .into_iter()
                    .zip(result_types.iter())
                    .map(|((cond, result), rdt)| (cond, cast_if_needed(result, rdt, &target_dt)))
                    .collect::<Vec<_>>();
                let coerced_else = cast_if_needed(analyzed_else, &else_dt, &target_dt);

                Ok((
                    Expr::CaseWhen {
                        branches: coerced_branches,
                        else_expr: Some(Box::new(coerced_else)),
                    },
                    target_dt,
                ))
            }
            Expr::BinaryOp { left, op, right } => {
                let (al, ldt) = self.analyze_expr(*left, resolver)?;
                let (ar, rdt) = self.analyze_expr(*right, resolver)?;

                match op {
                    // comparisons -> boolean, plus numeric widening / string unify
                    BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq => {
                        let (cl, cr, _common) = coerce_for_compare(al, ldt, ar, rdt)?;
                        Ok((
                            Expr::BinaryOp {
                                left: Box::new(cl),
                                op,
                                right: Box::new(cr),
                            },
                            DataType::Boolean,
                        ))
                    }

                    // arithmetic -> numeric result
                    BinaryOp::Plus | BinaryOp::Minus | BinaryOp::Multiply | BinaryOp::Divide => {
                        let (cl, cr, out) = coerce_for_arith(op, al, ldt, ar, rdt)?;
                        Ok((
                            Expr::BinaryOp {
                                left: Box::new(cl),
                                op,
                                right: Box::new(cr),
                            },
                            out,
                        ))
                    }
                }
            }

            #[cfg(feature = "vector")]
            Expr::CosineSimilarity { vector, query } => {
                let (av, vdt) = self.analyze_expr(*vector, resolver)?;
                ensure_vector_type("cosine_similarity", &vdt)?;
                let (aq, qdt) = self.analyze_expr(*query, resolver)?;
                ensure_vector_literal_type("cosine_similarity", &qdt)?;
                Ok((
                    Expr::CosineSimilarity {
                        vector: Box::new(av),
                        query: Box::new(aq),
                    },
                    DataType::Float32,
                ))
            }
            #[cfg(feature = "vector")]
            Expr::L2Distance { vector, query } => {
                let (av, vdt) = self.analyze_expr(*vector, resolver)?;
                ensure_vector_type("l2_distance", &vdt)?;
                let (aq, qdt) = self.analyze_expr(*query, resolver)?;
                ensure_vector_literal_type("l2_distance", &qdt)?;
                Ok((
                    Expr::L2Distance {
                        vector: Box::new(av),
                        query: Box::new(aq),
                    },
                    DataType::Float32,
                ))
            }
            #[cfg(feature = "vector")]
            Expr::DotProduct { vector, query } => {
                let (av, vdt) = self.analyze_expr(*vector, resolver)?;
                ensure_vector_type("dot_product", &vdt)?;
                let (aq, qdt) = self.analyze_expr(*query, resolver)?;
                ensure_vector_literal_type("dot_product", &qdt)?;
                Ok((
                    Expr::DotProduct {
                        vector: Box::new(av),
                        query: Box::new(aq),
                    },
                    DataType::Float32,
                ))
            }
            Expr::ScalarUdf { name, args } => {
                let mut analyzed_args = Vec::with_capacity(args.len());
                let mut arg_types = Vec::with_capacity(args.len());
                for arg in args {
                    let (a, dt) = self.analyze_expr(arg, resolver)?;
                    analyzed_args.push(a);
                    arg_types.push(dt);
                }
                let resolver_fn = self
                    .udf_type_resolvers
                    .read()
                    .expect("udf resolver lock poisoned")
                    .get(&name.to_ascii_lowercase())
                    .cloned()
                    .ok_or_else(|| FfqError::Planning(format!("unknown scalar udf: {name}")))?;
                let out_type = resolver_fn(&arg_types)?;
                Ok((
                    Expr::ScalarUdf {
                        name,
                        args: analyzed_args,
                    },
                    out_type,
                ))
            }
        }
    }
}

// -------------------------
// Resolver (name -> idx, dt)
// -------------------------

#[derive(Debug, Clone)]
struct Relation {
    name: String,
    fields: Vec<Arc<Field>>,
}

#[derive(Debug, Clone)]
struct Resolver {
    relations: Vec<Relation>,
}

impl Resolver {
    fn from_table(table: &str, schema: SchemaRef) -> Self {
        Self {
            relations: vec![Relation {
                name: table.to_string(),
                fields: schema.fields().iter().cloned().collect(),
            }],
        }
    }

    fn anonymous(schema: SchemaRef) -> Self {
        Self {
            relations: vec![Relation {
                name: "".to_string(),
                fields: schema.fields().iter().cloned().collect(),
            }],
        }
    }

    fn join(left: Resolver, right: Resolver) -> Self {
        let mut rels = vec![];
        rels.extend(left.relations);
        rels.extend(right.relations);
        Self { relations: rels }
    }

    fn schema(&self) -> SchemaRef {
        let mut fields: Vec<Field> = vec![];
        for r in &self.relations {
            for f in &r.fields {
                fields.push((**f).clone());
            }
        }
        Arc::new(Schema::new(fields))
    }

    fn project(&self, cols: &[String]) -> Result<(SchemaRef, Resolver)> {
        // Only allow projection of columns by name (unqualified or qualified)
        let mut out_fields = vec![];
        for c in cols {
            let (idx, _dt) = self.resolve(c)?;
            let f = self.field_at(idx)?;
            out_fields.push(f);
        }
        let schema = Arc::new(Schema::new(out_fields));
        Ok((schema.clone(), Resolver::anonymous(schema)))
    }

    fn resolve(&self, col: &str) -> Result<(usize, DataType)> {
        let (rel_opt, name) = split_qual(col);

        let mut found: Vec<(usize, DataType)> = vec![];
        let mut base = 0usize;

        for r in &self.relations {
            let rel_match = match rel_opt {
                Some(rel) => r.name == rel,
                None => true,
            };

            if rel_match {
                for (i, f) in r.fields.iter().enumerate() {
                    if f.name() == name {
                        found.push((base + i, f.data_type().clone()));
                    }
                }
            }
            base += r.fields.len();
        }

        match found.len() {
            0 => Err(FfqError::Planning(format!("unknown column: {col}"))),
            1 => Ok(found[0].clone()),
            _ => Err(FfqError::Planning(format!(
                "ambiguous column reference: {col} (use table.col)"
            ))),
        }
    }

    fn field_at(&self, idx: usize) -> Result<Field> {
        let mut base = 0usize;
        for r in &self.relations {
            if idx < base + r.fields.len() {
                let f: &Arc<Field> = &r.fields[idx - base];
                return Ok((**f).clone());
            }
            base += r.fields.len();
        }
        Err(FfqError::Planning(format!(
            "column index out of range: {idx}"
        )))
    }

    fn data_type_at(&self, idx: usize) -> Result<DataType> {
        Ok(self.field_at(idx)?.data_type().clone())
    }
}

fn split_qual(s: &str) -> (Option<&str>, &str) {
    if let Some((a, b)) = s.split_once('.') {
        (Some(a), b)
    } else {
        (None, s)
    }
}

// -------------------------
// Type inference + casts
// -------------------------

fn literal_type(v: &LiteralValue) -> DataType {
    match v {
        LiteralValue::Int64(_) => DataType::Int64,
        LiteralValue::Float64(_) => DataType::Float64,
        LiteralValue::Utf8(_) => DataType::Utf8,
        LiteralValue::Boolean(_) => DataType::Boolean,
        LiteralValue::Null => DataType::Null,
        #[cfg(feature = "vector")]
        LiteralValue::VectorF32(v) => DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)),
            v.len() as i32,
        ),
    }
}

#[cfg(feature = "vector")]
fn ensure_vector_type(func_name: &str, dt: &DataType) -> Result<()> {
    match dt {
        DataType::FixedSizeList(field, _) if field.data_type() == &DataType::Float32 => Ok(()),
        other => Err(FfqError::Planning(format!(
            "{func_name} requires vector column type FixedSizeList<Float32>, got {other:?}"
        ))),
    }
}

#[cfg(feature = "vector")]
fn ensure_vector_literal_type(func_name: &str, dt: &DataType) -> Result<()> {
    ensure_vector_type(func_name, dt)
}

fn is_numeric(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    )
}

fn insert_type_compatible(src: &DataType, dst: &DataType) -> bool {
    src == dst
        || matches!(
            (src, dst),
            (DataType::Int64, DataType::Float64) | (DataType::Float64, DataType::Int64)
        )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema, SchemaRef};

    use super::{Analyzer, SchemaProvider};
    use crate::logical_plan::LogicalPlan;
    use crate::sql_frontend::sql_to_logical;

    struct TestSchemaProvider {
        schemas: HashMap<String, SchemaRef>,
    }

    impl SchemaProvider for TestSchemaProvider {
        fn table_schema(&self, table: &str) -> ffq_common::Result<SchemaRef> {
            self.schemas
                .get(table)
                .cloned()
                .ok_or_else(|| ffq_common::FfqError::Planning(format!("unknown table: {table}")))
        }
    }

    #[test]
    fn analyze_insert_valid() {
        let mut schemas = HashMap::new();
        schemas.insert(
            "src".to_string(),
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)])),
        );
        schemas.insert(
            "dst".to_string(),
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)])),
        );
        let provider = TestSchemaProvider { schemas };
        let analyzer = Analyzer::new();
        let plan = sql_to_logical("INSERT INTO dst SELECT a FROM src", &HashMap::new())
            .expect("parse insert");
        let analyzed = analyzer.analyze(plan, &provider).expect("analyze insert");
        assert!(matches!(analyzed, LogicalPlan::InsertInto { .. }));
    }

    #[test]
    fn analyze_insert_type_mismatch() {
        let mut schemas = HashMap::new();
        schemas.insert(
            "src".to_string(),
            Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)])),
        );
        schemas.insert(
            "dst".to_string(),
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)])),
        );
        let provider = TestSchemaProvider { schemas };
        let analyzer = Analyzer::new();
        let plan = sql_to_logical("INSERT INTO dst SELECT a FROM src", &HashMap::new())
            .expect("parse insert");
        let err = analyzer
            .analyze(plan, &provider)
            .expect_err("expected type mismatch");
        assert!(
            err.to_string().contains("INSERT type mismatch"),
            "err={err}"
        );
    }

    #[cfg(feature = "vector")]
    #[test]
    fn analyze_cosine_similarity_requires_fixed_size_list_f32() {
        let mut schemas = HashMap::new();
        schemas.insert(
            "docs".to_string(),
            Arc::new(Schema::new(vec![Field::new(
                "emb",
                DataType::Float64,
                false,
            )])),
        );
        let provider = TestSchemaProvider { schemas };
        let analyzer = Analyzer::new();

        let mut params = HashMap::new();
        params.insert(
            "q".to_string(),
            crate::logical_plan::LiteralValue::VectorF32(vec![1.0, 0.0, 0.0]),
        );
        let plan = sql_to_logical(
            "SELECT cosine_similarity(emb, :q) AS score FROM docs",
            &params,
        )
        .expect("parse");
        let err = analyzer.analyze(plan, &provider).expect_err("must fail");
        assert!(
            err.to_string()
                .contains("requires vector column type FixedSizeList<Float32>"),
            "unexpected error: {err}"
        );
    }
}

fn numeric_rank(dt: &DataType) -> Option<u8> {
    Some(match dt {
        DataType::Int8 => 1,
        DataType::Int16 => 2,
        DataType::Int32 => 3,
        DataType::Int64 => 4,
        DataType::UInt8 => 1,
        DataType::UInt16 => 2,
        DataType::UInt32 => 3,
        DataType::UInt64 => 4,
        DataType::Float32 => 5,
        DataType::Float64 => 6,
        _ => return None,
    })
}

fn wider_numeric(a: &DataType, b: &DataType) -> Option<DataType> {
    let ra = numeric_rank(a)?;
    let rb = numeric_rank(b)?;
    if ra >= rb {
        Some(a.clone())
    } else {
        Some(b.clone())
    }
}

fn cast_if_needed(expr: Expr, from: &DataType, to: &DataType) -> Expr {
    if from == to {
        expr
    } else {
        Expr::Cast {
            expr: Box::new(expr),
            to_type: to.clone(),
        }
    }
}

fn coerce_for_compare(
    left: Expr,
    ldt: DataType,
    right: Expr,
    rdt: DataType,
) -> Result<(Expr, Expr, DataType)> {
    // Null can be cast to the other side.
    if ldt == DataType::Null {
        return Ok((cast_if_needed(left, &ldt, &rdt), right, rdt));
    }
    if rdt == DataType::Null {
        return Ok((left, cast_if_needed(right, &rdt, &ldt), ldt));
    }

    // Numeric widen
    if is_numeric(&ldt) && is_numeric(&rdt) {
        let target = wider_numeric(&ldt, &rdt).ok_or_else(|| {
            FfqError::Planning("failed to determine numeric widening type".to_string())
        })?;
        return Ok((
            cast_if_needed(left, &ldt, &target),
            cast_if_needed(right, &rdt, &target),
            target,
        ));
    }

    // String unify
    if matches!(ldt, DataType::Utf8 | DataType::LargeUtf8)
        && matches!(rdt, DataType::Utf8 | DataType::LargeUtf8)
    {
        let target = if ldt == DataType::LargeUtf8 || rdt == DataType::LargeUtf8 {
            DataType::LargeUtf8
        } else {
            DataType::Utf8
        };
        return Ok((
            cast_if_needed(left, &ldt, &target),
            cast_if_needed(right, &rdt, &target),
            target,
        ));
    }

    // Boolean equality allowed; ordering not ideal but keep minimal.
    if ldt == rdt {
        return Ok((left, right, ldt));
    }

    Err(FfqError::Planning(format!(
        "cannot compare types {ldt:?} and {rdt:?}"
    )))
}

fn coerce_for_arith(
    op: BinaryOp,
    left: Expr,
    ldt: DataType,
    right: Expr,
    rdt: DataType,
) -> Result<(Expr, Expr, DataType)> {
    if !is_numeric(&ldt) || !is_numeric(&rdt) {
        return Err(FfqError::Planning(
            "arithmetic requires numeric operands".to_string(),
        ));
    }

    // v1 choice: division produces float
    if op == BinaryOp::Divide {
        let target = DataType::Float64;
        return Ok((
            cast_if_needed(left, &ldt, &target),
            cast_if_needed(right, &rdt, &target),
            target,
        ));
    }

    let target = wider_numeric(&ldt, &rdt)
        .ok_or_else(|| FfqError::Planning("failed numeric widening".to_string()))?;
    Ok((
        cast_if_needed(left, &ldt, &target),
        cast_if_needed(right, &rdt, &target),
        target,
    ))
}

fn coerce_case_result_type(types: &[DataType]) -> Result<DataType> {
    let mut target: Option<DataType> = None;
    for dt in types {
        if *dt == DataType::Null {
            continue;
        }
        target = Some(match target {
            None => dt.clone(),
            Some(t) if t == *dt => t,
            Some(t) if is_numeric(&t) && is_numeric(dt) => wider_numeric(&t, dt).ok_or_else(|| {
                FfqError::Planning("failed to determine CASE numeric widening type".to_string())
            })?,
            Some(DataType::Utf8) if *dt == DataType::LargeUtf8 => DataType::LargeUtf8,
            Some(DataType::LargeUtf8) if *dt == DataType::Utf8 => DataType::LargeUtf8,
            Some(DataType::Utf8) if *dt == DataType::Utf8 => DataType::Utf8,
            Some(DataType::LargeUtf8) if *dt == DataType::LargeUtf8 => DataType::LargeUtf8,
            Some(DataType::Boolean) if *dt == DataType::Boolean => DataType::Boolean,
            Some(t) => {
                return Err(FfqError::Planning(format!(
                    "CASE branch type mismatch: cannot unify {t:?} and {dt:?}"
                )));
            }
        });
    }
    Ok(target.unwrap_or(DataType::Null))
}

fn types_compatible_for_equality(a: &DataType, b: &DataType) -> bool {
    if a == b {
        return true;
    }
    if is_numeric(a) && is_numeric(b) {
        return true;
    }
    matches!(
        (a, b),
        (DataType::Utf8, DataType::LargeUtf8) | (DataType::LargeUtf8, DataType::Utf8)
    )
}

fn expr_name(e: &Expr) -> &str {
    match e {
        Expr::Column(name) => name.as_str(),
        Expr::ColumnRef { name, .. } => name.as_str(),
        _ => "expr",
    }
}
