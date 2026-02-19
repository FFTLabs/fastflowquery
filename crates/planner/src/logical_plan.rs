use arrow_schema::DataType;
use serde::{Deserialize, Serialize};

/// Join semantics supported by the logical planner.
///
/// v1 currently only supports [`JoinType::Inner`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    /// Keep only rows where join keys match on both sides.
    Inner,
}

/// Optimizer hint controlling join distribution strategy.
///
/// This is a hint, not a hard promise. Physical planning may still choose a
/// safe fallback shape when constraints are not met.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinStrategyHint {
    /// Let optimizer/physical planner pick the strategy.
    Auto,
    /// Broadcast left side and build hash table from left.
    BroadcastLeft,
    /// Broadcast right side and build hash table from right.
    BroadcastRight,
    /// Shuffle both sides by join key and join partition-wise.
    Shuffle,
}

/// Scalar expression used by logical and physical planning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expr {
    /// Unresolved column name (before analysis).
    Column(String),
    /// Resolved column binding emitted by analyzer.
    ColumnRef {
        /// Resolved display name.
        name: String,
        /// Resolved column index in input schema.
        index: usize,
    },
    /// Scalar literal.
    Literal(LiteralValue),
    /// Binary operator expression.
    BinaryOp {
        /// Left operand.
        left: Box<Expr>,
        /// Binary operator.
        op: BinaryOp,
        /// Right operand.
        right: Box<Expr>,
    },
    /// Explicit type cast.
    Cast {
        /// Input expression.
        expr: Box<Expr>,
        /// Target type.
        to_type: DataType,
    },
    /// Boolean conjunction.
    And(Box<Expr>, Box<Expr>),
    /// Boolean disjunction.
    Or(Box<Expr>, Box<Expr>),
    /// Boolean negation.
    Not(Box<Expr>),

    #[cfg(feature = "vector")]
    CosineSimilarity { vector: Box<Expr>, query: Box<Expr> },
    #[cfg(feature = "vector")]
    L2Distance { vector: Box<Expr>, query: Box<Expr> },
    #[cfg(feature = "vector")]
    DotProduct { vector: Box<Expr>, query: Box<Expr> },
}

/// Literal values supported by the v1 planner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LiteralValue {
    /// 64-bit integer literal.
    Int64(i64),
    /// 64-bit floating literal.
    Float64(f64),
    /// UTF-8 string literal.
    Utf8(String),
    /// Boolean literal.
    Boolean(bool),
    /// Null literal.
    Null,

    #[cfg(feature = "vector")]
    VectorF32(Vec<f32>),
}

/// Binary operators supported by v1 expression evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryOp {
    /// Equality.
    Eq,
    /// Inequality.
    NotEq,
    /// Less-than.
    Lt,
    /// Less-than or equal.
    LtEq,
    /// Greater-than.
    Gt,
    /// Greater-than or equal.
    GtEq,
    /// Addition.
    Plus,
    /// Subtraction.
    Minus,
    /// Multiplication.
    Multiply,
    /// Division.
    Divide,
}

/// Logical plan tree produced by SQL/DataFrame frontend and rewritten by
/// analyzer/optimizer passes.
///
/// Contracts:
/// - `TableScan.projection` is best-effort pushdown and may be widened later.
/// - `Join.on` uses `(left_col, right_col)` column names.
/// - `Aggregate` uses SQL grouped-aggregate semantics.
/// - `TopKByScore` is the safe fallback path when vector index rewrite cannot
///   be applied.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalPlan {
    /// Scan a catalog table.
    TableScan {
        /// Catalog table name.
        table: String,
        /// Optional projected column names.
        projection: Option<Vec<String>>,
        /// Best-effort pushdown filters.
        filters: Vec<Expr>,
    },
    /// Compute named expressions from input rows.
    Projection {
        /// `(expr, output_name)` pairs.
        exprs: Vec<(Expr, String)>,
        /// Input plan.
        input: Box<LogicalPlan>,
    },
    /// Keep rows matching predicate.
    Filter {
        /// Boolean predicate.
        predicate: Expr,
        /// Input plan.
        input: Box<LogicalPlan>,
    },
    /// Equi-join two inputs using `on` key pairs.
    Join {
        /// Left input.
        left: Box<LogicalPlan>,
        /// Right input.
        right: Box<LogicalPlan>,
        /// Join key pairs `(left_col, right_col)`.
        on: Vec<(String, String)>,
        /// Join type.
        join_type: JoinType,
        /// Distribution strategy hint.
        strategy_hint: JoinStrategyHint,
    },
    /// Grouped aggregate.
    ///
    /// `group_exprs` define grouping keys; `aggr_exprs` define aggregate
    /// outputs and aliases.
    Aggregate {
        /// Grouping expressions.
        group_exprs: Vec<Expr>,
        /// Aggregate expressions and aliases.
        aggr_exprs: Vec<(AggExpr, String)>,
        /// Input plan.
        input: Box<LogicalPlan>,
    },
    /// Return at most `n` rows.
    Limit {
        /// Maximum number of rows.
        n: usize,
        /// Input plan.
        input: Box<LogicalPlan>,
    },
    /// Return top `k` rows by score expression.
    ///
    /// This is used for brute-force vector reranking and remains the fallback
    /// when index-backed rewrite preconditions fail.
    TopKByScore {
        /// Score expression.
        score_expr: Expr,
        /// Number of rows to keep.
        k: usize,
        /// Input plan.
        input: Box<LogicalPlan>,
    },
    /// Index-backed vector top-k logical operator.
    ///
    /// Rewritten from `TopKByScore` only when optimizer preconditions are met.
    VectorTopK {
        /// Table name.
        table: String,
        /// Query vector literal.
        query_vector: Vec<f32>,
        /// Number of rows to keep.
        k: usize,
        /// Optional provider-specific filter payload.
        filter: Option<String>,
    },
    /// Insert query result into a target table.
    InsertInto {
        /// Target table.
        table: String,
        /// Target column list.
        columns: Vec<String>,
        /// Input plan.
        input: Box<LogicalPlan>,
    },
}

/// Aggregate expression kinds supported by v1.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggExpr {
    /// Count non-null input rows.
    Count(Expr),
    /// Sum numeric input.
    Sum(Expr),
    /// Minimum input value.
    Min(Expr),
    /// Maximum input value.
    Max(Expr),
    /// Average numeric input.
    Avg(Expr),
}
