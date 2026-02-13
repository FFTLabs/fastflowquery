use arrow_schema::{DataType};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinStrategyHint {
    Auto,
    BroadcastLeft,
    BroadcastRight,
    Shuffle,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expr {
    Column(String),
    ColumnRef { name: String, index: usize },
    Literal(LiteralValue),
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    Cast {
        expr: Box<Expr>,
        to_type: DataType,
    },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),

    #[cfg(feature = "vector")]
    CosineSimilarity { vector: Box<Expr>, query: Box<Expr> },
    #[cfg(feature = "vector")]
    L2Distance { vector: Box<Expr>, query: Box<Expr> },
    #[cfg(feature = "vector")]
    DotProduct { vector: Box<Expr>, query: Box<Expr> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LiteralValue {
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Multiply,
    Divide,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalPlan {
    TableScan {
        table: String,
        projection: Option<Vec<String>>,
        filters: Vec<Expr>,
    },
    Projection {
        exprs: Vec<(Expr, String)>,
        input: Box<LogicalPlan>,
    },
    Filter {
        predicate: Expr,
        input: Box<LogicalPlan>,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        on: Vec<(String, String)>,
        join_type: JoinType,
        strategy_hint: JoinStrategyHint,
    },
    Aggregate {
        group_exprs: Vec<Expr>,
        aggr_exprs: Vec<(AggExpr, String)>,
        input: Box<LogicalPlan>,
    },
    Limit {
        n: usize,
        input: Box<LogicalPlan>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggExpr {
    Count(Expr),
    Sum(Expr),
    Min(Expr),
    Max(Expr),
    Avg(Expr),
}
