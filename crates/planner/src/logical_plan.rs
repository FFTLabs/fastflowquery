use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expr {
    Column(String),
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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
        schema: Option<SchemaRef>,
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

pub fn schema_placeholder() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("placeholder", DataType::Null, true)]))
}
