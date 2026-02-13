use ffq_planner::{BinaryOp, Expr, LiteralValue};

pub fn col(name: &str) -> Expr {
    Expr::Column(name.to_string())
}

pub fn lit_i64(v: i64) -> Expr {
    Expr::Literal(LiteralValue::Int64(v))
}

pub fn lit_f64(v: f64) -> Expr {
    Expr::Literal(LiteralValue::Float64(v))
}

pub fn lit_bool(v: bool) -> Expr {
    Expr::Literal(LiteralValue::Boolean(v))
}

pub fn lit_str(v: &str) -> Expr {
    Expr::Literal(LiteralValue::Utf8(v.to_string()))
}

pub fn eq(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOp::Eq,
        right: Box::new(right),
    }
}

pub fn and(left: Expr, right: Expr) -> Expr {
    Expr::And(Box::new(left), Box::new(right))
}

pub fn or(left: Expr, right: Expr) -> Expr {
    Expr::Or(Box::new(left), Box::new(right))
}
