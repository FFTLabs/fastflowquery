//! Expression compilation and evaluation for execution operators.
//!
//! Input contract:
//! - analyzer has resolved/typed expressions (primarily `ColumnRef`);
//! - execution may still accept unresolved `Column` as a compatibility fallback.
//!
//! Output contract:
//! - each evaluation returns an `ArrayRef` aligned to input batch row count.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int64Array,
    Int64Builder, StringArray, StringBuilder,
};
use arrow::compute::kernels::{
    boolean::{and_kleene, not, or_kleene},
    cast::cast,
    cmp::{eq, gt, gt_eq, lt, lt_eq, neq},
    numeric::{add, div, mul, sub},
};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use ffq_common::{FfqError, Result};

use crate::udf::get_scalar_udf;
use ffq_planner::{BinaryOp, Expr, LiteralValue};

/// Executable expression for the execution engine.
///
/// v1 philosophy:
/// - planner/analyzer produces Expr trees (mostly ColumnRef)
/// - execution compiles Expr -> PhysicalExpr
/// - evaluation returns Arrow ArrayRef aligned with the input RecordBatch length
pub trait PhysicalExpr: Send + Sync {
    /// Static output data type of this expression.
    fn data_type(&self) -> DataType;
    /// Evaluate the expression for every row in `batch`.
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef>;
}

/// Compile planner Expr into a runnable expression.
/// In v1 we expect analysis already ran, so columns should mostly be ColumnRef.
pub fn compile_expr(expr: &Expr, input_schema: &SchemaRef) -> Result<Arc<dyn PhysicalExpr>> {
    match expr {
        Expr::ColumnRef { index, .. } => {
            let dt = input_schema.field(*index).data_type().clone();
            Ok(Arc::new(ColumnExpr { index: *index, dt }))
        }
        Expr::Column(name) => {
            // Fallback while iterating: analyzer should resolve to ColumnRef eventually.
            let idx = input_schema
                .fields()
                .iter()
                .position(|f| f.name() == name)
                .ok_or_else(|| {
                    FfqError::Planning(format!("unknown column in execution: {name}"))
                })?;
            let dt = input_schema.field(idx).data_type().clone();
            Ok(Arc::new(ColumnExpr { index: idx, dt }))
        }

        Expr::Literal(v) => Ok(Arc::new(LiteralExpr {
            v: v.clone(),
            dt: literal_type(v),
        })),

        Expr::Cast { expr, to_type } => {
            let inner = compile_expr(expr, input_schema)?;
            Ok(Arc::new(CastExpr {
                inner,
                to_type: to_type.clone(),
            }))
        }

        Expr::Not(e) => {
            let inner = compile_expr(e, input_schema)?;
            Ok(Arc::new(NotExpr { inner }))
        }

        Expr::And(a, b) => {
            let left = compile_expr(a, input_schema)?;
            let right = compile_expr(b, input_schema)?;
            Ok(Arc::new(BoolBinaryExpr {
                left,
                right,
                op: BoolOp::And,
            }))
        }

        Expr::Or(a, b) => {
            let left = compile_expr(a, input_schema)?;
            let right = compile_expr(b, input_schema)?;
            Ok(Arc::new(BoolBinaryExpr {
                left,
                right,
                op: BoolOp::Or,
            }))
        }

        Expr::BinaryOp { left, op, right } => {
            let l = compile_expr(left, input_schema)?;
            let r = compile_expr(right, input_schema)?;
            let out = binary_out_type(*op, l.data_type(), r.data_type())?;

            Ok(Arc::new(BinaryExpr {
                left: l,
                right: r,
                op: *op,
                out,
            }))
        }
        Expr::ScalarUdf { name, args } => {
            let compiled_args = args
                .iter()
                .map(|a| compile_expr(a, input_schema))
                .collect::<Result<Vec<_>>>()?;
            let udf = get_scalar_udf(name).ok_or_else(|| {
                FfqError::Execution(format!(
                    "scalar udf '{}' is not registered in execution registry",
                    name
                ))
            })?;
            let out = udf.return_type(
                &compiled_args
                    .iter()
                    .map(|arg| arg.data_type())
                    .collect::<Vec<_>>(),
            )?;
            Ok(Arc::new(ScalarUdfExpr {
                udf_name: name.clone(),
                udf,
                args: compiled_args,
                out,
            }))
        }

        // ---------------- vector expressions ----------------
        #[cfg(feature = "vector")]
        Expr::CosineSimilarity { vector, query } => {
            let v = compile_expr(vector, input_schema)?;
            let q = extract_vector_literal(query)?;
            Ok(Arc::new(VectorExpr {
                vector: v,
                query: q,
                kind: VectorKind::Cosine,
            }))
        }

        #[cfg(feature = "vector")]
        Expr::L2Distance { vector, query } => {
            let v = compile_expr(vector, input_schema)?;
            let q = extract_vector_literal(query)?;
            Ok(Arc::new(VectorExpr {
                vector: v,
                query: q,
                kind: VectorKind::L2,
            }))
        }

        #[cfg(feature = "vector")]
        Expr::DotProduct { vector, query } => {
            let v = compile_expr(vector, input_schema)?;
            let q = extract_vector_literal(query)?;
            Ok(Arc::new(VectorExpr {
                vector: v,
                query: q,
                kind: VectorKind::Dot,
            }))
        }
    }
}

// =====================
// Standard expressions
// =====================

struct ColumnExpr {
    index: usize,
    dt: DataType,
}

impl PhysicalExpr for ColumnExpr {
    fn data_type(&self) -> DataType {
        self.dt.clone()
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        Ok(batch.column(self.index).clone())
    }
}

struct LiteralExpr {
    v: LiteralValue,
    dt: DataType,
}

impl PhysicalExpr for LiteralExpr {
    fn data_type(&self) -> DataType {
        self.dt.clone()
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        scalar_to_array(&self.v, batch.num_rows())
    }
}

struct CastExpr {
    inner: Arc<dyn PhysicalExpr>,
    to_type: DataType,
}

impl PhysicalExpr for CastExpr {
    fn data_type(&self) -> DataType {
        self.to_type.clone()
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let arr = self.inner.evaluate(batch)?;
        cast(&arr, &self.to_type).map_err(|e| FfqError::Execution(format!("cast failed: {e}")))
    }
}

struct NotExpr {
    inner: Arc<dyn PhysicalExpr>,
}

impl PhysicalExpr for NotExpr {
    fn data_type(&self) -> DataType {
        DataType::Boolean
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let arr = self.inner.evaluate(batch)?;
        let b = arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| FfqError::Execution("NOT expects boolean".to_string()))?;

        let out = not(b).map_err(|e| FfqError::Execution(format!("not failed: {e}")))?;
        Ok(Arc::new(out))
    }
}

#[derive(Clone, Copy)]
enum BoolOp {
    And,
    Or,
}

struct BoolBinaryExpr {
    left: Arc<dyn PhysicalExpr>,
    right: Arc<dyn PhysicalExpr>,
    op: BoolOp,
}

impl PhysicalExpr for BoolBinaryExpr {
    fn data_type(&self) -> DataType {
        DataType::Boolean
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let l = self.left.evaluate(batch)?;
        let r = self.right.evaluate(batch)?;

        let lb = l
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| FfqError::Execution("AND/OR expects boolean".to_string()))?;
        let rb = r
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| FfqError::Execution("AND/OR expects boolean".to_string()))?;

        let out = match self.op {
            BoolOp::And => and_kleene(lb, rb),
            BoolOp::Or => or_kleene(lb, rb),
        }
        .map_err(|e| FfqError::Execution(format!("boolean kernel failed: {e}")))?;

        Ok(Arc::new(out))
    }
}

struct BinaryExpr {
    left: Arc<dyn PhysicalExpr>,
    right: Arc<dyn PhysicalExpr>,
    op: BinaryOp,
    out: DataType,
}

struct ScalarUdfExpr {
    udf_name: String,
    udf: Arc<dyn crate::udf::ScalarUdf>,
    args: Vec<Arc<dyn PhysicalExpr>>,
    out: DataType,
}

impl PhysicalExpr for ScalarUdfExpr {
    fn data_type(&self) -> DataType {
        self.out.clone()
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let arrays = self
            .args
            .iter()
            .map(|arg| arg.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;
        self.udf
            .invoke(&arrays)
            .map_err(|e| FfqError::Execution(format!("scalar udf '{}' failed: {e}", self.udf_name)))
    }
}

impl PhysicalExpr for BinaryExpr {
    fn data_type(&self) -> DataType {
        self.out.clone()
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let l = self.left.evaluate(batch)?;
        let r = self.right.evaluate(batch)?;

        match self.op {
            // arithmetic
            BinaryOp::Plus | BinaryOp::Minus | BinaryOp::Multiply | BinaryOp::Divide => {
                eval_arith(self.op, &l, &r, &self.out)
            }
            // comparisons
            BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq => eval_cmp(self.op, &l, &r),
        }
    }
}

// ------------------ helpers ------------------

fn literal_type(v: &LiteralValue) -> DataType {
    match v {
        LiteralValue::Int64(_) => DataType::Int64,
        LiteralValue::Float64(_) => DataType::Float64,
        LiteralValue::Utf8(_) => DataType::Utf8,
        LiteralValue::Boolean(_) => DataType::Boolean,
        LiteralValue::Null => DataType::Null,
        #[cfg(feature = "vector")]
        LiteralValue::VectorF32(_) => DataType::Null, // only used as vector query literal in v1
    }
}

fn scalar_to_array(v: &LiteralValue, len: usize) -> Result<ArrayRef> {
    match v {
        LiteralValue::Int64(x) => {
            let mut b = Int64Builder::with_capacity(len);
            for _ in 0..len {
                b.append_value(*x);
            }
            Ok(Arc::new(b.finish()))
        }
        LiteralValue::Float64(x) => {
            let mut b = Float64Builder::with_capacity(len);
            for _ in 0..len {
                b.append_value(*x);
            }
            Ok(Arc::new(b.finish()))
        }
        LiteralValue::Boolean(x) => {
            let mut b = BooleanBuilder::with_capacity(len);
            for _ in 0..len {
                b.append_value(*x);
            }
            Ok(Arc::new(b.finish()))
        }
        LiteralValue::Utf8(s) => {
            let mut b = StringBuilder::with_capacity(len, s.len() * len);
            for _ in 0..len {
                b.append_value(s);
            }
            Ok(Arc::new(b.finish()))
        }
        LiteralValue::Null => Ok(arrow::array::new_null_array(&DataType::Null, len)),
        #[cfg(feature = "vector")]
        LiteralValue::VectorF32(_) => Err(FfqError::Unsupported(
            "Vector literal can only be used as query literal for vector functions in v1"
                .to_string(),
        )),
    }
}

fn binary_out_type(op: BinaryOp, l: DataType, r: DataType) -> Result<DataType> {
    match op {
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::LtEq
        | BinaryOp::Gt
        | BinaryOp::GtEq => Ok(DataType::Boolean),

        BinaryOp::Plus | BinaryOp::Minus | BinaryOp::Multiply | BinaryOp::Divide => {
            if l != r {
                return Err(FfqError::Planning(format!(
                    "execution expects casts inserted by analyzer; got {l:?} vs {r:?}"
                )));
            }
            Ok(l)
        }
    }
}

fn eval_arith(op: BinaryOp, l: &ArrayRef, r: &ArrayRef, out: &DataType) -> Result<ArrayRef> {
    match out {
        DataType::Int64 => {
            let la = l
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| FfqError::Execution("expected Int64 array".to_string()))?;
            let ra = r
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| FfqError::Execution("expected Int64 array".to_string()))?;

            let res = match op {
                BinaryOp::Plus => add(la, ra),
                BinaryOp::Minus => sub(la, ra),
                BinaryOp::Multiply => mul(la, ra),
                BinaryOp::Divide => div(la, ra),
                _ => unreachable!(),
            }
            .map_err(|e| FfqError::Execution(format!("arith kernel failed: {e}")))?;

            Ok(res)
        }

        DataType::Float64 => {
            let la = l
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| FfqError::Execution("expected Float64 array".to_string()))?;
            let ra = r
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| FfqError::Execution("expected Float64 array".to_string()))?;

            let res = match op {
                BinaryOp::Plus => add(la, ra),
                BinaryOp::Minus => sub(la, ra),
                BinaryOp::Multiply => mul(la, ra),
                BinaryOp::Divide => div(la, ra),
                _ => unreachable!(),
            }
            .map_err(|e| FfqError::Execution(format!("arith kernel failed: {e}")))?;

            Ok(res)
        }

        _ => Err(FfqError::Unsupported(format!(
            "arith not supported for type {out:?} in v1"
        ))),
    }
}

fn eval_cmp(op: BinaryOp, l: &ArrayRef, r: &ArrayRef) -> Result<ArrayRef> {
    match l.data_type() {
        DataType::Int64 => {
            let la = l.as_any().downcast_ref::<Int64Array>().unwrap();
            let ra = r.as_any().downcast_ref::<Int64Array>().unwrap();
            let res = match op {
                BinaryOp::Eq => eq(la, ra),
                BinaryOp::NotEq => neq(la, ra),
                BinaryOp::Lt => lt(la, ra),
                BinaryOp::LtEq => lt_eq(la, ra),
                BinaryOp::Gt => gt(la, ra),
                BinaryOp::GtEq => gt_eq(la, ra),
                _ => unreachable!(),
            }
            .map_err(|e| FfqError::Execution(format!("cmp kernel failed: {e}")))?;
            Ok(Arc::new(res))
        }

        DataType::Float64 => {
            let la = l.as_any().downcast_ref::<Float64Array>().unwrap();
            let ra = r.as_any().downcast_ref::<Float64Array>().unwrap();
            let res = match op {
                BinaryOp::Eq => eq(la, ra),
                BinaryOp::NotEq => neq(la, ra),
                BinaryOp::Lt => lt(la, ra),
                BinaryOp::LtEq => lt_eq(la, ra),
                BinaryOp::Gt => gt(la, ra),
                BinaryOp::GtEq => gt_eq(la, ra),
                _ => unreachable!(),
            }
            .map_err(|e| FfqError::Execution(format!("cmp kernel failed: {e}")))?;
            Ok(Arc::new(res))
        }

        DataType::Utf8 => {
            let la = l.as_any().downcast_ref::<StringArray>().unwrap();
            let ra = r.as_any().downcast_ref::<StringArray>().unwrap();
            let res = match op {
                BinaryOp::Eq => eq(la, ra),
                BinaryOp::NotEq => neq(la, ra),
                BinaryOp::Lt => lt(la, ra),
                BinaryOp::LtEq => lt_eq(la, ra),
                BinaryOp::Gt => gt(la, ra),
                BinaryOp::GtEq => gt_eq(la, ra),
                _ => unreachable!(),
            }
            .map_err(|e| FfqError::Execution(format!("cmp kernel failed: {e}")))?;
            Ok(Arc::new(res))
        }

        DataType::Boolean => {
            let la = l.as_any().downcast_ref::<BooleanArray>().unwrap();
            let ra = r.as_any().downcast_ref::<BooleanArray>().unwrap();
            let res = match op {
                BinaryOp::Eq => eq(la, ra),
                BinaryOp::NotEq => neq(la, ra),
                _ => {
                    return Err(FfqError::Unsupported(
                        "ordering comparisons not supported for boolean in v1".to_string(),
                    ));
                }
            }
            .map_err(|e| FfqError::Execution(format!("cmp kernel failed: {e}")))?;
            Ok(Arc::new(res))
        }

        other => Err(FfqError::Unsupported(format!(
            "comparison not supported for {other:?} in v1"
        ))),
    }
}

// =====================
// Vector expressions
// =====================

#[cfg(feature = "vector")]
#[derive(Clone, Copy)]
enum VectorKind {
    Cosine,
    L2,
    Dot,
}

#[cfg(feature = "vector")]
struct VectorExpr {
    vector: Arc<dyn PhysicalExpr>,
    query: Vec<f32>,
    kind: VectorKind,
}

#[cfg(feature = "vector")]
impl PhysicalExpr for VectorExpr {
    fn data_type(&self) -> DataType {
        DataType::Float32
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        use arrow::array::{FixedSizeListArray, Float32Array, Float32Builder};

        let arr = self.vector.evaluate(batch)?;
        let list = arr
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| {
                FfqError::Execution("vector column must be FixedSizeList".to_string())
            })?;

        let dim = list.value_length() as usize;
        if self.query.len() != dim {
            return Err(FfqError::Execution(format!(
                "query vector dim {} != column dim {}",
                self.query.len(),
                dim
            )));
        }

        let values = list
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| FfqError::Execution("vector values must be Float32".to_string()))?;

        let mut out = Float32Builder::new();

        let qnorm = if matches!(self.kind, VectorKind::Cosine) {
            let s: f32 = self.query.iter().map(|x| x * x).sum();
            s.sqrt()
        } else {
            0.0
        };

        for row in 0..list.len() {
            if list.is_null(row) {
                out.append_null();
                continue;
            }

            let start = row * dim;
            let mut dot = 0.0f32;
            let mut vnorm2 = 0.0f32;
            let mut l2_2 = 0.0f32;

            for j in 0..dim {
                let v = values.value(start + j);
                let q = self.query[j];

                dot += v * q;
                vnorm2 += v * v;
                let d = v - q;
                l2_2 += d * d;
            }

            let score = match self.kind {
                VectorKind::Dot => dot,
                VectorKind::L2 => l2_2.sqrt(),
                VectorKind::Cosine => {
                    let vnorm = vnorm2.sqrt();
                    if vnorm == 0.0 || qnorm == 0.0 {
                        0.0
                    } else {
                        dot / (vnorm * qnorm)
                    }
                }
            };

            out.append_value(score);
        }

        Ok(Arc::new(out.finish()))
    }
}

#[cfg(feature = "vector")]
fn extract_vector_literal(e: &Expr) -> Result<Vec<f32>> {
    match e {
        Expr::Literal(LiteralValue::VectorF32(v)) => Ok(v.clone()),
        _ => Err(FfqError::Unsupported(
            "v1 vector functions require query to be a vector literal param (LiteralValue::VectorF32)"
                .to_string(),
        )),
    }
}

#[cfg(all(test, feature = "vector"))]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, FixedSizeListBuilder, Float32Array, Float32Builder, Int64Array};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};

    use super::compile_expr;
    use ffq_planner::{Expr, LiteralValue};

    fn vector_schema(dim: i32) -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "emb",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
            true,
        )]))
    }

    fn vector_batch_3(rows: &[([f32; 3], bool)]) -> RecordBatch {
        let mut list_builder = FixedSizeListBuilder::new(Float32Builder::new(), 3);
        for (v, valid) in rows {
            list_builder.values().append_value(v[0]);
            list_builder.values().append_value(v[1]);
            list_builder.values().append_value(v[2]);
            list_builder.append(*valid);
        }
        let emb = Arc::new(list_builder.finish());
        let schema = vector_schema(3);
        RecordBatch::try_new(schema, vec![emb]).expect("batch")
    }

    fn eval(expr: Expr, batch: &RecordBatch) -> Float32Array {
        let compiled = compile_expr(&expr, batch.schema_ref()).expect("compile");
        let out = compiled.evaluate(batch).expect("evaluate");
        out.as_any()
            .downcast_ref::<Float32Array>()
            .expect("float32 out")
            .clone()
    }

    #[test]
    fn cosine_similarity_numeric_and_edge_cases() {
        let batch = vector_batch_3(&[
            ([1.0, 0.0, 0.0], true),
            ([0.0, 1.0, 0.0], true),
            ([-1.0, 0.0, 0.0], true),
            ([0.0, 0.0, 0.0], true), // zero vector row
            ([0.0, 0.0, 0.0], false),
        ]);

        let expr = Expr::CosineSimilarity {
            vector: Box::new(Expr::Column("emb".to_string())),
            query: Box::new(Expr::Literal(LiteralValue::VectorF32(vec![1.0, 0.0, 0.0]))),
        };
        let out = eval(expr, &batch);

        assert!((out.value(0) - 1.0).abs() < 1e-6);
        assert!((out.value(1) - 0.0).abs() < 1e-6);
        assert!((out.value(2) - (-1.0)).abs() < 1e-6);
        assert!((out.value(3) - 0.0).abs() < 1e-6);
        assert!(out.is_null(4));
    }

    #[test]
    fn l2_distance_numeric_and_negative_values() {
        let batch = vector_batch_3(&[([1.0, 2.0, 3.0], true), ([-1.0, 0.0, 0.0], true)]);
        let expr = Expr::L2Distance {
            vector: Box::new(Expr::Column("emb".to_string())),
            query: Box::new(Expr::Literal(LiteralValue::VectorF32(vec![1.0, 0.0, 0.0]))),
        };
        let out = eval(expr, &batch);
        // sqrt((1-1)^2 + (2-0)^2 + (3-0)^2) = sqrt(13)
        assert!((out.value(0) - 13.0_f32.sqrt()).abs() < 1e-6);
        // sqrt((-1-1)^2) = 2
        assert!((out.value(1) - 2.0).abs() < 1e-6);
    }

    #[test]
    fn dot_product_numeric_and_nulls() {
        let batch = vector_batch_3(&[
            ([1.0, 2.0, 3.0], true),
            ([-1.0, 0.5, 0.0], true),
            ([0.0, 0.0, 0.0], false),
        ]);
        let expr = Expr::DotProduct {
            vector: Box::new(Expr::Column("emb".to_string())),
            query: Box::new(Expr::Literal(LiteralValue::VectorF32(vec![1.0, -1.0, 0.5]))),
        };
        let out = eval(expr, &batch);
        // 1*1 + 2*(-1) + 3*0.5 = 0.5
        assert!((out.value(0) - 0.5).abs() < 1e-6);
        // -1*1 + 0.5*(-1) + 0*0.5 = -1.5
        assert!((out.value(1) - (-1.5)).abs() < 1e-6);
        assert!(out.is_null(2));
    }

    #[test]
    fn vector_dim_mismatch_returns_clear_error() {
        let batch = vector_batch_3(&[([1.0, 0.0, 0.0], true)]);
        let expr = Expr::CosineSimilarity {
            vector: Box::new(Expr::Column("emb".to_string())),
            query: Box::new(Expr::Literal(LiteralValue::VectorF32(vec![1.0, 0.0]))),
        };
        let compiled = compile_expr(&expr, batch.schema_ref()).expect("compile");
        let err = compiled.evaluate(&batch).expect_err("dim mismatch");
        let msg = err.to_string();
        assert!(
            msg.contains("query vector dim 2 != column dim 3"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn non_fixed_size_list_vector_column_returns_clear_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("emb", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
        )
        .expect("batch");

        let expr = Expr::DotProduct {
            vector: Box::new(Expr::Column("emb".to_string())),
            query: Box::new(Expr::Literal(LiteralValue::VectorF32(vec![1.0, 0.0, 0.0]))),
        };
        let compiled = compile_expr(&expr, &schema).expect("compile");
        let err = compiled.evaluate(&batch).expect_err("type mismatch");
        let msg = err.to_string();
        assert!(
            msg.contains("vector column must be FixedSizeList"),
            "unexpected error: {msg}"
        );
    }
}
