use arrow::array::ArrayRef;

#[cfg(feature = "vector")]
use arrow::array::{Array, FixedSizeListArray, Float32Array};

use arrow::compute::kernels::cast;
use arrow::datatypes::DataType;
use ffq_common::{FfqError, Result};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum PhysicalExpr {
    Column(usize),
    Literal(ArrayRef),
    Cast {
        expr: Box<PhysicalExpr>,
        to_type: DataType,
    },

    #[cfg(feature = "vector")]
    CosineSimilarity {
        vector_col: Box<PhysicalExpr>,
        query_vec: Box<PhysicalExpr>,
    },
}

impl PhysicalExpr {
    pub fn evaluate(&self, _batch: &arrow::record_batch::RecordBatch) -> Result<ArrayRef> {
        match self {
            PhysicalExpr::Literal(v) => Ok(Arc::clone(v)),
            PhysicalExpr::Cast { expr, to_type } => {
                let arr = expr.evaluate(_batch)?;
                cast(&arr, to_type).map_err(|e| FfqError::Execution(e.to_string()))
            }
            _ => Err(FfqError::Unsupported("expression not implemented".to_string())),
        }
    }
}

#[cfg(feature = "vector")]
pub fn cosine_similarity_fixed_size_list_f32(
    vectors: &FixedSizeListArray,
    _query: &Float32Array,
) -> Result<Float32Array> {
    let len = vectors.len();
    Ok(Float32Array::from(vec![0.0_f32; len]))
}
