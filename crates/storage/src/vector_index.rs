use futures::future::BoxFuture;

use ffq_common::Result;

#[derive(Debug, Clone, PartialEq)]
pub struct VectorTopKRow {
    pub id: i64,
    pub score: f32,
    pub payload_json: Option<String>,
}

pub trait VectorIndexProvider: Send + Sync {
    fn topk<'a>(
        &'a self,
        query_vec: Vec<f32>,
        k: usize,
        filter: Option<String>,
    ) -> BoxFuture<'a, Result<Vec<VectorTopKRow>>>;
}
