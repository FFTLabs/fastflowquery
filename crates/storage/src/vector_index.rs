use futures::future::BoxFuture;

use ffq_common::Result;

/// One vector top-k result row returned by index providers.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorTopKRow {
    /// Document identifier.
    pub id: i64,
    /// Similarity/distance score as returned by provider.
    pub score: f32,
    /// Optional payload serialized as JSON text.
    pub payload_json: Option<String>,
}

/// Query-time knobs for vector index providers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VectorQueryOptions {
    /// Optional query-time metric override (`cosine`, `dot`, `l2`).
    pub metric: Option<String>,
    /// Optional query-time HNSW `ef_search` override.
    pub ef_search: Option<usize>,
}

/// Vector index abstraction used by `VectorTopKExec`.
pub trait VectorIndexProvider: Send + Sync {
    /// Fetch top-k rows for `query_vec`, optionally applying provider-specific filter.
    fn topk<'a>(
        &'a self,
        query_vec: Vec<f32>,
        k: usize,
        filter: Option<String>,
        options: VectorQueryOptions,
    ) -> BoxFuture<'a, Result<Vec<VectorTopKRow>>>;
}
