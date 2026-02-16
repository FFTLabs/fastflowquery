use std::fs;
use std::path::{Path, PathBuf};

use ffq_common::{FfqError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BenchmarkQueryId {
    TpchQ1,
    TpchQ3,
    RagTopkBruteforce,
    RagTopkQdrant,
}

impl BenchmarkQueryId {
    pub fn stable_id(self) -> &'static str {
        match self {
            Self::TpchQ1 => "tpch_q1",
            Self::TpchQ3 => "tpch_q3",
            Self::RagTopkBruteforce => "rag_topk_bruteforce",
            Self::RagTopkQdrant => "rag_topk_qdrant",
        }
    }

    pub fn file_name(self) -> &'static str {
        match self {
            Self::TpchQ1 => "canonical/tpch_q1.sql",
            Self::TpchQ3 => "canonical/tpch_q3.sql",
            Self::RagTopkBruteforce => "rag_topk_bruteforce.sql",
            Self::RagTopkQdrant => "rag_topk_qdrant.sql",
        }
    }
}

pub const CANONICAL_BENCHMARK_QUERIES: [BenchmarkQueryId; 4] = [
    BenchmarkQueryId::TpchQ1,
    BenchmarkQueryId::TpchQ3,
    BenchmarkQueryId::RagTopkBruteforce,
    BenchmarkQueryId::RagTopkQdrant,
];

pub fn default_benchmark_query_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/bench/queries")
        .to_path_buf()
}

pub fn load_benchmark_query(id: BenchmarkQueryId) -> Result<String> {
    load_benchmark_query_from_root(&default_benchmark_query_root(), id)
}

pub fn load_benchmark_query_from_root(root: &Path, id: BenchmarkQueryId) -> Result<String> {
    let path = root.join(id.file_name());
    let query = fs::read_to_string(&path).map_err(|e| {
        FfqError::Io(std::io::Error::new(
            e.kind(),
            format!(
                "failed reading benchmark query '{}' at {}: {e}",
                id.stable_id(),
                path.display()
            ),
        ))
    })?;
    let trimmed = query.trim().to_string();
    if trimmed.is_empty() {
        return Err(FfqError::InvalidConfig(format!(
            "benchmark query '{}' at {} is empty",
            id.stable_id(),
            path.display()
        )));
    }
    Ok(trimmed)
}

pub fn load_all_benchmark_queries() -> Result<Vec<(BenchmarkQueryId, String)>> {
    let root = default_benchmark_query_root();
    CANONICAL_BENCHMARK_QUERIES
        .iter()
        .copied()
        .map(|id| load_benchmark_query_from_root(&root, id).map(|sql| (id, sql)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ffq_sql::parse_sql;

    #[test]
    fn loads_and_parses_all_canonical_benchmark_queries() {
        let queries = load_all_benchmark_queries().expect("load benchmark queries");
        assert_eq!(queries.len(), CANONICAL_BENCHMARK_QUERIES.len());
        for (id, sql) in queries {
            assert!(
                !sql.is_empty(),
                "query {} must not be empty",
                id.stable_id()
            );
            parse_sql(&sql).unwrap_or_else(|e| panic!("{} failed to parse: {e}", id.stable_id()));
        }
    }
}
