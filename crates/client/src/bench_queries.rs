use std::fs;
use std::path::{Path, PathBuf};

use ffq_common::{FfqError, Result};

/// Identifier for canonical benchmark SQL files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BenchmarkQueryId {
    /// TPC-H Q1 aggregate workload.
    TpchQ1,
    /// TPC-H Q3 join + filter workload.
    TpchQ3,
    /// Vector brute-force top-k benchmark query.
    RagTopkBruteforce,
    /// Optional qdrant-backed vector top-k benchmark query.
    RagTopkQdrant,
    /// Window benchmark with narrow partitions.
    WindowNarrowPartitions,
    /// Window benchmark with wide partitions.
    WindowWidePartitions,
    /// Window benchmark with skewed partition keys.
    WindowSkewedKeys,
    /// Window benchmark with many window expressions sharing a sort.
    WindowManyExpressions,
    /// Adaptive-shuffle benchmark with many tiny reduce groups.
    AdaptiveShuffleTinyPartitions,
    /// Adaptive-shuffle benchmark with large/coalescable reduce groups.
    AdaptiveShuffleLargePartitions,
    /// Adaptive-shuffle benchmark with skewed partition key distribution.
    AdaptiveShuffleSkewedKeys,
    /// Adaptive-shuffle mixed workload benchmark (join + aggregate).
    AdaptiveShuffleMixedWorkload,
}

impl BenchmarkQueryId {
    /// Stable machine-readable identifier used in result artifacts.
    pub fn stable_id(self) -> &'static str {
        match self {
            Self::TpchQ1 => "tpch_q1",
            Self::TpchQ3 => "tpch_q3",
            Self::RagTopkBruteforce => "rag_topk_bruteforce",
            Self::RagTopkQdrant => "rag_topk_qdrant",
            Self::WindowNarrowPartitions => "window_narrow_partitions",
            Self::WindowWidePartitions => "window_wide_partitions",
            Self::WindowSkewedKeys => "window_skewed_keys",
            Self::WindowManyExpressions => "window_many_expressions",
            Self::AdaptiveShuffleTinyPartitions => "adaptive_shuffle_tiny_partitions",
            Self::AdaptiveShuffleLargePartitions => "adaptive_shuffle_large_partitions",
            Self::AdaptiveShuffleSkewedKeys => "adaptive_shuffle_skewed_keys",
            Self::AdaptiveShuffleMixedWorkload => "adaptive_shuffle_mixed_workload",
        }
    }

    /// Relative SQL file location under the benchmark query root.
    pub fn file_name(self) -> &'static str {
        match self {
            Self::TpchQ1 => "canonical/tpch_q1.sql",
            Self::TpchQ3 => "canonical/tpch_q3.sql",
            Self::RagTopkBruteforce => "rag_topk_bruteforce.sql",
            Self::RagTopkQdrant => "rag_topk_qdrant.sql",
            Self::WindowNarrowPartitions => "window/window_narrow_partitions.sql",
            Self::WindowWidePartitions => "window/window_wide_partitions.sql",
            Self::WindowSkewedKeys => "window/window_skewed_keys.sql",
            Self::WindowManyExpressions => "window/window_many_expressions.sql",
            Self::AdaptiveShuffleTinyPartitions => "adaptive/adaptive_shuffle_tiny_partitions.sql",
            Self::AdaptiveShuffleLargePartitions => {
                "adaptive/adaptive_shuffle_large_partitions.sql"
            }
            Self::AdaptiveShuffleSkewedKeys => "adaptive/adaptive_shuffle_skewed_keys.sql",
            Self::AdaptiveShuffleMixedWorkload => "adaptive/adaptive_shuffle_mixed_workload.sql",
        }
    }
}

/// Ordered list of benchmark queries expected by the benchmark runner.
pub const CANONICAL_BENCHMARK_QUERIES: [BenchmarkQueryId; 12] = [
    BenchmarkQueryId::TpchQ1,
    BenchmarkQueryId::TpchQ3,
    BenchmarkQueryId::RagTopkBruteforce,
    BenchmarkQueryId::RagTopkQdrant,
    BenchmarkQueryId::WindowNarrowPartitions,
    BenchmarkQueryId::WindowWidePartitions,
    BenchmarkQueryId::WindowSkewedKeys,
    BenchmarkQueryId::WindowManyExpressions,
    BenchmarkQueryId::AdaptiveShuffleTinyPartitions,
    BenchmarkQueryId::AdaptiveShuffleLargePartitions,
    BenchmarkQueryId::AdaptiveShuffleSkewedKeys,
    BenchmarkQueryId::AdaptiveShuffleMixedWorkload,
];

/// Returns the default benchmark query directory.
pub fn default_benchmark_query_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/bench/queries")
        .to_path_buf()
}

/// Loads one benchmark SQL file from the default query root.
///
/// # Errors
/// Returns an error when file loading fails or query content is empty.
pub fn load_benchmark_query(id: BenchmarkQueryId) -> Result<String> {
    load_benchmark_query_from_root(&default_benchmark_query_root(), id)
}

/// Loads one benchmark SQL file from an explicit query root.
///
/// # Errors
/// Returns an error when file loading fails or query content is empty.
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

/// Loads all canonical benchmark SQL files.
///
/// # Errors
/// Returns an error if any canonical query fails to load.
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
