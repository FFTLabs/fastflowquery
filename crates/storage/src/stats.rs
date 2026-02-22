use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Lightweight table statistics used by optimizer heuristics.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct TableStats {
    /// Estimated row count if known.
    pub rows: Option<u64>,
    /// Estimated bytes if known.
    pub bytes: Option<u64>,
}

/// Scalar min/max value representation for persisted file statistics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value")]
pub enum ScalarStatValue {
    /// 64-bit signed integer.
    Int64(i64),
    /// 64-bit floating value.
    Float64(f64),
    /// Boolean value.
    Bool(bool),
    /// UTF-8 text value.
    Utf8(String),
}

/// Min/max range for one column in a parquet file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnRangeStats {
    /// Column minimum value.
    pub min: ScalarStatValue,
    /// Column maximum value.
    pub max: ScalarStatValue,
}

/// Persistable per-file parquet statistics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParquetFileStats {
    /// Source file path.
    pub path: String,
    /// File size in bytes.
    pub size_bytes: u64,
    /// Total row count from parquet metadata.
    pub row_count: u64,
    /// Per-column min/max when available.
    #[serde(default)]
    pub column_ranges: HashMap<String, ColumnRangeStats>,
}
