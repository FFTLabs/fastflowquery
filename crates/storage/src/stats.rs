use serde::{Deserialize, Serialize};

/// Lightweight table statistics used by optimizer heuristics.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct TableStats {
    /// Estimated row count if known.
    pub rows: Option<u64>,
    /// Estimated bytes if known.
    pub bytes: Option<u64>,
}
