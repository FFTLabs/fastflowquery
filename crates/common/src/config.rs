use serde::{Deserialize, Serialize};

/// Schema inference policy for parquet-like tables with optional schema.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SchemaInferencePolicy {
    /// Never infer schema; explicit schema is required.
    Off,
    /// Infer schema when missing; fail on incompatible multi-file schema merges.
    On,
    /// Infer schema with strict compatibility checks.
    Strict,
    /// Infer schema with permissive numeric widening where supported.
    Permissive,
}

impl Default for SchemaInferencePolicy {
    fn default() -> Self {
        Self::On
    }
}

impl SchemaInferencePolicy {
    /// Returns whether schema inference is permitted.
    pub fn allows_inference(self) -> bool {
        !matches!(self, Self::Off)
    }

    /// Returns whether permissive schema merge behavior is enabled.
    pub fn is_permissive_merge(self) -> bool {
        matches!(self, Self::On | Self::Permissive)
    }
}

/// Behavior when observed file fingerprint no longer matches cached schema metadata.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SchemaDriftPolicy {
    /// Fail query/registration when schema drift is detected.
    Fail,
    /// Refresh inferred schema from current files when drift is detected.
    Refresh,
}

impl Default for SchemaDriftPolicy {
    fn default() -> Self {
        Self::Refresh
    }
}

/// Global engine/session configuration shared across planner/runtime layers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Target rows per output/input batch for operators.
    pub batch_size_rows: usize,
    /// Soft per-task memory budget used by spill decisions.
    pub mem_budget_bytes: usize,

    /// Shuffle partition count used by distributed and physical planning paths.
    pub shuffle_partitions: usize,
    /// Broadcast join threshold in bytes for optimizer join hinting.
    pub broadcast_threshold_bytes: u64,

    /// Directory used for spill files.
    pub spill_dir: String,
    /// Optional catalog file path (`.json` or `.toml`).
    pub catalog_path: Option<String>,
    /// Optional distributed coordinator endpoint (for example `http://127.0.0.1:50051`).
    pub coordinator_endpoint: Option<String>,
    /// Schema inference policy.
    #[serde(default)]
    pub schema_inference: SchemaInferencePolicy,
    /// Schema drift policy.
    #[serde(default)]
    pub schema_drift_policy: SchemaDriftPolicy,
    /// Whether inferred schema/fingerprint metadata should be persisted back to catalog.
    #[serde(default)]
    pub schema_writeback: bool,
    /// Maximum recursive expansion depth for `WITH RECURSIVE` planning.
    #[serde(default = "default_recursive_cte_max_depth")]
    pub recursive_cte_max_depth: usize,
}

fn default_recursive_cte_max_depth() -> usize {
    32
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            batch_size_rows: 8192,
            mem_budget_bytes: 512 * 1024 * 1024, // 512MB
            shuffle_partitions: 64,
            broadcast_threshold_bytes: 64 * 1024 * 1024, // 64MB
            spill_dir: "./ffq_spill".to_string(),
            catalog_path: None,
            coordinator_endpoint: None,
            schema_inference: SchemaInferencePolicy::default(),
            schema_drift_policy: SchemaDriftPolicy::default(),
            schema_writeback: false,
            recursive_cte_max_depth: default_recursive_cte_max_depth(),
        }
    }
}
