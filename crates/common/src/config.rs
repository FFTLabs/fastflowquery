use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SchemaInferencePolicy {
    Off,
    On,
    Strict,
    Permissive,
}

impl Default for SchemaInferencePolicy {
    fn default() -> Self {
        Self::On
    }
}

impl SchemaInferencePolicy {
    pub fn allows_inference(self) -> bool {
        !matches!(self, Self::Off)
    }

    pub fn is_permissive_merge(self) -> bool {
        matches!(self, Self::On | Self::Permissive)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SchemaDriftPolicy {
    Fail,
    Refresh,
}

impl Default for SchemaDriftPolicy {
    fn default() -> Self {
        Self::Refresh
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    pub batch_size_rows: usize,
    pub mem_budget_bytes: usize,

    pub shuffle_partitions: usize,
    pub broadcast_threshold_bytes: u64,

    pub spill_dir: String,
    pub catalog_path: Option<String>,
    pub coordinator_endpoint: Option<String>,
    #[serde(default)]
    pub schema_inference: SchemaInferencePolicy,
    #[serde(default)]
    pub schema_drift_policy: SchemaDriftPolicy,
    #[serde(default)]
    pub schema_writeback: bool,
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
        }
    }
}
