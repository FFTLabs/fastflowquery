use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    pub batch_size_rows: usize,
    pub mem_budget_bytes: usize,

    pub shuffle_partitions: usize,
    pub broadcast_threshold_bytes: u64,

    pub spill_dir: String,
    pub catalog_path: Option<String>,
    pub coordinator_endpoint: Option<String>,
    #[serde(default = "default_infer_on_register")]
    pub infer_on_register: bool,
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
            infer_on_register: default_infer_on_register(),
        }
    }
}

const fn default_infer_on_register() -> bool {
    true
}
