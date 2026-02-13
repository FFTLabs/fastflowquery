use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    pub batch_size_rows: usize,
    pub mem_budget_bytes: usize,
    pub spill_dir: String,
    pub shuffle_partitions: usize,
    pub broadcast_threshold_bytes: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            batch_size_rows: 8192,
            mem_budget_bytes: 512 * 1024 * 1024,
            spill_dir: ".ffq_spill".to_string(),
            shuffle_partitions: 64,
            broadcast_threshold_bytes: 64 * 1024 * 1024,
        }
    }
}
