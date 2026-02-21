use serde::{Deserialize, Serialize};

/// Build relative path to one shuffle partition IPC file.
pub fn shuffle_path(
    query_id: u64,
    stage_id: u64,
    map_task: u64,
    attempt: u32,
    reduce_partition: u32,
) -> String {
    format!("shuffle/{query_id}/{stage_id}/{map_task}/{attempt}/part-{reduce_partition}.ipc")
}

/// Build relative directory path for one map-task attempt.
pub fn map_task_dir(query_id: u64, stage_id: u64, map_task: u64, attempt: u32) -> String {
    format!("shuffle/{query_id}/{stage_id}/{map_task}/{attempt}")
}

/// Build relative base directory for a map task containing all attempts.
pub fn map_task_base_dir(query_id: u64, stage_id: u64, map_task: u64) -> String {
    format!("shuffle/{query_id}/{stage_id}/{map_task}")
}

/// Build relative path to JSON index metadata for one map-task attempt.
pub fn index_json_path(query_id: u64, stage_id: u64, map_task: u64, attempt: u32) -> String {
    format!(
        "{}/index.json",
        map_task_dir(query_id, stage_id, map_task, attempt)
    )
}

/// Build relative path to binary index metadata for one map-task attempt.
pub fn index_bin_path(query_id: u64, stage_id: u64, map_task: u64, attempt: u32) -> String {
    format!(
        "{}/index.bin",
        map_task_dir(query_id, stage_id, map_task, attempt)
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
/// Compression codec for on-disk shuffle partition payloads.
pub enum ShuffleCompressionCodec {
    /// Store payload as raw Arrow IPC stream bytes.
    #[default]
    None,
    /// Store payload as LZ4 frame-compressed bytes.
    Lz4,
    /// Store payload as Zstd-compressed bytes.
    Zstd,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Metadata describing one map-output partition artifact.
pub struct ShufflePartitionMeta {
    /// Reduce partition id.
    pub reduce_partition: u32,
    /// Relative file path of partition payload.
    pub file: String,
    /// Payload size in bytes.
    pub bytes: u64,
    /// Compressed payload bytes (excluding framing header).
    #[serde(default)]
    pub compressed_bytes: u64,
    /// Uncompressed Arrow IPC payload bytes.
    #[serde(default)]
    pub uncompressed_bytes: u64,
    /// Compression codec used for this partition payload.
    #[serde(default)]
    pub codec: ShuffleCompressionCodec,
    /// Row count in payload.
    pub rows: u64,
    /// Batch count in payload.
    pub batches: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Per-attempt index metadata describing all produced partitions.
pub struct MapTaskIndex {
    /// Query id.
    pub query_id: u64,
    /// Stage id.
    pub stage_id: u64,
    /// Map task id.
    pub map_task: u64,
    /// Attempt number.
    pub attempt: u32,
    /// Creation time in unix milliseconds.
    #[serde(default)]
    pub created_at_ms: u64,
    /// Partition metadata entries for this attempt.
    pub partitions: Vec<ShufflePartitionMeta>,
}
