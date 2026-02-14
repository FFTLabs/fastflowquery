use serde::{Deserialize, Serialize};

pub fn shuffle_path(
    query_id: u64,
    stage_id: u64,
    map_task: u64,
    attempt: u32,
    reduce_partition: u32,
) -> String {
    format!("shuffle/{query_id}/{stage_id}/{map_task}/{attempt}/part-{reduce_partition}.ipc")
}

pub fn map_task_dir(query_id: u64, stage_id: u64, map_task: u64, attempt: u32) -> String {
    format!("shuffle/{query_id}/{stage_id}/{map_task}/{attempt}")
}

pub fn index_json_path(query_id: u64, stage_id: u64, map_task: u64, attempt: u32) -> String {
    format!(
        "{}/index.json",
        map_task_dir(query_id, stage_id, map_task, attempt)
    )
}

pub fn index_bin_path(query_id: u64, stage_id: u64, map_task: u64, attempt: u32) -> String {
    format!(
        "{}/index.bin",
        map_task_dir(query_id, stage_id, map_task, attempt)
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShufflePartitionMeta {
    pub reduce_partition: u32,
    pub file: String,
    pub bytes: u64,
    pub rows: u64,
    pub batches: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapTaskIndex {
    pub query_id: u64,
    pub stage_id: u64,
    pub map_task: u64,
    pub attempt: u32,
    pub partitions: Vec<ShufflePartitionMeta>,
}
