use std::fs;
use std::io::Cursor;
use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use ffq_common::{FfqError, Result};

use crate::layout::{
    MapTaskIndex, ShufflePartitionMeta, index_bin_path, index_json_path, map_task_base_dir,
    shuffle_path,
};

const INDEX_BIN_MAGIC: &[u8; 4] = b"FFQI";
const INDEX_BIN_HEADER_LEN: usize = 12;

/// Reads shuffle partitions and index metadata from local storage.
pub struct ShuffleReader {
    root_dir: PathBuf,
    fetch_chunk_bytes: usize,
}

impl ShuffleReader {
    /// Create a reader rooted at `root_dir`.
    pub fn new(root_dir: impl Into<PathBuf>) -> Self {
        Self {
            root_dir: root_dir.into(),
            fetch_chunk_bytes: 64 * 1024,
        }
    }

    /// Configure maximum chunk size used by streamed partition fetch simulation.
    pub fn with_fetch_chunk_bytes(mut self, bytes: usize) -> Self {
        self.fetch_chunk_bytes = bytes.max(1);
        self
    }

    /// Read map-task index metadata, preferring binary index when present.
    pub fn read_map_task_index(
        &self,
        query_id: u64,
        stage_id: u64,
        map_task: u64,
        attempt: u32,
    ) -> Result<MapTaskIndex> {
        let bin = self
            .root_dir
            .join(index_bin_path(query_id, stage_id, map_task, attempt));
        if bin.exists() {
            let bytes = fs::read(bin)?;
            return decode_index_binary(&bytes);
        }

        let json = self
            .root_dir
            .join(index_json_path(query_id, stage_id, map_task, attempt));
        let bytes = fs::read(json)?;
        serde_json::from_slice(&bytes)
            .map_err(|e| FfqError::Execution(format!("index json decode failed: {e}")))
    }

    /// List available attempt ids for a given `(query, stage, map_task)`.
    pub fn available_attempts(
        &self,
        query_id: u64,
        stage_id: u64,
        map_task: u64,
    ) -> Result<Vec<u32>> {
        let base = self
            .root_dir
            .join(map_task_base_dir(query_id, stage_id, map_task));
        if !base.exists() {
            return Ok(Vec::new());
        }

        let mut attempts = fs::read_dir(base)?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let is_dir = e.file_type().ok()?.is_dir();
                if !is_dir {
                    return None;
                }
                e.file_name().to_string_lossy().parse::<u32>().ok()
            })
            .collect::<Vec<_>>();
        attempts.sort_unstable();
        Ok(attempts)
    }

    /// Return highest available attempt id for a map task, if any exists.
    pub fn latest_attempt(
        &self,
        query_id: u64,
        stage_id: u64,
        map_task: u64,
    ) -> Result<Option<u32>> {
        Ok(self
            .available_attempts(query_id, stage_id, map_task)?
            .into_iter()
            .max())
    }

    /// Return partition metadata for one reduce partition in one attempt.
    pub fn partition_meta(
        &self,
        query_id: u64,
        stage_id: u64,
        map_task: u64,
        attempt: u32,
        reduce_partition: u32,
    ) -> Result<ShufflePartitionMeta> {
        let idx = self.read_map_task_index(query_id, stage_id, map_task, attempt)?;
        idx.partitions
            .into_iter()
            .find(|p| p.reduce_partition == reduce_partition)
            .ok_or_else(|| {
                FfqError::Execution(format!(
                    "partition {reduce_partition} not found in shuffle index"
                ))
            })
    }

    /// Read one partition payload and decode as Arrow record batches.
    pub fn read_partition(
        &self,
        query_id: u64,
        stage_id: u64,
        map_task: u64,
        attempt: u32,
        reduce_partition: u32,
    ) -> Result<Vec<RecordBatch>> {
        let rel = shuffle_path(query_id, stage_id, map_task, attempt, reduce_partition);
        let bytes = fs::read(self.root_dir.join(rel))?;
        decode_ipc_bytes(&bytes)
    }

    /// Read partition payload using the newest available attempt.
    pub fn read_partition_latest(
        &self,
        query_id: u64,
        stage_id: u64,
        map_task: u64,
        reduce_partition: u32,
    ) -> Result<(u32, Vec<RecordBatch>)> {
        let attempt = self
            .latest_attempt(query_id, stage_id, map_task)?
            .ok_or_else(|| {
                FfqError::Execution("no shuffle attempts found for map task".to_string())
            })?;
        let batches =
            self.read_partition(query_id, stage_id, map_task, attempt, reduce_partition)?;
        Ok((attempt, batches))
    }

    // Simulates FetchShufflePartition as server-streamed byte chunks.
    /// Read one partition payload and split bytes into fetch-sized chunks.
    pub fn fetch_partition_chunks(
        &self,
        query_id: u64,
        stage_id: u64,
        map_task: u64,
        attempt: u32,
        reduce_partition: u32,
    ) -> Result<Vec<Vec<u8>>> {
        let rel = shuffle_path(query_id, stage_id, map_task, attempt, reduce_partition);
        let bytes = fs::read(self.root_dir.join(rel))?;
        let mut out = Vec::new();
        let mut offset = 0;
        while offset < bytes.len() {
            let end = (offset + self.fetch_chunk_bytes).min(bytes.len());
            out.push(bytes[offset..end].to_vec());
            offset = end;
        }
        Ok(out)
    }

    /// Fetch partition chunks for the newest available attempt.
    pub fn fetch_partition_chunks_latest(
        &self,
        query_id: u64,
        stage_id: u64,
        map_task: u64,
        reduce_partition: u32,
    ) -> Result<(u32, Vec<Vec<u8>>)> {
        let attempt = self
            .latest_attempt(query_id, stage_id, map_task)?
            .ok_or_else(|| {
                FfqError::Execution("no shuffle attempts found for map task".to_string())
            })?;
        let chunks =
            self.fetch_partition_chunks(query_id, stage_id, map_task, attempt, reduce_partition)?;
        Ok((attempt, chunks))
    }

    /// Decode record batches from previously streamed byte chunks.
    pub fn read_partition_from_streamed_chunks(
        &self,
        chunks: impl IntoIterator<Item = Vec<u8>>,
    ) -> Result<Vec<RecordBatch>> {
        let payload = chunks.into_iter().flatten().collect::<Vec<_>>();
        decode_ipc_bytes(&payload)
    }
}

fn decode_ipc_bytes(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let cur = Cursor::new(bytes.to_vec());
    let reader = arrow::ipc::reader::StreamReader::try_new(cur, None)
        .map_err(|e| FfqError::Execution(format!("ipc reader init failed: {e}")))?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| FfqError::Execution(format!("ipc read failed: {e}")))
}

fn decode_index_binary(bytes: &[u8]) -> Result<MapTaskIndex> {
    if bytes.len() < INDEX_BIN_HEADER_LEN {
        return Err(FfqError::Execution(
            "index.bin is too small to contain header".to_string(),
        ));
    }
    if &bytes[0..4] != INDEX_BIN_MAGIC {
        return Err(FfqError::Execution("invalid index.bin magic".to_string()));
    }
    let _version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    let len = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
    if bytes.len() < INDEX_BIN_HEADER_LEN + len {
        return Err(FfqError::Execution(
            "index.bin payload length is invalid".to_string(),
        ));
    }
    let payload = &bytes[INDEX_BIN_HEADER_LEN..INDEX_BIN_HEADER_LEN + len];
    serde_json::from_slice(payload)
        .map_err(|e| FfqError::Execution(format!("index.bin decode failed: {e}")))
}
