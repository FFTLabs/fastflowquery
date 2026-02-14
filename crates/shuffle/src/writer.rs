use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::record_batch::RecordBatch;
use ffq_common::{FfqError, Result};

use crate::layout::{
    index_bin_path, index_json_path, map_task_dir, shuffle_path, MapTaskIndex, ShufflePartitionMeta,
};

const INDEX_BIN_MAGIC: &[u8; 4] = b"FFQI";
const INDEX_BIN_VERSION: u32 = 1;

pub struct ShuffleWriter {
    root_dir: PathBuf,
}

impl ShuffleWriter {
    pub fn new(root_dir: impl Into<PathBuf>) -> Self {
        Self {
            root_dir: root_dir.into(),
        }
    }

    pub fn write_partition(
        &self,
        query_id: u64,
        stage_id: u64,
        map_task: u64,
        attempt: u32,
        reduce_partition: u32,
        batches: &[RecordBatch],
    ) -> Result<ShufflePartitionMeta> {
        let rel = shuffle_path(query_id, stage_id, map_task, attempt, reduce_partition);
        let abs = self.root_dir.join(&rel);
        if let Some(parent) = abs.parent() {
            fs::create_dir_all(parent)?;
        }

        let schema = batches.first().map(|b| b.schema()).ok_or_else(|| {
            FfqError::InvalidConfig("shuffle partition cannot be empty".to_string())
        })?;

        let mut file = File::create(&abs)?;
        {
            let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut file, schema.as_ref())
                .map_err(|e| FfqError::Execution(format!("ipc writer init failed: {e}")))?;
            for b in batches {
                writer
                    .write(b)
                    .map_err(|e| FfqError::Execution(format!("ipc write failed: {e}")))?;
            }
            writer
                .finish()
                .map_err(|e| FfqError::Execution(format!("ipc finish failed: {e}")))?;
        }
        file.flush()?;

        let bytes = fs::metadata(&abs)?.len();
        let rows = batches.iter().map(|b| b.num_rows() as u64).sum();
        let batches_count = batches.len() as u64;

        Ok(ShufflePartitionMeta {
            reduce_partition,
            file: rel,
            bytes,
            rows,
            batches: batches_count,
        })
    }

    pub fn write_map_task_index(
        &self,
        query_id: u64,
        stage_id: u64,
        map_task: u64,
        attempt: u32,
        mut partitions: Vec<ShufflePartitionMeta>,
    ) -> Result<MapTaskIndex> {
        partitions.sort_by_key(|p| p.reduce_partition);
        let index = MapTaskIndex {
            query_id,
            stage_id,
            map_task,
            attempt,
            created_at_ms: now_unix_ms()?,
            partitions,
        };

        let dir = self
            .root_dir
            .join(map_task_dir(query_id, stage_id, map_task, attempt));
        fs::create_dir_all(&dir)?;

        let json_path = self
            .root_dir
            .join(index_json_path(query_id, stage_id, map_task, attempt));
        let json_bytes = serde_json::to_vec_pretty(&index)
            .map_err(|e| FfqError::Execution(format!("index json encode failed: {e}")))?;
        fs::write(&json_path, &json_bytes)?;

        let bin_path = self
            .root_dir
            .join(index_bin_path(query_id, stage_id, map_task, attempt));
        self.write_binary_index(&bin_path, &json_bytes)?;

        Ok(index)
    }

    fn write_binary_index(&self, path: &Path, json_payload: &[u8]) -> Result<()> {
        let mut out = Vec::with_capacity(16 + json_payload.len());
        out.extend_from_slice(INDEX_BIN_MAGIC);
        out.extend_from_slice(&INDEX_BIN_VERSION.to_le_bytes());
        out.extend_from_slice(&(json_payload.len() as u32).to_le_bytes());
        out.extend_from_slice(json_payload);
        fs::write(path, out)?;
        Ok(())
    }

    pub fn cleanup_expired_attempts(&self, ttl: Duration, now: SystemTime) -> Result<usize> {
        let shuffle_root = self.root_dir.join("shuffle");
        if !shuffle_root.exists() {
            return Ok(0);
        }

        let now_ms = to_unix_ms(now)?;
        let ttl_ms = ttl.as_millis() as u64;
        let mut removed = 0_usize;

        for query in safe_read_dir(&shuffle_root)? {
            let query = query?;
            if !query.file_type()?.is_dir() {
                continue;
            }
            for stage in safe_read_dir(&query.path())? {
                let stage = stage?;
                if !stage.file_type()?.is_dir() {
                    continue;
                }
                for map in safe_read_dir(&stage.path())? {
                    let map = map?;
                    if !map.file_type()?.is_dir() {
                        continue;
                    }
                    let mut attempt_dirs = Vec::new();
                    for attempt in safe_read_dir(&map.path())? {
                        let attempt = attempt?;
                        if !attempt.file_type()?.is_dir() {
                            continue;
                        }
                        if let Ok(id) = attempt.file_name().to_string_lossy().parse::<u32>() {
                            attempt_dirs.push((id, attempt.path()));
                        }
                    }

                    let latest_attempt = attempt_dirs.iter().map(|(id, _)| *id).max();
                    for (attempt_id, attempt_path) in attempt_dirs {
                        if Some(attempt_id) == latest_attempt {
                            continue;
                        }
                        if let Some(created_at_ms) =
                            self.read_attempt_created_at_ms(&attempt_path)?
                        {
                            let expired = created_at_ms.saturating_add(ttl_ms) <= now_ms;
                            if expired {
                                fs::remove_dir_all(attempt_path)?;
                                removed += 1;
                            }
                        }
                    }
                }
            }
        }

        Ok(removed)
    }

    fn read_attempt_created_at_ms(&self, attempt_dir: &Path) -> Result<Option<u64>> {
        let json_path = attempt_dir.join("index.json");
        if !json_path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(json_path)?;
        let idx: MapTaskIndex = serde_json::from_slice(&bytes)
            .map_err(|e| FfqError::Execution(format!("index json decode failed: {e}")))?;
        Ok(Some(idx.created_at_ms))
    }
}

fn safe_read_dir(path: &Path) -> Result<fs::ReadDir> {
    fs::read_dir(path).map_err(FfqError::from)
}

fn now_unix_ms() -> Result<u64> {
    to_unix_ms(SystemTime::now())
}

fn to_unix_ms(ts: SystemTime) -> Result<u64> {
    ts.duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .map_err(|e| FfqError::Execution(format!("clock error: {e}")))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::layout::{index_json_path, MapTaskIndex};
    use crate::reader::ShuffleReader;

    use super::ShuffleWriter;

    fn temp_shuffle_root() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("ffq_shuffle_test_{nanos}"))
    }

    #[test]
    fn writes_index_and_reads_partition_from_streamed_chunks() {
        let root = temp_shuffle_root();
        let writer = ShuffleWriter::new(&root);

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
        )
        .expect("batch");

        let meta = writer
            .write_partition(100, 2, 7, 1, 3, &[batch])
            .expect("write partition");
        let idx = writer
            .write_map_task_index(100, 2, 7, 1, vec![meta.clone()])
            .expect("write index");
        assert_eq!(idx.partitions.len(), 1);
        assert_eq!(idx.partitions[0].reduce_partition, 3);

        let reader = ShuffleReader::new(&root).with_fetch_chunk_bytes(7);
        let read_meta = reader.partition_meta(100, 2, 7, 1, 3).expect("read meta");
        assert_eq!(read_meta.bytes, meta.bytes);

        let chunks = reader
            .fetch_partition_chunks(100, 2, 7, 1, 3)
            .expect("chunks");
        assert!(chunks.len() > 1);
        let batches = reader
            .read_partition_from_streamed_chunks(chunks)
            .expect("decode");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn ignores_old_attempts_and_cleans_up_by_ttl() {
        let root = temp_shuffle_root();
        let writer = ShuffleWriter::new(&root);
        let reader = ShuffleReader::new(&root).with_fetch_chunk_bytes(4);

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));

        let batch_old = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10_i64]))],
        )
        .expect("old batch");
        let old_meta = writer
            .write_partition(1, 1, 1, 1, 0, &[batch_old])
            .expect("old part");
        writer
            .write_map_task_index(1, 1, 1, 1, vec![old_meta])
            .expect("old index");

        let batch_new = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![20_i64]))],
        )
        .expect("new batch");
        let new_meta = writer
            .write_partition(1, 1, 1, 2, 0, &[batch_new])
            .expect("new part");
        writer
            .write_map_task_index(1, 1, 1, 2, vec![new_meta])
            .expect("new index");

        // Mark attempt 1 as very old in index.json to make TTL cleanup deterministic.
        let old_json = root.join(index_json_path(1, 1, 1, 1));
        let mut idx: MapTaskIndex =
            serde_json::from_slice(&std::fs::read(&old_json).expect("read index")).expect("json");
        idx.created_at_ms = 1;
        std::fs::write(&old_json, serde_json::to_vec_pretty(&idx).expect("encode"))
            .expect("write old index");

        let (attempt, chunks) = reader
            .fetch_partition_chunks_latest(1, 1, 1, 0)
            .expect("latest chunks");
        assert_eq!(attempt, 2);
        let batches = reader
            .read_partition_from_streamed_chunks(chunks)
            .expect("decode");
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64");
        assert_eq!(arr.value(0), 20);

        let removed = writer
            .cleanup_expired_attempts(Duration::from_millis(1), SystemTime::now())
            .expect("cleanup");
        assert!(removed >= 1);
        let attempts = reader.available_attempts(1, 1, 1).expect("attempts");
        assert_eq!(attempts, vec![2]);

        let _ = std::fs::remove_dir_all(root);
    }
}
