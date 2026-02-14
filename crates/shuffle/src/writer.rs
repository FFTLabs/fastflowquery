use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

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
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

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
}
