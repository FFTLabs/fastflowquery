use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::record_batch::RecordBatch;
use ffq_common::{FfqError, Result};
use lz4_flex::frame::FrameEncoder;

use crate::layout::{
    MapTaskIndex, ShuffleCompressionCodec, ShufflePartitionChunkMeta, ShufflePartitionMeta,
    index_bin_path, index_json_path, map_task_dir, shuffle_path,
};

const INDEX_BIN_MAGIC: &[u8; 4] = b"FFQI";
const INDEX_BIN_VERSION: u32 = 1;
const SHUFFLE_PAYLOAD_MAGIC: &[u8; 4] = b"FFQS";
const SHUFFLE_PAYLOAD_VERSION: u8 = 1;
const SHUFFLE_PAYLOAD_HEADER_LEN: usize = 24;

/// Writes shuffle partition payloads and map-task index metadata.
pub struct ShuffleWriter {
    root_dir: PathBuf,
    compression_codec: ShuffleCompressionCodec,
}

impl ShuffleWriter {
    /// Create a writer rooted at `root_dir`.
    pub fn new(root_dir: impl Into<PathBuf>) -> Self {
        Self {
            root_dir: root_dir.into(),
            compression_codec: ShuffleCompressionCodec::None,
        }
    }

    /// Configure compression codec for partition payloads written by this writer.
    pub fn with_compression_codec(mut self, codec: ShuffleCompressionCodec) -> Self {
        self.compression_codec = codec;
        self
    }

    /// Write one reduce partition payload as Arrow IPC and return its metadata.
    pub fn write_partition(
        &self,
        query_id: u64,
        stage_id: u64,
        map_task: u64,
        attempt: u32,
        reduce_partition: u32,
        batches: &[RecordBatch],
    ) -> Result<ShufflePartitionMeta> {
        let schema = batches.first().map(|b| b.schema()).ok_or_else(|| {
            FfqError::InvalidConfig("shuffle partition cannot be empty".to_string())
        })?;
        let chunk = self.append_partition_chunk(
            query_id,
            stage_id,
            map_task,
            attempt,
            reduce_partition,
            batches,
            schema.as_ref(),
        )?;

        Ok(ShufflePartitionMeta {
            reduce_partition,
            file: shuffle_path(query_id, stage_id, map_task, attempt, reduce_partition),
            bytes: chunk.frame_bytes,
            compressed_bytes: chunk.compressed_bytes,
            uncompressed_bytes: chunk.uncompressed_bytes,
            codec: self.compression_codec,
            chunks: vec![chunk.clone()],
            rows: chunk.rows,
            batches: chunk.batches,
        })
    }

    /// Append one chunk frame to a partition payload file and return chunk metadata.
    pub fn append_partition_chunk(
        &self,
        query_id: u64,
        stage_id: u64,
        map_task: u64,
        attempt: u32,
        reduce_partition: u32,
        batches: &[RecordBatch],
        schema: &arrow::datatypes::Schema,
    ) -> Result<ShufflePartitionChunkMeta> {
        let rel = shuffle_path(query_id, stage_id, map_task, attempt, reduce_partition);
        let abs = self.root_dir.join(&rel);
        if let Some(parent) = abs.parent() {
            fs::create_dir_all(parent)?;
        }

        let ipc_payload = encode_ipc_payload(batches, schema)?;
        let uncompressed_bytes = ipc_payload.len() as u64;
        let compressed_payload = compress_ipc_payload(&ipc_payload, self.compression_codec)?;
        let compressed_bytes = compressed_payload.len() as u64;
        let framed_payload = frame_payload(
            self.compression_codec,
            uncompressed_bytes,
            compressed_bytes,
            &compressed_payload,
        );
        let checksum32 = adler32(&framed_payload);
        let frame_bytes = framed_payload.len() as u64;
        let rows = batches.iter().map(|b| b.num_rows() as u64).sum::<u64>();
        let batches_count = batches.len() as u64;
        let offset_bytes = fs::metadata(&abs).map(|m| m.len()).unwrap_or(0);

        let mut file = OpenOptions::new().create(true).append(true).open(&abs)?;
        file.write_all(&framed_payload)?;
        file.flush()?;

        Ok(ShufflePartitionChunkMeta {
            offset_bytes,
            frame_bytes,
            compressed_bytes,
            uncompressed_bytes,
            rows,
            batches: batches_count,
            checksum32,
        })
    }

    /// Write JSON and binary index files for one map-task attempt.
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

    /// Remove expired non-latest attempts based on `ttl`.
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

fn encode_ipc_payload(
    batches: &[RecordBatch],
    schema: &arrow::datatypes::Schema,
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut out, schema)
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
    Ok(out)
}

fn compress_ipc_payload(payload: &[u8], codec: ShuffleCompressionCodec) -> Result<Vec<u8>> {
    match codec {
        ShuffleCompressionCodec::None => Ok(payload.to_vec()),
        ShuffleCompressionCodec::Lz4 => {
            let mut encoder = FrameEncoder::new(Vec::new());
            encoder
                .write_all(payload)
                .map_err(|e| FfqError::Execution(format!("lz4 encode failed: {e}")))?;
            encoder
                .finish()
                .map_err(|e| FfqError::Execution(format!("lz4 finalize failed: {e}")))
        }
        ShuffleCompressionCodec::Zstd => zstd::stream::encode_all(payload, 0)
            .map_err(|e| FfqError::Execution(format!("zstd encode failed: {e}"))),
    }
}

fn codec_to_u8(codec: ShuffleCompressionCodec) -> u8 {
    match codec {
        ShuffleCompressionCodec::None => 0,
        ShuffleCompressionCodec::Lz4 => 1,
        ShuffleCompressionCodec::Zstd => 2,
    }
}

fn frame_payload(
    codec: ShuffleCompressionCodec,
    uncompressed_bytes: u64,
    compressed_bytes: u64,
    compressed_payload: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(SHUFFLE_PAYLOAD_HEADER_LEN + compressed_payload.len());
    out.extend_from_slice(SHUFFLE_PAYLOAD_MAGIC);
    out.push(SHUFFLE_PAYLOAD_VERSION);
    out.push(codec_to_u8(codec));
    out.extend_from_slice(&[0_u8, 0_u8]);
    out.extend_from_slice(&uncompressed_bytes.to_le_bytes());
    out.extend_from_slice(&compressed_bytes.to_le_bytes());
    out.extend_from_slice(compressed_payload);
    out
}

fn adler32(payload: &[u8]) -> u32 {
    const MOD: u32 = 65_521;
    let mut a: u32 = 1;
    let mut b: u32 = 0;
    for byte in payload {
        a = (a + u32::from(*byte)) % MOD;
        b = (b + a) % MOD;
    }
    (b << 16) | a
}

/// Build aggregated partition metadata from appended chunk metadata.
pub fn aggregate_partition_chunks(
    query_id: u64,
    stage_id: u64,
    map_task: u64,
    attempt: u32,
    codec: ShuffleCompressionCodec,
    chunks_by_partition: HashMap<u32, Vec<ShufflePartitionChunkMeta>>,
) -> Vec<ShufflePartitionMeta> {
    let mut out = Vec::new();
    for (reduce_partition, mut chunks) in chunks_by_partition {
        chunks.sort_by_key(|c| c.offset_bytes);
        let bytes = chunks.iter().map(|c| c.frame_bytes).sum::<u64>();
        let compressed_bytes = chunks.iter().map(|c| c.compressed_bytes).sum::<u64>();
        let uncompressed_bytes = chunks.iter().map(|c| c.uncompressed_bytes).sum::<u64>();
        let rows = chunks.iter().map(|c| c.rows).sum::<u64>();
        let batches = chunks.iter().map(|c| c.batches).sum::<u64>();
        out.push(ShufflePartitionMeta {
            reduce_partition,
            file: shuffle_path(query_id, stage_id, map_task, attempt, reduce_partition),
            bytes,
            compressed_bytes,
            uncompressed_bytes,
            codec,
            chunks,
            rows,
            batches,
        });
    }
    out.sort_by_key(|m| m.reduce_partition);
    out
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::layout::{
        MapTaskIndex, ShuffleCompressionCodec, ShufflePartitionChunkMeta, index_json_path,
    };
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
        let writer = ShuffleWriter::new(&root).with_compression_codec(ShuffleCompressionCodec::Lz4);

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
        assert_eq!(read_meta.codec, ShuffleCompressionCodec::Lz4);
        assert!(read_meta.uncompressed_bytes >= read_meta.compressed_bytes);
        assert_eq!(read_meta.chunks.len(), 1);

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
    fn appends_multiple_chunks_and_records_chunk_index_entries() {
        let root = temp_shuffle_root();
        let writer =
            ShuffleWriter::new(&root).with_compression_codec(ShuffleCompressionCodec::Zstd);
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
        )
        .expect("batch1");
        let b2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![3_i64, 4]))],
        )
        .expect("batch2");
        let c1 = writer
            .append_partition_chunk(9, 1, 0, 1, 0, &[b1], schema.as_ref())
            .expect("chunk1");
        let c2 = writer
            .append_partition_chunk(9, 1, 0, 1, 0, &[b2], schema.as_ref())
            .expect("chunk2");
        let mut by_part = HashMap::<u32, Vec<ShufflePartitionChunkMeta>>::new();
        by_part.insert(0, vec![c1.clone(), c2.clone()]);
        let parts =
            super::aggregate_partition_chunks(9, 1, 0, 1, ShuffleCompressionCodec::Zstd, by_part);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].chunks.len(), 2);
        assert_eq!(parts[0].chunks[0].offset_bytes, c1.offset_bytes);
        assert_eq!(parts[0].chunks[1].offset_bytes, c2.offset_bytes);
        writer
            .write_map_task_index(9, 1, 0, 1, parts.clone())
            .expect("index");
        let reader = ShuffleReader::new(&root);
        let batches = reader.read_partition(9, 1, 0, 1, 0).expect("read");
        let rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
        assert_eq!(rows, 4);
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
