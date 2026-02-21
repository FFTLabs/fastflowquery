use std::fs;
use std::io::{Cursor, Read};
use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use ffq_common::{FfqError, Result};
use lz4_flex::frame::FrameDecoder;

use crate::layout::{
    MapTaskIndex, ShuffleCompressionCodec, ShufflePartitionMeta, index_bin_path, index_json_path,
    map_task_base_dir, shuffle_path,
};

const INDEX_BIN_MAGIC: &[u8; 4] = b"FFQI";
const INDEX_BIN_HEADER_LEN: usize = 12;
const SHUFFLE_PAYLOAD_MAGIC: &[u8; 4] = b"FFQS";
const SHUFFLE_PAYLOAD_HEADER_LEN: usize = 24;

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
        let file = fs::File::open(self.root_dir.join(rel))?;
        decode_partition_payload(file)
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
        let reader = ChunkedReader::new(chunks.into_iter().collect());
        decode_partition_payload(reader)
    }
}

fn decode_ipc_bytes(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    decode_ipc_read(Cursor::new(bytes.to_vec()))
}

fn decode_ipc_read<R: Read>(reader: R) -> Result<Vec<RecordBatch>> {
    let reader = arrow::ipc::reader::StreamReader::try_new(reader, None)
        .map_err(|e| FfqError::Execution(format!("ipc reader init failed: {e}")))?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| FfqError::Execution(format!("ipc read failed: {e}")))
}

fn decode_partition_payload<R: Read>(mut reader: R) -> Result<Vec<RecordBatch>> {
    let mut magic = [0_u8; 4];
    reader.read_exact(&mut magic)?;
    if &magic != SHUFFLE_PAYLOAD_MAGIC {
        let mut legacy = magic.to_vec();
        reader.read_to_end(&mut legacy)?;
        return decode_ipc_bytes(&legacy);
    }

    let mut rest_header = [0_u8; SHUFFLE_PAYLOAD_HEADER_LEN - 4];
    reader.read_exact(&mut rest_header)?;
    let version = rest_header[0];
    if version != 1 {
        return Err(FfqError::Execution(format!(
            "unsupported shuffle payload version {version}"
        )));
    }
    let codec = codec_from_u8(rest_header[1])?;
    let _uncompressed_bytes = u64::from_le_bytes([
        rest_header[4],
        rest_header[5],
        rest_header[6],
        rest_header[7],
        rest_header[8],
        rest_header[9],
        rest_header[10],
        rest_header[11],
    ]);
    let compressed_bytes = u64::from_le_bytes([
        rest_header[12],
        rest_header[13],
        rest_header[14],
        rest_header[15],
        rest_header[16],
        rest_header[17],
        rest_header[18],
        rest_header[19],
    ]);
    let mut limited = reader.take(compressed_bytes);
    match codec {
        ShuffleCompressionCodec::None => decode_ipc_read(&mut limited),
        ShuffleCompressionCodec::Lz4 => {
            let decoder = FrameDecoder::new(&mut limited);
            decode_ipc_read(decoder)
        }
        ShuffleCompressionCodec::Zstd => {
            let decoder = zstd::stream::read::Decoder::new(&mut limited)
                .map_err(|e| FfqError::Execution(format!("zstd decode init failed: {e}")))?;
            decode_ipc_read(decoder)
        }
    }
}

fn codec_from_u8(raw: u8) -> Result<ShuffleCompressionCodec> {
    match raw {
        0 => Ok(ShuffleCompressionCodec::None),
        1 => Ok(ShuffleCompressionCodec::Lz4),
        2 => Ok(ShuffleCompressionCodec::Zstd),
        other => Err(FfqError::Execution(format!(
            "unsupported shuffle payload codec {other}"
        ))),
    }
}

struct ChunkedReader {
    chunks: Vec<Vec<u8>>,
    chunk_idx: usize,
    chunk_offset: usize,
}

impl ChunkedReader {
    fn new(chunks: Vec<Vec<u8>>) -> Self {
        Self {
            chunks,
            chunk_idx: 0,
            chunk_offset: 0,
        }
    }
}

impl Read for ChunkedReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let mut written = 0;
        while written < buf.len() && self.chunk_idx < self.chunks.len() {
            let chunk = &self.chunks[self.chunk_idx];
            if self.chunk_offset >= chunk.len() {
                self.chunk_idx += 1;
                self.chunk_offset = 0;
                continue;
            }
            let remain_chunk = chunk.len() - self.chunk_offset;
            let remain_buf = buf.len() - written;
            let take = remain_chunk.min(remain_buf);
            buf[written..written + take]
                .copy_from_slice(&chunk[self.chunk_offset..self.chunk_offset + take]);
            written += take;
            self.chunk_offset += take;
            if self.chunk_offset >= chunk.len() {
                self.chunk_idx += 1;
                self.chunk_offset = 0;
            }
        }
        Ok(written)
    }
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
