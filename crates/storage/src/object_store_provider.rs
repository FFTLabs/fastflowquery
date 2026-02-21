use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use ffq_common::{FfqError, Result};
use ffq_execution::{ExecNode, SendableRecordBatchStream, StreamAdapter, TaskContext};
use ffq_planner::Expr;
use futures::TryStreamExt;
use object_store::{GetOptions, ObjectStore, parse_url_opts};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use url::Url;

use crate::catalog::TableDef;
use crate::provider::{Stats, StorageExecNode, StorageProvider};

/// Object-store backed parquet scan provider (S3/GCS/Azure via `object_store`).
pub struct ObjectStoreProvider;

impl ObjectStoreProvider {
    /// Creates an object-store provider.
    pub fn new() -> Self {
        Self
    }
}

/// Returns true if `path` looks like an object-store style URI.
#[must_use]
pub fn is_object_store_uri(path: &str) -> bool {
    path.contains("://")
}

#[derive(Debug, Clone)]
struct ObjectStoreSettings {
    retry_attempts: usize,
    retry_backoff_ms: u64,
    max_concurrency: usize,
    range_chunk_size_bytes: usize,
    timeout_secs: Option<u64>,
    connect_timeout_secs: Option<u64>,
}

impl Default for ObjectStoreSettings {
    fn default() -> Self {
        Self {
            retry_attempts: 3,
            retry_backoff_ms: 250,
            max_concurrency: 4,
            range_chunk_size_bytes: 8 * 1024 * 1024,
            timeout_secs: Some(30),
            connect_timeout_secs: Some(5),
        }
    }
}

impl ObjectStoreSettings {
    fn from_table(table: &TableDef) -> Self {
        let mut s = Self::default();
        if let Some(v) = std::env::var("FFQ_OBJECT_STORE_RETRY_ATTEMPTS")
            .ok()
            .and_then(|x| x.parse::<usize>().ok())
        {
            s.retry_attempts = v.max(1);
        }
        if let Some(v) = std::env::var("FFQ_OBJECT_STORE_RETRY_BACKOFF_MS")
            .ok()
            .and_then(|x| x.parse::<u64>().ok())
        {
            s.retry_backoff_ms = v;
        }
        if let Some(v) = std::env::var("FFQ_OBJECT_STORE_MAX_CONCURRENCY")
            .ok()
            .and_then(|x| x.parse::<usize>().ok())
        {
            s.max_concurrency = v.max(1);
        }
        if let Some(v) = std::env::var("FFQ_OBJECT_STORE_RANGE_CHUNK_SIZE")
            .ok()
            .and_then(|x| x.parse::<usize>().ok())
        {
            s.range_chunk_size_bytes = v.max(1024);
        }
        if let Some(v) = std::env::var("FFQ_OBJECT_STORE_TIMEOUT_SECS")
            .ok()
            .and_then(|x| x.parse::<u64>().ok())
        {
            s.timeout_secs = Some(v.max(1));
        }
        if let Some(v) = std::env::var("FFQ_OBJECT_STORE_CONNECT_TIMEOUT_SECS")
            .ok()
            .and_then(|x| x.parse::<u64>().ok())
        {
            s.connect_timeout_secs = Some(v.max(1));
        }

        if let Some(v) = table
            .options
            .get("object_store.retry_attempts")
            .and_then(|x| x.parse::<usize>().ok())
        {
            s.retry_attempts = v.max(1);
        }
        if let Some(v) = table
            .options
            .get("object_store.retry_backoff_ms")
            .and_then(|x| x.parse::<u64>().ok())
        {
            s.retry_backoff_ms = v;
        }
        if let Some(v) = table
            .options
            .get("object_store.max_concurrency")
            .and_then(|x| x.parse::<usize>().ok())
        {
            s.max_concurrency = v.max(1);
        }
        if let Some(v) = table
            .options
            .get("object_store.range_chunk_size_bytes")
            .and_then(|x| x.parse::<usize>().ok())
        {
            s.range_chunk_size_bytes = v.max(1024);
        }
        if let Some(v) = table
            .options
            .get("object_store.timeout_secs")
            .and_then(|x| x.parse::<u64>().ok())
        {
            s.timeout_secs = Some(v.max(1));
        }
        if let Some(v) = table
            .options
            .get("object_store.connect_timeout_secs")
            .and_then(|x| x.parse::<u64>().ok())
        {
            s.connect_timeout_secs = Some(v.max(1));
        }
        s
    }
}

fn build_object_store_options(
    table: &TableDef,
    settings: &ObjectStoreSettings,
) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for (k, v) in &table.options {
        if let Some(rest) = k.strip_prefix("object_store.") {
            out.insert(rest.to_string(), v.clone());
        }
    }
    if let Some(v) = settings.timeout_secs {
        out.insert("timeout".to_string(), format!("{v} seconds"));
    }
    if let Some(v) = settings.connect_timeout_secs {
        out.insert("connect_timeout".to_string(), format!("{v} seconds"));
    }
    out
}

#[derive(Debug)]
struct ObjectStoreScanNode {
    uris: Vec<String>,
    schema: SchemaRef,
    source_schema: SchemaRef,
    projection_indices: Vec<usize>,
    settings: ObjectStoreSettings,
    options: HashMap<String, String>,
}

impl ExecNode for ObjectStoreScanNode {
    fn name(&self) -> &'static str {
        "ObjectStoreScanNode"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let mut out = Vec::<Result<RecordBatch>>::new();
        let mut all_batches = Vec::<RecordBatch>::new();
        for uri in &self.uris {
            let bytes = fetch_object_with_retry(uri, &self.options, &self.settings)?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
                .map_err(|e| {
                    FfqError::Execution(format!("parquet reader build failed for '{uri}': {e}"))
                })?
                .build()
                .map_err(|e| {
                    FfqError::Execution(format!("parquet reader open failed for '{uri}': {e}"))
                })?;
            for batch in reader {
                let batch = batch.map_err(|e| {
                    FfqError::Execution(format!("parquet decode failed for '{uri}': {e}"))
                })?;
                all_batches.push(batch);
            }
        }

        for batch in all_batches {
            if batch.schema().fields().len() != self.source_schema.fields().len() {
                return Err(FfqError::Execution(format!(
                    "object-store parquet scan schema mismatch: expected {} columns, got {}",
                    self.source_schema.fields().len(),
                    batch.schema().fields().len()
                )));
            }
            let cols = self
                .projection_indices
                .iter()
                .map(|idx| batch.column(*idx).clone())
                .collect::<Vec<_>>();
            out.push(
                RecordBatch::try_new(self.schema.clone(), cols).map_err(|e| {
                    FfqError::Execution(format!("object-store projection failed: {e}"))
                }),
            );
        }

        Ok(Box::pin(StreamAdapter::new(
            self.schema.clone(),
            futures::stream::iter(out),
        )))
    }
}

fn fetch_object_with_retry(
    uri: &str,
    options: &HashMap<String, String>,
    settings: &ObjectStoreSettings,
) -> Result<bytes::Bytes> {
    let mut last_err = None;
    for attempt in 1..=settings.retry_attempts {
        match fetch_object_once(uri, options, settings) {
            Ok(v) => return Ok(v),
            Err(e) => {
                last_err = Some(e);
                if attempt < settings.retry_attempts {
                    thread::sleep(Duration::from_millis(settings.retry_backoff_ms));
                }
            }
        }
    }
    Err(FfqError::Execution(format!(
        "object-store fetch failed after {} attempts for '{}': {}",
        settings.retry_attempts,
        uri,
        last_err
            .map(|e| e.to_string())
            .unwrap_or_else(|| "unknown error".to_string())
    )))
}

fn fetch_object_once(
    uri: &str,
    options: &HashMap<String, String>,
    settings: &ObjectStoreSettings,
) -> Result<bytes::Bytes> {
    let url = Url::parse(uri)
        .map_err(|e| FfqError::InvalidConfig(format!("invalid object-store uri '{}': {e}", uri)))?;
    let (store, path) = parse_url_opts(&url, options.clone()).map_err(|e| {
        FfqError::InvalidConfig(format!("failed to build object store for '{}': {e}", uri))
    })?;

    let head = futures::executor::block_on(store.head(&path))
        .map_err(|e| FfqError::Execution(format!("object-store head failed for '{}': {e}", uri)))?;

    if head.size > settings.range_chunk_size_bytes {
        let mut ranges = Vec::new();
        let mut start = 0usize;
        while start < head.size {
            let end = (start + settings.range_chunk_size_bytes).min(head.size);
            ranges.push(start..end);
            start = end;
        }
        let mut chunks = Vec::new();
        for chunk in ranges.chunks(settings.max_concurrency.max(1)) {
            let next =
                futures::executor::block_on(store.get_ranges(&path, chunk)).map_err(|e| {
                    FfqError::Execution(format!(
                        "object-store ranged get failed for '{}': {e}",
                        uri
                    ))
                })?;
            chunks.extend(next);
        }
        let mut combined = Vec::with_capacity(head.size);
        for c in chunks {
            combined.extend_from_slice(&c);
        }
        return Ok(combined.into());
    }

    futures::executor::block_on(async {
        store
            .get_opts(&path, GetOptions::default())
            .await
            .and_then(|r| r.bytes())
            .await
    })
    .map_err(|e| FfqError::Execution(format!("object-store get failed for '{}': {e}", uri)))
}

impl StorageProvider for ObjectStoreProvider {
    fn estimate_stats(&self, table: &TableDef) -> Stats {
        Stats {
            estimated_rows: table.stats.rows,
            estimated_bytes: table.stats.bytes,
        }
    }

    fn scan(
        &self,
        table: &TableDef,
        projection: Option<Vec<String>>,
        _filters: Vec<Expr>,
    ) -> Result<StorageExecNode> {
        if table.format.to_ascii_lowercase() != "parquet" {
            return Err(FfqError::Unsupported(format!(
                "object-store provider currently supports only parquet format, got '{}'",
                table.format
            )));
        }

        let settings = ObjectStoreSettings::from_table(table);
        let options = build_object_store_options(table, &settings);
        let paths = table.data_paths()?;
        if paths.is_empty() {
            return Err(FfqError::InvalidConfig(format!(
                "table '{}' has no object-store paths configured",
                table.name
            )));
        }
        for path in &paths {
            if !is_object_store_uri(path) {
                return Err(FfqError::InvalidConfig(format!(
                    "path '{}' is not an object-store uri; expected scheme://...",
                    path
                )));
            }
        }

        let source_schema = match &table.schema {
            Some(s) => Arc::new(s.clone()),
            None => {
                return Err(FfqError::InvalidConfig(format!(
                    "table '{}' requires schema for object-store scans in current implementation",
                    table.name
                )));
            }
        };

        let (schema, projection_indices) = if let Some(cols) = &projection {
            let mut fields = Vec::with_capacity(cols.len());
            let mut indices = Vec::with_capacity(cols.len());
            for col in cols {
                let idx = source_schema.index_of(col).map_err(|_| {
                    FfqError::Planning(format!(
                        "projection column '{}' not found in table '{}'",
                        col, table.name
                    ))
                })?;
                indices.push(idx);
                fields.push(source_schema.field(idx).clone());
            }
            (Arc::new(Schema::new(fields)), indices)
        } else {
            (
                source_schema.clone(),
                (0..source_schema.fields().len()).collect::<Vec<_>>(),
            )
        };

        Ok(Arc::new(ObjectStoreScanNode {
            uris: paths,
            schema,
            source_schema,
            projection_indices,
            settings,
            options,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use futures::TryStreamExt;
    use parquet::arrow::ArrowWriter;

    use crate::TableStats;

    #[test]
    fn object_store_uri_detection_requires_scheme() {
        assert!(is_object_store_uri("s3://bucket/path.parquet"));
        assert!(is_object_store_uri("gs://bucket/path.parquet"));
        assert!(is_object_store_uri("file:///tmp/x.parquet"));
        assert!(!is_object_store_uri("/tmp/x.parquet"));
        assert!(!is_object_store_uri("relative/path.parquet"));
    }

    #[test]
    fn object_store_scan_reads_file_uri_parquet() {
        let p = unique_path("object_store_file_uri_scan", "parquet");
        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
        ]));
        write_parquet_file(
            &p,
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            ],
        );

        let uri = Url::from_file_path(&p).expect("file uri").to_string();
        let provider = ObjectStoreProvider::new();
        let table = TableDef {
            name: "t".to_string(),
            uri,
            paths: vec![],
            format: "parquet".to_string(),
            schema: Some(schema.as_ref().clone()),
            stats: TableStats::default(),
            options: HashMap::new(),
        };
        let node = provider
            .scan(&table, Some(vec!["id".to_string()]), vec![])
            .expect("scan");
        let stream = node
            .execute(Arc::new(TaskContext {
                batch_size_rows: 1024,
                mem_budget_bytes: usize::MAX,
            }))
            .expect("execute");
        let batches =
            futures::executor::block_on(stream.try_collect::<Vec<RecordBatch>>()).expect("collect");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].schema().fields().len(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "id");

        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn object_store_scan_retries_then_fails_for_missing_object() {
        let missing = unique_path("object_store_missing", "parquet");
        let uri = Url::from_file_path(&missing).expect("file uri").to_string();
        let schema = Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]);
        let mut options = HashMap::new();
        options.insert("object_store.retry_attempts".to_string(), "2".to_string());
        options.insert("object_store.retry_backoff_ms".to_string(), "0".to_string());
        let table = TableDef {
            name: "missing".to_string(),
            uri,
            paths: vec![],
            format: "parquet".to_string(),
            schema: Some(schema),
            stats: TableStats::default(),
            options,
        };
        let provider = ObjectStoreProvider::new();
        let node = provider.scan(&table, None, vec![]).expect("scan");
        let err = node
            .execute(Arc::new(TaskContext {
                batch_size_rows: 1024,
                mem_budget_bytes: usize::MAX,
            }))
            .expect_err("expected failure");
        let msg = err.to_string();
        assert!(msg.contains("after 2 attempts"));
        assert!(msg.contains("object-store fetch failed"));
    }

    fn write_parquet_file(path: &std::path::Path, schema: Arc<Schema>, cols: Vec<ArrayRef>) {
        let batch = RecordBatch::try_new(schema.clone(), cols).expect("build batch");
        let file = File::create(path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }

    fn unique_path(prefix: &str, ext: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("ffq_storage_{prefix}_{nanos}.{ext}"))
    }
}
