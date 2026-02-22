use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use ffq_common::metrics::global_metrics;
use ffq_common::{FfqError, Result};
use ffq_execution::{ExecNode, SendableRecordBatchStream, StreamAdapter, TaskContext};
use ffq_planner::{BinaryOp, Expr, LiteralValue};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::statistics::Statistics as ParquetStatistics;
use serde::{Deserialize, Serialize};

use crate::catalog::TableDef;
use crate::provider::{Stats, StorageExecNode, StorageProvider};
use crate::stats::{ColumnRangeStats, ParquetFileStats, ScalarStatValue};

/// Local parquet-backed [`StorageProvider`] implementation.
///
/// Supports:
/// - schema inference from parquet footers
/// - deterministic multi-file schema merge with strict/permissive policy
/// - basic projection pushdown by column selection
///
/// Drift semantics:
/// - drift detection itself is handled by client-side schema-fingerprint policy
/// - this provider exposes [`FileFingerprint`] helpers used by that logic
pub struct ParquetProvider;

/// Stable per-file fingerprint used for schema drift detection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileFingerprint {
    /// File path.
    pub path: String,
    /// File size in bytes.
    pub size_bytes: u64,
    /// File modification timestamp (nanoseconds since Unix epoch).
    pub mtime_ns: u128,
}

#[derive(Debug, Clone)]
struct CacheSettings {
    metadata_enabled: bool,
    block_enabled: bool,
    ttl: Duration,
    metadata_max_entries: usize,
    block_max_entries: usize,
}

impl Default for CacheSettings {
    fn default() -> Self {
        Self {
            metadata_enabled: true,
            block_enabled: false,
            ttl: Duration::from_secs(300),
            metadata_max_entries: 4096,
            block_max_entries: 64,
        }
    }
}

impl CacheSettings {
    fn from_table(table: &TableDef) -> Self {
        let mut s = Self::from_env();
        if let Some(v) = table.options.get("cache.metadata.enabled") {
            s.metadata_enabled = parse_bool(v, s.metadata_enabled);
        }
        if let Some(v) = table.options.get("cache.block.enabled") {
            s.block_enabled = parse_bool(v, s.block_enabled);
        }
        if let Some(v) = table
            .options
            .get("cache.ttl_secs")
            .and_then(|v| v.parse::<u64>().ok())
        {
            s.ttl = Duration::from_secs(v);
        }
        s
    }

    fn from_env() -> Self {
        let mut s = Self::default();
        if let Ok(v) = std::env::var("FFQ_PARQUET_METADATA_CACHE_ENABLED") {
            s.metadata_enabled = parse_bool(&v, s.metadata_enabled);
        }
        if let Ok(v) = std::env::var("FFQ_PARQUET_BLOCK_CACHE_ENABLED") {
            s.block_enabled = parse_bool(&v, s.block_enabled);
        }
        if let Some(v) = std::env::var("FFQ_FILE_CACHE_TTL_SECS")
            .ok()
            .and_then(|x| x.parse::<u64>().ok())
        {
            s.ttl = Duration::from_secs(v);
        }
        if let Some(v) = std::env::var("FFQ_PARQUET_METADATA_CACHE_MAX_ENTRIES")
            .ok()
            .and_then(|x| x.parse::<usize>().ok())
        {
            s.metadata_max_entries = v.max(1);
        }
        if let Some(v) = std::env::var("FFQ_PARQUET_BLOCK_CACHE_MAX_ENTRIES")
            .ok()
            .and_then(|x| x.parse::<usize>().ok())
        {
            s.block_max_entries = v.max(1);
        }
        s
    }
}

#[derive(Debug, Clone)]
struct FileIdentity {
    size_bytes: u64,
    mtime_ns: u128,
}

#[derive(Debug, Clone)]
struct MetadataCacheEntry {
    inserted_at: SystemTime,
    identity: FileIdentity,
    schema: Schema,
    stats: ParquetFileStats,
}

#[derive(Debug, Clone)]
struct BlockCacheEntry {
    inserted_at: SystemTime,
    identity: FileIdentity,
    source_schema: SchemaRef,
    full_batches: Vec<RecordBatch>,
}

static METADATA_CACHE: OnceLock<RwLock<HashMap<String, MetadataCacheEntry>>> = OnceLock::new();
static BLOCK_CACHE: OnceLock<RwLock<HashMap<String, BlockCacheEntry>>> = OnceLock::new();

fn metadata_cache() -> &'static RwLock<HashMap<String, MetadataCacheEntry>> {
    METADATA_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

fn block_cache() -> &'static RwLock<HashMap<String, BlockCacheEntry>> {
    BLOCK_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

fn parse_bool(raw: &str, default: bool) -> bool {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => true,
        "0" | "false" | "no" | "off" => false,
        _ => default,
    }
}

impl ParquetProvider {
    /// Creates a parquet provider instance.
    pub fn new() -> Self {
        Self
    }

    /// Infers schema for a parquet path list using permissive merge policy.
    ///
    /// Equivalent to [`ParquetProvider::infer_parquet_schema_with_policy`] with
    /// `permissive_merge = true`.
    ///
    /// # Errors
    /// Returns an error for empty path lists, read/decode failures, or incompatible schemas.
    pub fn infer_parquet_schema(paths: &[String]) -> Result<arrow_schema::Schema> {
        Self::infer_parquet_schema_with_policy(paths, true)
    }

    /// Infers schema for one or more parquet files and merges them deterministically.
    ///
    /// Policy:
    /// - strict mode (`permissive_merge = false`): requires exact type compatibility
    /// - permissive mode (`permissive_merge = true`): allows nullable widening and
    ///   limited numeric widening (for example int32 + int64 => int64)
    ///
    /// # Errors
    /// Returns an error for empty path list, parquet read failures, or incompatible schemas.
    pub fn infer_parquet_schema_with_policy(
        paths: &[String],
        permissive_merge: bool,
    ) -> Result<arrow_schema::Schema> {
        if paths.is_empty() {
            return Err(FfqError::InvalidConfig(
                "cannot infer parquet schema from empty path list".to_string(),
            ));
        }

        let mut inferred: Option<arrow_schema::Schema> = None;
        let cache_settings = CacheSettings::from_env();
        for path in paths {
            let meta = get_or_load_metadata(path, &cache_settings)?;
            let schema = meta.schema.clone();

            match &inferred {
                None => inferred = Some(schema),
                Some(existing) => {
                    inferred = Some(merge_schemas(existing, &schema, path, permissive_merge)?)
                }
            };
        }

        inferred.ok_or_else(|| {
            FfqError::InvalidConfig("failed to infer parquet schema from input paths".to_string())
        })
    }

    /// Builds per-file fingerprints for schema drift checks.
    ///
    /// # Errors
    /// Returns an error when file metadata cannot be read.
    pub fn fingerprint_paths(paths: &[String]) -> Result<Vec<FileFingerprint>> {
        let mut out = Vec::with_capacity(paths.len());
        for path in paths {
            let md = std::fs::metadata(path).map_err(|e| {
                FfqError::InvalidConfig(format!("failed to stat parquet path '{}': {e}", path))
            })?;
            let modified = md.modified().map_err(|e| {
                FfqError::InvalidConfig(format!("failed to read modified time for '{}': {e}", path))
            })?;
            let mtime_ns = modified
                .duration_since(UNIX_EPOCH)
                .map_err(|e| {
                    FfqError::InvalidConfig(format!("invalid modified time for '{}': {e}", path))
                })?
                .as_nanos();
            out.push(FileFingerprint {
                path: path.clone(),
                size_bytes: md.len(),
                mtime_ns,
            });
        }
        Ok(out)
    }

    /// Collects parquet file statistics used for optimizer heuristics and pruning.
    ///
    /// Per file captures:
    /// - `row_count`
    /// - `size_bytes`
    /// - per-column min/max (for supported parquet statistics types)
    ///
    /// # Errors
    /// Returns an error when file metadata or parquet metadata read fails.
    pub fn collect_parquet_file_stats(paths: &[String]) -> Result<Vec<ParquetFileStats>> {
        let mut out = Vec::with_capacity(paths.len());
        let cache_settings = CacheSettings::from_env();
        for path in paths {
            let meta = get_or_load_metadata(path, &cache_settings)?;
            out.push(meta.stats.clone());
        }
        Ok(out)
    }
}

fn file_identity(path: &str) -> Result<FileIdentity> {
    let md = std::fs::metadata(path).map_err(|e| {
        FfqError::InvalidConfig(format!("failed to stat parquet path '{}': {e}", path))
    })?;
    let modified = md.modified().map_err(|e| {
        FfqError::InvalidConfig(format!("failed to read modified time for '{}': {e}", path))
    })?;
    let mtime_ns = modified
        .duration_since(UNIX_EPOCH)
        .map_err(|e| FfqError::InvalidConfig(format!("invalid modified time for '{}': {e}", path)))?
        .as_nanos();
    Ok(FileIdentity {
        size_bytes: md.len(),
        mtime_ns,
    })
}

fn get_or_load_metadata(path: &str, settings: &CacheSettings) -> Result<MetadataCacheEntry> {
    let identity = file_identity(path)?;
    if settings.metadata_enabled {
        let now = SystemTime::now();
        if let Some(hit) = metadata_cache()
            .read()
            .ok()
            .and_then(|cache| cache.get(path).cloned())
            .filter(|entry| {
                entry.identity.size_bytes == identity.size_bytes
                    && entry.identity.mtime_ns == identity.mtime_ns
                    && now
                        .duration_since(entry.inserted_at)
                        .map(|age| age <= settings.ttl)
                        .unwrap_or(false)
            })
        {
            global_metrics().inc_file_cache_event("metadata", true);
            return Ok(hit);
        }
        global_metrics().inc_file_cache_event("metadata", false);
    }
    let loaded = load_metadata_entry(path, identity)?;
    if settings.metadata_enabled {
        if let Ok(mut cache) = metadata_cache().write() {
            evict_cache_map(&mut cache, settings.ttl, settings.metadata_max_entries);
            cache.insert(path.to_string(), loaded.clone());
        }
    }
    Ok(loaded)
}

fn load_metadata_entry(path: &str, identity: FileIdentity) -> Result<MetadataCacheEntry> {
    let size_bytes = identity.size_bytes;
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
        FfqError::Execution(format!(
            "parquet metadata reader build failed for '{path}': {e}"
        ))
    })?;
    let schema = builder.schema().as_ref().clone();
    let meta = builder.metadata();
    let row_count = meta.file_metadata().num_rows() as u64;
    let mut column_ranges = HashMap::<String, ColumnRangeStats>::new();
    for rg in meta.row_groups() {
        for col in rg.columns() {
            let Some(stats) = col.statistics() else {
                continue;
            };
            let Some(range) = column_range_from_parquet_stats(stats) else {
                continue;
            };
            let name = col.column_descr().name().to_string();
            match column_ranges.get_mut(&name) {
                Some(existing) => merge_column_ranges(existing, &range),
                None => {
                    column_ranges.insert(name, range);
                }
            }
        }
    }
    Ok(MetadataCacheEntry {
        inserted_at: SystemTime::now(),
        identity,
        schema,
        stats: ParquetFileStats {
            path: path.to_string(),
            size_bytes,
            row_count,
            column_ranges,
        },
    })
}

fn get_or_load_block_batches(path: &str, settings: &CacheSettings) -> Result<Vec<RecordBatch>> {
    let identity = file_identity(path)?;
    if settings.block_enabled {
        let now = SystemTime::now();
        if let Some(hit) = block_cache()
            .read()
            .ok()
            .and_then(|cache| cache.get(path).cloned())
            .filter(|entry| {
                entry.identity.size_bytes == identity.size_bytes
                    && entry.identity.mtime_ns == identity.mtime_ns
                    && now
                        .duration_since(entry.inserted_at)
                        .map(|age| age <= settings.ttl)
                        .unwrap_or(false)
            })
        {
            let _ = &hit.source_schema;
            global_metrics().inc_file_cache_event("block", true);
            return Ok(hit.full_batches);
        }
        global_metrics().inc_file_cache_event("block", false);
    }

    let batches = load_full_batches(path)?;
    if settings.block_enabled {
        if let Ok(mut cache) = block_cache().write() {
            evict_cache_map(&mut cache, settings.ttl, settings.block_max_entries);
            cache.insert(
                path.to_string(),
                BlockCacheEntry {
                    inserted_at: SystemTime::now(),
                    identity,
                    source_schema: batches
                        .first()
                        .map(|b| b.schema())
                        .unwrap_or_else(|| Arc::new(Schema::empty())),
                    full_batches: batches.clone(),
                },
            );
        }
    }
    Ok(batches)
}

fn load_full_batches(path: &str) -> Result<Vec<RecordBatch>> {
    let file = File::open(path).map_err(|e| {
        FfqError::Execution(format!("parquet scan open failed for '{}': {e}", path))
    })?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| FfqError::Execution(format!("parquet reader build failed: {e}")))?
        .build()
        .map_err(|e| FfqError::Execution(format!("parquet reader open failed: {e}")))?;
    let mut out = Vec::new();
    for batch in reader {
        out.push(batch.map_err(|e| FfqError::Execution(format!("parquet decode failed: {e}")))?);
    }
    Ok(out)
}

fn evict_cache_map<T>(cache: &mut HashMap<String, T>, _ttl: Duration, max_entries: usize) {
    while cache.len() >= max_entries {
        let Some(k) = cache.keys().next().cloned() else {
            break;
        };
        cache.remove(&k);
    }
}

fn merge_schemas(
    base: &Schema,
    next: &Schema,
    path: &str,
    permissive_merge: bool,
) -> Result<Schema> {
    if base.fields().len() != next.fields().len() {
        return Err(FfqError::InvalidConfig(format!(
            "incompatible parquet files: schema mismatch across table paths; '{}' has {} fields but expected {}",
            path,
            next.fields().len(),
            base.fields().len()
        )));
    }

    let mut fields = Vec::with_capacity(base.fields().len());
    for (idx, (left, right)) in base.fields().iter().zip(next.fields().iter()).enumerate() {
        fields.push(merge_field(
            left.as_ref(),
            right.as_ref(),
            idx,
            path,
            permissive_merge,
        )?);
    }

    Ok(Schema::new_with_metadata(fields, base.metadata().clone()))
}

fn merge_field(
    left: &Field,
    right: &Field,
    idx: usize,
    path: &str,
    permissive_merge: bool,
) -> Result<Field> {
    if left.name() != right.name() {
        return Err(FfqError::InvalidConfig(format!(
            "incompatible parquet files: field-name mismatch at field {idx}; '{}' has name '{}' but expected '{}' from first schema",
            path,
            right.name(),
            left.name()
        )));
    }

    let dt = merge_data_types(
        left.data_type(),
        right.data_type(),
        left.name(),
        path,
        permissive_merge,
    )?;
    let nullable = left.is_nullable() || right.is_nullable();
    Ok(Field::new(left.name(), dt, nullable).with_metadata(left.metadata().clone()))
}

fn merge_data_types(
    left: &DataType,
    right: &DataType,
    field: &str,
    path: &str,
    permissive_merge: bool,
) -> Result<DataType> {
    if left == right {
        return Ok(left.clone());
    }

    if permissive_merge {
        if let Some(widened) = widen_numeric_type(left, right) {
            return Ok(widened);
        }
    }

    if !permissive_merge {
        return Err(FfqError::InvalidConfig(format!(
            "incompatible parquet files: field-type mismatch at '{field}' under strict policy; '{}' has type {:?} but expected {:?}",
            path, right, left
        )));
    }

    if let Some(widened) = widen_numeric_type(left, right) {
        return Ok(widened);
    }

    Err(FfqError::InvalidConfig(format!(
        "incompatible parquet files: field-type mismatch at '{field}'; '{}' has type {:?} but expected {:?}",
        path, right, left
    )))
}

fn widen_numeric_type(left: &DataType, right: &DataType) -> Option<DataType> {
    if !(is_numeric_type(left) && is_numeric_type(right)) {
        return None;
    }

    if matches!(left, DataType::Float64) || matches!(right, DataType::Float64) {
        return Some(DataType::Float64);
    }
    if matches!(left, DataType::Float32) || matches!(right, DataType::Float32) {
        return Some(DataType::Float64);
    }

    match (numeric_family(left), numeric_family(right)) {
        (Some(NumericFamily::Signed(a)), Some(NumericFamily::Signed(b))) => {
            Some(signed_type(a.max(b)))
        }
        (Some(NumericFamily::Unsigned(a)), Some(NumericFamily::Unsigned(b))) => {
            Some(unsigned_type(a.max(b)))
        }
        (Some(NumericFamily::Signed(_)), Some(NumericFamily::Unsigned(_)))
        | (Some(NumericFamily::Unsigned(_)), Some(NumericFamily::Signed(_))) => {
            Some(DataType::Float64)
        }
        _ => None,
    }
}

fn is_numeric_type(dt: &DataType) -> bool {
    numeric_family(dt).is_some() || matches!(dt, DataType::Float32 | DataType::Float64)
}

#[derive(Debug, Clone, Copy)]
enum NumericFamily {
    Signed(u8),
    Unsigned(u8),
}

fn numeric_family(dt: &DataType) -> Option<NumericFamily> {
    match dt {
        DataType::Int8 => Some(NumericFamily::Signed(8)),
        DataType::Int16 => Some(NumericFamily::Signed(16)),
        DataType::Int32 => Some(NumericFamily::Signed(32)),
        DataType::Int64 => Some(NumericFamily::Signed(64)),
        DataType::UInt8 => Some(NumericFamily::Unsigned(8)),
        DataType::UInt16 => Some(NumericFamily::Unsigned(16)),
        DataType::UInt32 => Some(NumericFamily::Unsigned(32)),
        DataType::UInt64 => Some(NumericFamily::Unsigned(64)),
        _ => None,
    }
}

fn signed_type(bits: u8) -> DataType {
    match bits {
        0..=8 => DataType::Int8,
        9..=16 => DataType::Int16,
        17..=32 => DataType::Int32,
        _ => DataType::Int64,
    }
}

fn unsigned_type(bits: u8) -> DataType {
    match bits {
        0..=8 => DataType::UInt8,
        9..=16 => DataType::UInt16,
        17..=32 => DataType::UInt32,
        _ => DataType::UInt64,
    }
}

impl StorageProvider for ParquetProvider {
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
        filters: Vec<Expr>,
    ) -> Result<StorageExecNode> {
        if table.format.to_lowercase() != "parquet" {
            return Err(FfqError::Unsupported(format!(
                "format not supported by ParquetProvider: {}",
                table.format
            )));
        }

        let cache_settings = CacheSettings::from_table(table);
        let all_paths = table.data_paths()?;
        let partition_columns = table.partition_columns();
        let partition_layout = table.partition_layout();
        let partition_pruned_paths =
            if partition_columns.is_empty() || partition_layout != "hive" || filters.is_empty() {
                all_paths
            } else {
                prune_partition_paths_hive(&all_paths, &partition_columns, &filters)
            };
        let file_stats = read_parquet_file_stats_metadata(table).unwrap_or_default();
        let paths = if filters.is_empty() || file_stats.is_empty() {
            partition_pruned_paths
        } else {
            prune_paths_with_file_stats(&partition_pruned_paths, &filters, &file_stats)
        };
        let source_schema = match &table.schema {
            Some(s) => Arc::new(s.clone()),
            None => Arc::new(Self::infer_parquet_schema(&paths)?),
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
            let indices = (0..source_schema.fields().len()).collect::<Vec<_>>();
            (source_schema.clone(), indices)
        };

        Ok(Arc::new(ParquetScanNode {
            paths,
            schema,
            source_schema,
            projection_indices,
            filters,
            cache_settings,
        }))
    }
}

/// Execution node that scans parquet files and emits Arrow record batches.
pub struct ParquetScanNode {
    paths: Vec<String>,
    schema: SchemaRef,
    source_schema: SchemaRef,
    projection_indices: Vec<usize>,
    filters: Vec<Expr>,
    cache_settings: CacheSettings,
}

impl ExecNode for ParquetScanNode {
    fn name(&self) -> &'static str {
        "ParquetScanNode"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        // v1 embedded path: read local parquet files eagerly and stream batches.
        let mut out = Vec::<Result<RecordBatch>>::new();
        let _ = &self.filters;
        for path in &self.paths {
            let full_batches = get_or_load_block_batches(path, &self.cache_settings)?;
            for batch in full_batches {
                if batch.schema().fields().len() != self.source_schema.fields().len() {
                    return Err(FfqError::Execution(format!(
                        "parquet scan schema mismatch for '{}': expected {} columns, got {}",
                        path,
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
                        FfqError::Execution(format!("parquet projection failed: {e}"))
                    }),
                );
            }
        }

        Ok(Box::pin(StreamAdapter::new(
            self.schema.clone(),
            futures::stream::iter(out),
        )))
    }
}

#[derive(Debug, Clone, PartialEq)]
enum PartitionScalar {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tri {
    True,
    False,
    Unknown,
}

fn prune_partition_paths_hive(
    paths: &[String],
    partition_columns: &[String],
    filters: &[Expr],
) -> Vec<String> {
    paths
        .iter()
        .filter(|path| {
            let values = parse_hive_partition_values(path, partition_columns);
            !filters
                .iter()
                .any(|f| matches!(eval_partition_predicate(f, &values), Tri::False))
        })
        .cloned()
        .collect::<Vec<_>>()
}

fn parse_hive_partition_values(
    path: &str,
    partition_columns: &[String],
) -> HashMap<String, PartitionScalar> {
    let mut out = HashMap::new();
    for segment in path.split('/') {
        let Some((k, raw_v)) = segment.split_once('=') else {
            continue;
        };
        let key = k.trim();
        if !partition_columns.iter().any(|c| c == key) {
            continue;
        }
        let value = if raw_v.eq_ignore_ascii_case("true") {
            PartitionScalar::Bool(true)
        } else if raw_v.eq_ignore_ascii_case("false") {
            PartitionScalar::Bool(false)
        } else if let Ok(v) = raw_v.parse::<i64>() {
            PartitionScalar::Int(v)
        } else if let Ok(v) = raw_v.parse::<f64>() {
            PartitionScalar::Float(v)
        } else {
            PartitionScalar::Str(raw_v.to_string())
        };
        out.insert(key.to_string(), value);
    }
    out
}

fn eval_partition_predicate(expr: &Expr, values: &HashMap<String, PartitionScalar>) -> Tri {
    match expr {
        Expr::And(l, r) => match (
            eval_partition_predicate(l, values),
            eval_partition_predicate(r, values),
        ) {
            (Tri::False, _) | (_, Tri::False) => Tri::False,
            (Tri::True, Tri::True) => Tri::True,
            _ => Tri::Unknown,
        },
        Expr::Or(l, r) => match (
            eval_partition_predicate(l, values),
            eval_partition_predicate(r, values),
        ) {
            (Tri::True, _) | (_, Tri::True) => Tri::True,
            (Tri::False, Tri::False) => Tri::False,
            _ => Tri::Unknown,
        },
        Expr::Not(inner) => match eval_partition_predicate(inner, values) {
            Tri::True => Tri::False,
            Tri::False => Tri::True,
            Tri::Unknown => Tri::Unknown,
        },
        Expr::BinaryOp { left, op, right } => eval_partition_binary(left, *op, right, values),
        _ => Tri::Unknown,
    }
}

fn eval_partition_binary(
    left: &Expr,
    op: BinaryOp,
    right: &Expr,
    values: &HashMap<String, PartitionScalar>,
) -> Tri {
    if let (Some((col, lit)), false) = (
        column_and_literal(left, right),
        matches!(
            op,
            BinaryOp::Plus | BinaryOp::Minus | BinaryOp::Multiply | BinaryOp::Divide
        ),
    ) {
        return eval_partition_comparison(col, op, lit, values);
    }
    if let (Some((col, lit)), false) = (
        column_and_literal(right, left),
        matches!(
            op,
            BinaryOp::Plus | BinaryOp::Minus | BinaryOp::Multiply | BinaryOp::Divide
        ),
    ) {
        let swapped = match op {
            BinaryOp::Lt => BinaryOp::Gt,
            BinaryOp::LtEq => BinaryOp::GtEq,
            BinaryOp::Gt => BinaryOp::Lt,
            BinaryOp::GtEq => BinaryOp::LtEq,
            other => other,
        };
        return eval_partition_comparison(col, swapped, lit, values);
    }
    Tri::Unknown
}

fn column_and_literal<'a>(
    col_expr: &'a Expr,
    lit_expr: &'a Expr,
) -> Option<(&'a str, &'a LiteralValue)> {
    let col = match col_expr {
        Expr::Column(name) => name.as_str(),
        Expr::ColumnRef { name, .. } => name.as_str(),
        _ => return None,
    };
    let lit = match lit_expr {
        Expr::Literal(v) => v,
        _ => return None,
    };
    Some((col, lit))
}

fn eval_partition_comparison(
    column: &str,
    op: BinaryOp,
    literal: &LiteralValue,
    values: &HashMap<String, PartitionScalar>,
) -> Tri {
    let Some(partition_value) = values.get(column) else {
        return Tri::Unknown;
    };
    let Some(cmp) = compare_partition_value(partition_value, literal) else {
        return Tri::Unknown;
    };
    let matched = match op {
        BinaryOp::Eq => cmp == 0,
        BinaryOp::NotEq => cmp != 0,
        BinaryOp::Lt => cmp < 0,
        BinaryOp::LtEq => cmp <= 0,
        BinaryOp::Gt => cmp > 0,
        BinaryOp::GtEq => cmp >= 0,
        _ => return Tri::Unknown,
    };
    if matched { Tri::True } else { Tri::False }
}

fn compare_partition_value(left: &PartitionScalar, right: &LiteralValue) -> Option<i8> {
    match (left, right) {
        (PartitionScalar::Str(a), LiteralValue::Utf8(b)) => Some(ordering_to_i8(a.cmp(b))),
        (PartitionScalar::Int(a), LiteralValue::Int64(b)) => Some(ordering_to_i8(a.cmp(b))),
        (PartitionScalar::Float(a), LiteralValue::Float64(b)) => {
            a.partial_cmp(b).map(ordering_to_i8)
        }
        (PartitionScalar::Int(a), LiteralValue::Float64(b)) => {
            (*a as f64).partial_cmp(b).map(ordering_to_i8)
        }
        (PartitionScalar::Float(a), LiteralValue::Int64(b)) => {
            a.partial_cmp(&(*b as f64)).map(ordering_to_i8)
        }
        (PartitionScalar::Bool(a), LiteralValue::Boolean(b)) => Some(ordering_to_i8(a.cmp(b))),
        _ => None,
    }
}

fn ordering_to_i8(ord: std::cmp::Ordering) -> i8 {
    match ord {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

fn column_range_from_parquet_stats(stats: &ParquetStatistics) -> Option<ColumnRangeStats> {
    match stats {
        ParquetStatistics::Boolean(s) => Some(ColumnRangeStats {
            min: ScalarStatValue::Bool(*s.min_opt()?),
            max: ScalarStatValue::Bool(*s.max_opt()?),
        }),
        ParquetStatistics::Int32(s) => Some(ColumnRangeStats {
            min: ScalarStatValue::Int64(*s.min_opt()? as i64),
            max: ScalarStatValue::Int64(*s.max_opt()? as i64),
        }),
        ParquetStatistics::Int64(s) => Some(ColumnRangeStats {
            min: ScalarStatValue::Int64(*s.min_opt()?),
            max: ScalarStatValue::Int64(*s.max_opt()?),
        }),
        ParquetStatistics::Float(s) => Some(ColumnRangeStats {
            min: ScalarStatValue::Float64(*s.min_opt()? as f64),
            max: ScalarStatValue::Float64(*s.max_opt()? as f64),
        }),
        ParquetStatistics::Double(s) => Some(ColumnRangeStats {
            min: ScalarStatValue::Float64(*s.min_opt()?),
            max: ScalarStatValue::Float64(*s.max_opt()?),
        }),
        ParquetStatistics::ByteArray(s) => {
            let min = std::str::from_utf8(s.min_opt()?.data()).ok()?.to_string();
            let max = std::str::from_utf8(s.max_opt()?.data()).ok()?.to_string();
            Some(ColumnRangeStats {
                min: ScalarStatValue::Utf8(min),
                max: ScalarStatValue::Utf8(max),
            })
        }
        ParquetStatistics::FixedLenByteArray(s) => {
            let min = std::str::from_utf8(s.min_opt()?.data()).ok()?.to_string();
            let max = std::str::from_utf8(s.max_opt()?.data()).ok()?.to_string();
            Some(ColumnRangeStats {
                min: ScalarStatValue::Utf8(min),
                max: ScalarStatValue::Utf8(max),
            })
        }
        ParquetStatistics::Int96(_) => None,
    }
}

fn merge_column_ranges(current: &mut ColumnRangeStats, incoming: &ColumnRangeStats) {
    if scalar_stat_cmp(&incoming.min, &current.min).is_some_and(|ord| matches!(ord, Ordering::Less))
    {
        current.min = incoming.min.clone();
    }
    if scalar_stat_cmp(&incoming.max, &current.max)
        .is_some_and(|ord| matches!(ord, Ordering::Greater))
    {
        current.max = incoming.max.clone();
    }
}

fn scalar_stat_cmp(left: &ScalarStatValue, right: &ScalarStatValue) -> Option<Ordering> {
    match (left, right) {
        (ScalarStatValue::Int64(a), ScalarStatValue::Int64(b)) => Some(a.cmp(b)),
        (ScalarStatValue::Float64(a), ScalarStatValue::Float64(b)) => a.partial_cmp(b),
        (ScalarStatValue::Int64(a), ScalarStatValue::Float64(b)) => (*a as f64).partial_cmp(b),
        (ScalarStatValue::Float64(a), ScalarStatValue::Int64(b)) => a.partial_cmp(&(*b as f64)),
        (ScalarStatValue::Bool(a), ScalarStatValue::Bool(b)) => Some(a.cmp(b)),
        (ScalarStatValue::Utf8(a), ScalarStatValue::Utf8(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

fn read_parquet_file_stats_metadata(table: &TableDef) -> Option<Vec<ParquetFileStats>> {
    let raw = table.options.get("stats.parquet.files")?;
    serde_json::from_str(raw).ok()
}

fn prune_paths_with_file_stats(
    paths: &[String],
    filters: &[Expr],
    file_stats: &[ParquetFileStats],
) -> Vec<String> {
    let by_path = file_stats
        .iter()
        .map(|s| (s.path.as_str(), s))
        .collect::<HashMap<_, _>>();
    paths
        .iter()
        .filter(|path| {
            let Some(stats) = by_path.get(path.as_str()) else {
                return true;
            };
            !filters.iter().any(|f| {
                matches!(
                    eval_file_stats_predicate(f, &stats.column_ranges),
                    Tri::False
                )
            })
        })
        .cloned()
        .collect::<Vec<_>>()
}

fn eval_file_stats_predicate(expr: &Expr, ranges: &HashMap<String, ColumnRangeStats>) -> Tri {
    match expr {
        Expr::And(l, r) => match (
            eval_file_stats_predicate(l, ranges),
            eval_file_stats_predicate(r, ranges),
        ) {
            (Tri::False, _) | (_, Tri::False) => Tri::False,
            (Tri::True, Tri::True) => Tri::True,
            _ => Tri::Unknown,
        },
        Expr::Or(l, r) => match (
            eval_file_stats_predicate(l, ranges),
            eval_file_stats_predicate(r, ranges),
        ) {
            (Tri::True, _) | (_, Tri::True) => Tri::True,
            (Tri::False, Tri::False) => Tri::False,
            _ => Tri::Unknown,
        },
        Expr::Not(inner) => match eval_file_stats_predicate(inner, ranges) {
            Tri::True => Tri::False,
            Tri::False => Tri::True,
            Tri::Unknown => Tri::Unknown,
        },
        Expr::BinaryOp { left, op, right } => eval_file_stats_binary(left, *op, right, ranges),
        _ => Tri::Unknown,
    }
}

fn eval_file_stats_binary(
    left: &Expr,
    op: BinaryOp,
    right: &Expr,
    ranges: &HashMap<String, ColumnRangeStats>,
) -> Tri {
    if let Some((column, lit)) = column_and_literal(left, right) {
        return eval_file_range(column, op, lit, ranges);
    }
    if let Some((column, lit)) = column_and_literal(right, left) {
        let swapped = match op {
            BinaryOp::Lt => BinaryOp::Gt,
            BinaryOp::LtEq => BinaryOp::GtEq,
            BinaryOp::Gt => BinaryOp::Lt,
            BinaryOp::GtEq => BinaryOp::LtEq,
            other => other,
        };
        return eval_file_range(column, swapped, lit, ranges);
    }
    Tri::Unknown
}

fn eval_file_range(
    column: &str,
    op: BinaryOp,
    literal: &LiteralValue,
    ranges: &HashMap<String, ColumnRangeStats>,
) -> Tri {
    let Some(range) = ranges.get(column) else {
        return Tri::Unknown;
    };
    let min_cmp = compare_scalar_stat_literal(&range.min, literal);
    let max_cmp = compare_scalar_stat_literal(&range.max, literal);
    match op {
        BinaryOp::Eq => match (min_cmp, max_cmp) {
            (Some(min), Some(max)) if min == 1 || max == -1 => Tri::False,
            _ => Tri::Unknown,
        },
        BinaryOp::NotEq => match (min_cmp, max_cmp, scalar_stat_cmp(&range.min, &range.max)) {
            (Some(0), Some(0), Some(Ordering::Equal)) => Tri::False,
            _ => Tri::Unknown,
        },
        BinaryOp::Lt => match min_cmp {
            Some(ord) if ord >= 0 => Tri::False,
            _ => Tri::Unknown,
        },
        BinaryOp::LtEq => match min_cmp {
            Some(ord) if ord == 1 => Tri::False,
            _ => Tri::Unknown,
        },
        BinaryOp::Gt => match max_cmp {
            Some(ord) if ord <= 0 => Tri::False,
            _ => Tri::Unknown,
        },
        BinaryOp::GtEq => match max_cmp {
            Some(ord) if ord == -1 => Tri::False,
            _ => Tri::Unknown,
        },
        _ => Tri::Unknown,
    }
}

fn compare_scalar_stat_literal(left: &ScalarStatValue, right: &LiteralValue) -> Option<i8> {
    let ord = match (left, right) {
        (ScalarStatValue::Int64(a), LiteralValue::Int64(b)) => a.cmp(b),
        (ScalarStatValue::Float64(a), LiteralValue::Float64(b)) => a.partial_cmp(b)?,
        (ScalarStatValue::Int64(a), LiteralValue::Float64(b)) => (*a as f64).partial_cmp(b)?,
        (ScalarStatValue::Float64(a), LiteralValue::Int64(b)) => a.partial_cmp(&(*b as f64))?,
        (ScalarStatValue::Bool(a), LiteralValue::Boolean(b)) => a.cmp(b),
        (ScalarStatValue::Utf8(a), LiteralValue::Utf8(b)) => a.cmp(b),
        _ => return None,
    };
    Some(ordering_to_i8(ord))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use arrow::array::ArrayRef;
    use arrow::array::{Float32Array, Int32Array, Int64Array};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::DataType;
    use ffq_common::metrics::global_metrics;
    use futures::TryStreamExt;
    use parquet::arrow::ArrowWriter;

    use super::*;
    use crate::TableStats;

    fn fixture_path(file: &str) -> String {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        root.join("../../tests/fixtures/parquet")
            .join(file)
            .to_string_lossy()
            .to_string()
    }

    #[test]
    fn infer_parquet_schema_from_fixture_file() {
        let paths = vec![fixture_path("lineitem.parquet")];
        let schema = ParquetProvider::infer_parquet_schema(&paths).expect("infer schema");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "l_orderkey");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).name(), "l_partkey");
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
    }

    #[test]
    fn parquet_scan_infers_schema_when_missing_in_catalog() {
        let provider = ParquetProvider::new();
        let table = TableDef {
            name: "lineitem".to_string(),
            uri: fixture_path("lineitem.parquet"),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: None,
            stats: TableStats::default(),
            options: HashMap::new(),
        };
        let node = provider
            .scan(&table, None, Vec::new())
            .expect("scan should infer schema");
        let schema = node.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "l_orderkey");
    }

    #[test]
    fn infer_parquet_schema_rejects_incompatible_files() {
        let paths = vec![
            fixture_path("lineitem.parquet"),
            fixture_path("orders.parquet"),
        ];
        let err = ParquetProvider::infer_parquet_schema(&paths).expect_err("must reject");
        let msg = format!("{err}");
        assert!(msg.contains("incompatible parquet files"));
        assert!(msg.contains("field 0"));
    }

    #[test]
    fn infer_parquet_schema_widens_numeric_and_nullable() {
        let p1 = unique_path("merge_int32", "parquet");
        let p2 = unique_path("merge_int64_nullable", "parquet");

        write_parquet_file(
            &p1,
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1_i32, 2])) as ArrayRef],
        );
        write_parquet_file(
            &p2,
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![Some(3_i64), None])) as ArrayRef],
        );

        let paths = vec![
            p1.to_string_lossy().to_string(),
            p2.to_string_lossy().to_string(),
        ];
        let schema = ParquetProvider::infer_parquet_schema(&paths).expect("merge schema");
        assert_eq!(schema.field(0).name(), "v");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert!(schema.field(0).is_nullable());

        let _ = std::fs::remove_file(p1);
        let _ = std::fs::remove_file(p2);
    }

    #[test]
    fn infer_parquet_schema_widens_int_and_float_to_float64() {
        let p1 = unique_path("merge_int64", "parquet");
        let p2 = unique_path("merge_float32", "parquet");

        write_parquet_file(
            &p1,
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2])) as ArrayRef],
        );
        write_parquet_file(
            &p2,
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float32, false)])),
            vec![Arc::new(Float32Array::from(vec![1.25_f32, 2.5])) as ArrayRef],
        );

        let paths = vec![
            p1.to_string_lossy().to_string(),
            p2.to_string_lossy().to_string(),
        ];
        let schema = ParquetProvider::infer_parquet_schema(&paths).expect("merge schema");
        assert_eq!(schema.field(0).name(), "v");
        assert_eq!(schema.field(0).data_type(), &DataType::Float64);

        let _ = std::fs::remove_file(p1);
        let _ = std::fs::remove_file(p2);
    }

    #[test]
    fn partition_pruning_hive_matches_eq_and_range_filters() {
        let paths = vec![
            "/tmp/t/ds=2025-01-01/region=us/part-0.parquet".to_string(),
            "/tmp/t/ds=2025-01-02/region=eu/part-1.parquet".to_string(),
            "/tmp/t/ds=2025-01-03/region=us/part-2.parquet".to_string(),
        ];
        let filters = vec![
            Expr::BinaryOp {
                left: Box::new(Expr::Column("region".to_string())),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Literal(LiteralValue::Utf8("us".to_string()))),
            },
            Expr::BinaryOp {
                left: Box::new(Expr::Column("ds".to_string())),
                op: BinaryOp::GtEq,
                right: Box::new(Expr::Literal(LiteralValue::Utf8("2025-01-02".to_string()))),
            },
        ];
        let pruned =
            prune_partition_paths_hive(&paths, &["ds".to_string(), "region".to_string()], &filters);
        assert_eq!(
            pruned,
            vec!["/tmp/t/ds=2025-01-03/region=us/part-2.parquet".to_string()]
        );
    }

    #[test]
    fn partition_pruning_keeps_paths_for_unknown_predicates() {
        let paths = vec![
            "/tmp/t/ds=2025-01-01/part-0.parquet".to_string(),
            "/tmp/t/ds=2025-01-02/part-1.parquet".to_string(),
        ];
        let filters = vec![Expr::BinaryOp {
            left: Box::new(Expr::Column("non_partition_col".to_string())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(LiteralValue::Int64(1))),
        }];
        let pruned = prune_partition_paths_hive(&paths, &["ds".to_string()], &filters);
        assert_eq!(pruned, paths);
    }

    #[test]
    fn collect_parquet_file_stats_extracts_rows_size_and_min_max() {
        let p = unique_path("stats_collect", "parquet");
        write_parquet_file(
            &p,
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![2_i64, 9, 4])) as ArrayRef],
        );
        let paths = vec![p.to_string_lossy().to_string()];
        let stats = ParquetProvider::collect_parquet_file_stats(&paths).expect("collect stats");
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].row_count, 3);
        assert!(stats[0].size_bytes > 0);
        let v = stats[0].column_ranges.get("v").expect("range");
        assert_eq!(v.min, ScalarStatValue::Int64(2));
        assert_eq!(v.max, ScalarStatValue::Int64(9));
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn file_stats_pruning_rejects_files_outside_range() {
        let paths = vec![
            "/tmp/t/a.parquet".to_string(),
            "/tmp/t/b.parquet".to_string(),
            "/tmp/t/c.parquet".to_string(),
        ];
        let stats = vec![
            ParquetFileStats {
                path: "/tmp/t/a.parquet".to_string(),
                size_bytes: 1,
                row_count: 1,
                column_ranges: HashMap::from([(
                    "x".to_string(),
                    ColumnRangeStats {
                        min: ScalarStatValue::Int64(1),
                        max: ScalarStatValue::Int64(5),
                    },
                )]),
            },
            ParquetFileStats {
                path: "/tmp/t/b.parquet".to_string(),
                size_bytes: 1,
                row_count: 1,
                column_ranges: HashMap::from([(
                    "x".to_string(),
                    ColumnRangeStats {
                        min: ScalarStatValue::Int64(8),
                        max: ScalarStatValue::Int64(10),
                    },
                )]),
            },
            ParquetFileStats {
                path: "/tmp/t/c.parquet".to_string(),
                size_bytes: 1,
                row_count: 1,
                column_ranges: HashMap::from([(
                    "x".to_string(),
                    ColumnRangeStats {
                        min: ScalarStatValue::Int64(12),
                        max: ScalarStatValue::Int64(15),
                    },
                )]),
            },
        ];
        let filters = vec![Expr::BinaryOp {
            left: Box::new(Expr::Column("x".to_string())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(LiteralValue::Int64(9))),
        }];
        let pruned = prune_paths_with_file_stats(&paths, &filters, &stats);
        assert_eq!(pruned, vec!["/tmp/t/b.parquet".to_string()]);
    }

    #[test]
    fn block_cache_records_miss_then_hit_events() {
        let p = unique_path("block_cache", "parquet");
        write_parquet_file(
            &p,
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef],
        );
        let mut options = HashMap::new();
        options.insert("cache.block.enabled".to_string(), "true".to_string());
        options.insert("cache.ttl_secs".to_string(), "300".to_string());
        let table = TableDef {
            name: "t".to_string(),
            uri: p.to_string_lossy().to_string(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: None,
            stats: TableStats::default(),
            options,
        };
        let provider = ParquetProvider::new();
        let node = provider.scan(&table, None, Vec::new()).expect("scan node");
        let stream1 = node
            .execute(Arc::new(TaskContext {
                batch_size_rows: 1024,
                mem_budget_bytes: usize::MAX,
            }))
            .expect("execute 1");
        let _b1 = futures::executor::block_on(stream1.try_collect::<Vec<RecordBatch>>())
            .expect("collect 1");
        let stream2 = node
            .execute(Arc::new(TaskContext {
                batch_size_rows: 1024,
                mem_budget_bytes: usize::MAX,
            }))
            .expect("execute 2");
        let _b2 = futures::executor::block_on(stream2.try_collect::<Vec<RecordBatch>>())
            .expect("collect 2");

        let text = global_metrics().render_prometheus();
        assert!(text.contains("ffq_file_cache_events_total"));
        assert!(text.contains("cache_kind=\"block\",result=\"miss\""));
        assert!(text.contains("cache_kind=\"block\",result=\"hit\""));
        let _ = std::fs::remove_file(p);
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
