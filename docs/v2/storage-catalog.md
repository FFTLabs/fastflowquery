# Storage and Catalog (v2 Bootstrap)

- Status: draft
- Owner: @ffq-docs
- Last Verified Commit: TBD
- Last Verified Date: TBD
- Source: inherited/adapted from prior version docs; v2 verification pending


This page documents the bootstrapped v2 storage/catalog behavior in FFQ.

## Scope

v1 storage/catalog provides:
1. `StorageProvider` abstraction for scan + stats.
2. Parquet-backed scan path (`ParquetProvider`) as primary implementation.
3. Optional object-store provider surface (feature `s3`, currently experimental placeholder).
4. Optional qdrant vector index provider surface (feature `qdrant`) for vector top-k path.
5. Persistent catalog in `tables.json` or `tables.toml`.

## StorageProvider Contract

Defined in `crates/storage/src/provider.rs`.

```rust
pub trait StorageProvider: Send + Sync {
    fn estimate_stats(&self, table: &TableDef) -> Stats;

    fn scan(
        &self,
        table: &TableDef,
        projection: Option<Vec<String>>,
        filters: Vec<Expr>,
    ) -> Result<StorageExecNode>;
}
```

Notes:
1. `estimate_stats` is used for planning/heuristics (`rows`, `bytes`).
2. `scan` returns an `ExecNode` that produces Arrow `RecordBatch` stream.
3. Current v1 parquet scan keeps `projection/filters` in node state; aggressive pushdown is limited.

## File-Level Caching (EPIC 8.3)

FFQ now includes a provider-level parquet file cache with two layers:

1. metadata cache (schema + file statistics from parquet metadata)
2. optional block cache (decoded full `RecordBatch` sets per parquet file)

Implementation:

1. `crates/storage/src/parquet_provider.rs` (`CacheSettings`, `METADATA_CACHE`, `BLOCK_CACHE`)
2. `crates/common/src/metrics.rs` (`ffq_file_cache_events_total`)

### Cache behavior

1. Caches are process-local and in-memory.
2. Cache validity checks require both:
   - file identity match (`size_bytes`, `mtime_ns`)
   - TTL freshness (`inserted_at + ttl`)
3. If either check fails, entry is treated as miss and rebuilt.
4. Cache capacity uses bounded entry counts with eviction when max entries are reached.

### Configuration

Environment-level controls:

1. `FFQ_PARQUET_METADATA_CACHE_ENABLED` (`true|false`, default `true`)
2. `FFQ_PARQUET_BLOCK_CACHE_ENABLED` (`true|false`, default `false`)
3. `FFQ_FILE_CACHE_TTL_SECS` (default `300`)
4. `FFQ_PARQUET_METADATA_CACHE_MAX_ENTRIES` (default `4096`)
5. `FFQ_PARQUET_BLOCK_CACHE_MAX_ENTRIES` (default `64`)

Per-table option overrides (for booleans/TTL):

1. `cache.metadata.enabled`
2. `cache.block.enabled`
3. `cache.ttl_secs`

Precedence:

1. environment defaults are loaded first
2. table options override env values for metadata/block enablement and TTL

### Observability (hit ratio)

Cache outcomes are emitted via:

1. `ffq_file_cache_events_total{cache_kind="metadata|block",result="hit|miss"}`

Use this to compute hit ratio:

1. `hits / (hits + misses)` per `cache_kind`

Operational recommendation:

1. start with metadata cache enabled and block cache disabled
2. enable block cache only for repeated scan-heavy workloads with stable files
3. tune TTL and max entries per workload size and memory budget

## Parquet Path (Primary v1 Data Path)

Implemented in `crates/storage/src/parquet_provider.rs`.

Behavior:
1. Validates table format is `parquet`.
2. Resolves input files via `TableDef::data_paths()`:
   - uses `paths` if non-empty,
   - otherwise uses single `uri`,
   - errors if both are empty.
3. Builds a `ParquetScanNode` and reads local parquet files.
4. Streams Arrow record batches to runtime.

Execution integration:
1. Embedded runtime invokes `ParquetProvider::scan(...)` in `crates/client/src/runtime.rs`.
2. Worker runtime invokes the same provider in `crates/distributed/src/worker.rs`.

### Partitioned tables + partition pruning (EPIC 8.1, partial)

Current support includes hive-style partition pruning for parquet path expansion.

Behavior (supported subset):

1. partition values encoded in path segments (for example `.../dt=2026-01-01/country=de/...`)
2. equality and range predicates on partition columns can prune candidate files
3. non-pushdownable predicates fall back to normal scan-time filtering

Evidence:

1. `crates/storage/src/parquet_provider.rs`
2. test `partition_pruning_hive_matches_eq_and_range_filters`

Current limits:

1. partition layout/catalog contracts are still lightweight (not a full metastore model)
2. pruning support is subset-based, not full SQL predicate normalization across all expressions

### Statistics collection (EPIC 8.2, partial)

FFQ exposes two levels of storage stats today:

1. table-level heuristic stats (`TableStats`: `rows`, `bytes`)
2. parquet file metadata stats (`ParquetFileStats`: `row_count`, `size_bytes`, per-column min/max where available)

Where they live:

1. `crates/storage/src/stats.rs`
2. `crates/storage/src/parquet_provider.rs`
3. `crates/storage/src/provider.rs` (`estimate_stats`)

How they are used today:

1. planner/optimizer heuristics (for example join strategy decisions) use table-level estimated rows/bytes
2. parquet metadata extraction supports richer persisted file stats and cache metadata

Current limits:

1. optimizer use of file-level min/max is partial and not a full cost-based framework
2. `EXPLAIN` visibility for all file-level statistics remains limited

## Object Store Behavior (`s3`)

Surface exists behind feature `s3`:
1. `crates/storage/src/object_store_provider.rs`
2. `crates/storage/Cargo.toml` feature `s3`
3. runtime routing in:
   - `crates/client/src/runtime.rs`
   - `crates/distributed/src/worker.rs`

Current behavior:
1. URI-style parquet table paths (`scheme://...`) route to `ObjectStoreProvider`.
2. Local file paths still route to `ParquetProvider`.
3. Object-store scans currently support parquet format.
4. Provider executes resilient object reads with retry + backoff + timeout controls.

### Retry, timeout, multipart-style range fetch

Provider fetch path:
1. performs `head` to discover object size
2. uses full get for small objects
3. uses ranged chunk reads for large objects (`range_chunk_size_bytes`) and reassembles bytes
4. retries transient failures with configured attempt/backoff policy

Config controls:

Environment:
1. `FFQ_OBJECT_STORE_RETRY_ATTEMPTS`
2. `FFQ_OBJECT_STORE_RETRY_BACKOFF_MS`
3. `FFQ_OBJECT_STORE_MAX_CONCURRENCY`
4. `FFQ_OBJECT_STORE_RANGE_CHUNK_SIZE`
5. `FFQ_OBJECT_STORE_TIMEOUT_SECS`
6. `FFQ_OBJECT_STORE_CONNECT_TIMEOUT_SECS`

Table options:
1. `object_store.retry_attempts`
2. `object_store.retry_backoff_ms`
3. `object_store.max_concurrency`
4. `object_store.range_chunk_size_bytes`
5. `object_store.timeout_secs`
6. `object_store.connect_timeout_secs`

Credential/config chain:
1. Any `object_store.<key>=<value>` option is forwarded to `object_store::parse_url_opts`.
2. Provider-specific keys for S3/GCS/Azure can be set in table options or standard environment variables used by the underlying object-store SDK path.

Operational guidance:
1. start with moderate retries (`3`) and short backoff (`250ms`)
2. set `range_chunk_size_bytes` based on network characteristics
3. tune `max_concurrency` to avoid read amplification and memory spikes

## Optional Qdrant Behavior (`qdrant`)

Vector index provider surface:
- Trait: `crates/storage/src/vector_index.rs`
- Implementation: `crates/storage/src/qdrant_provider.rs`
- Feature gate: `crates/storage/Cargo.toml` -> `qdrant`

Behavior:
1. `QdrantProvider::from_table` reads options from `TableDef.options`, including:
   - `qdrant.endpoint`
   - `qdrant.collection`
   - `qdrant.with_payload`
2. `topk(query_vec, k, filter)` executes Qdrant search and returns rows:
   - `id`
   - `score`
   - optional `payload_json`
3. Optional JSON-encoded filter payload is supported for the planner pushdown subset.

Note: this path is used by vector execution operators and optimizer rewrites; it is not a generic parquet replacement.

## Catalog Model

Catalog is implemented in `crates/storage/src/catalog.rs`.

### `TableDef` schema

```rust
pub struct TableDef {
    pub name: String,
    pub uri: String,
    pub paths: Vec<String>,
    pub format: String,
    pub schema: Option<Schema>,
    pub stats: TableStats,
    pub options: HashMap<String, String>,
}
```

Field intent:
1. `name`: table identifier in SQL/API.
2. `uri`/`paths`: physical location(s); `paths` takes precedence.
3. `format`: storage format/provider selector (`parquet`, `qdrant`, etc.).
4. `schema`: optional persisted Arrow schema; if missing for parquet, inference policy controls whether planning can infer it.
5. `stats`: optional lightweight stats (`rows`, `bytes`) for planning heuristics.
6. `options`: provider-specific options (for example qdrant connection metadata).

### Catalog operations

Key methods:
1. `register_table(table)`
2. `get(name)`
3. `load(path)` for `.json` or `.toml`
4. `save(path)` for `.json` or `.toml`

Format detection is extension-based:
1. `.json` -> JSON loader/saver
2. `.toml` -> TOML loader/saver
3. other/no extension -> invalid config error

### Persistence model (`tables.json` / `tables.toml`)

Supported on load:
1. Bare list: `[ {table...}, ... ]`
2. Wrapped object:
   - JSON: `{ "tables": [ ... ] }`
   - TOML: `[[tables]] ...`

Save behavior:
1. Saves as wrapped form (`tables = [...]`).
2. Uses atomic-style commit flow (`write_atomically`) with staged temp file and backup rename.
3. Protects against partial catalog overwrite on failed rename/commit.

## Registration and Query Examples

### Example 1: manual-schema flow (explicit schema in catalog/register)

```rust
use ffq_client::Engine;
use ffq_common::EngineConfig;
use ffq_storage::TableDef;

let engine = Engine::new(EngineConfig::default())?;

engine.register_table(
    "lineitem",
    TableDef {
        name: "lineitem".to_string(),
        uri: "./data/lineitem.parquet".to_string(),
        paths: vec![],
        format: "parquet".to_string(),
        schema: Some(Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
        ])),
        stats: Default::default(),
        options: Default::default(),
    },
);

let rows = engine
    .sql("SELECT l_orderkey FROM lineitem LIMIT 10")?
    .collect()
    .await?;
```

### Example 2: inferred-schema flow (schema omitted)

```rust
let mut cfg = EngineConfig::default();
cfg.schema_inference = ffq_common::SchemaInferencePolicy::On;
cfg.schema_drift_policy = ffq_common::SchemaDriftPolicy::Refresh;
let engine = Engine::new(cfg)?;

engine.register_table(
    "lineitem",
    TableDef {
        name: "lineitem".to_string(),
        uri: "./data/lineitem.parquet".to_string(),
        paths: vec![],
        format: "parquet".to_string(),
        schema: None, // inferred from parquet footer
        stats: Default::default(),
        options: Default::default(),
    },
);

let rows = engine
    .sql("SELECT l_orderkey FROM lineitem LIMIT 10")?
    .collect()
    .await?;
```

### Example 3: multi-file parquet table via `paths`

```rust
engine.register_table(
    "events",
    TableDef {
        name: "events".to_string(),
        uri: String::new(),
        paths: vec![
            "./data/events/part-000.parquet".to_string(),
            "./data/events/part-001.parquet".to_string(),
        ],
        format: "parquet".to_string(),
        schema: None,
        stats: Default::default(),
        options: Default::default(),
    },
);
```

## Restart Persistence Behavior

Session startup (`crates/client/src/session.rs`):
1. Reads `FFQ_CATALOG_PATH` (default: `./ffq_tables/tables.json`).
2. If file exists, loads catalog via `Catalog::load(...)`.
3. Otherwise starts with empty catalog.

Catalog update persistence:
1. Write-oriented APIs (for example `save_as_table`) update catalog in memory.
2. `Session::persist_catalog()` writes catalog back to configured file.
3. On next engine/session start, saved tables are reloaded and queryable.

Operational guidance:
1. Keep `FFQ_CATALOG_PATH` stable across restarts.
2. Use `.json` or `.toml` extension explicitly.
3. Treat catalog file as source of truth for table registration continuity.

## Schema Inference Policies (SCH-08)

`EngineConfig` now exposes three explicit schema policy controls:

1. `schema_inference = off|on|strict|permissive`
2. `schema_writeback = true|false`
3. `schema_drift_policy = fail|refresh`

Environment override surface:

1. `FFQ_SCHEMA_INFERENCE`
2. `FFQ_SCHEMA_WRITEBACK`
3. `FFQ_SCHEMA_DRIFT_POLICY`

Behavior contract:

1. `off`: parquet tables without `schema` do not infer and later planning fails with a clear missing-schema error.
2. `on`: inference enabled, permissive merge behavior for compatible numeric widening.
3. `strict`: inference enabled, but schema mismatches across files fail early (no numeric widening).
4. `permissive`: inference enabled with permissive merge behavior (nullable + allowed numeric widening).
5. `schema_writeback=true`: inferred schema + fingerprint metadata is persisted to catalog file.
6. `schema_drift_policy=fail`: cached fingerprint mismatch fails query.
7. `schema_drift_policy=refresh`: cached fingerprint mismatch triggers schema refresh.

Recommended policy sets:

1. Development:
   - `schema_inference=on`
   - `schema_drift_policy=refresh`
   - optional `schema_writeback=true`
2. Strict reproducibility/CI:
   - `schema_inference=strict`
   - `schema_drift_policy=fail`
   - optional `schema_writeback=true`

## Migration Guide: Manual Schema -> Inference

If your catalogs were fully manual-schema and you want to adopt inference:

1. Start with `schema_inference=on` and `schema_drift_policy=refresh`.
2. Remove `schema` from selected parquet tables in `tables.json/toml`.
3. Run existing query/integration tests.
4. Enable `schema_writeback=true` to persist inferred schema and fingerprints.
5. After stabilization, consider `schema_inference=strict` for tighter multi-file controls.

Rollback path:

1. Set `schema_inference=off`.
2. Restore explicit `schema` entries in catalog for affected tables.

## Schema Troubleshooting

Common inference/drift failures and actions:

1. `schema inference failed ...`:
   - verify parquet file paths and read permissions
   - verify files are valid parquet
   - if inference intentionally disabled, set `schema` manually or enable inference
2. `schema drift detected ...`:
   - data files changed vs cached fingerprint
   - use `schema_drift_policy=refresh` to refresh automatically
   - keep `fail` for strict reproducibility
3. `incompatible parquet files ...`:
   - table points to parquet files with incompatible schemas
   - align file schemas or split into separate tables

## Official TPC-H Catalog Profiles (13.4.3)

Host-local catalog profiles for official dbgen parquet fixtures are provided under:

1. `tests/fixtures/catalog/tpch_dbgen_sf1_parquet.tables.json`
2. `tests/fixtures/catalog/tpch_dbgen_sf1_parquet.tables.toml`

These profiles predeclare `customer`, `orders`, and `lineitem` with required schemas/options so
Q1/Q3 can run without manual `register_table(...)` calls.

Usage pattern:

1. Set `FFQ_CATALOG_PATH` to one of the profile files.
2. Start the engine/session.
3. Execute canonical benchmark queries directly.

Validation coverage:

1. `crates/client/tests/tpch_catalog_profiles.rs` verifies profile load/parsing and Q1/Q3 execution flow.

## Relevant Code References

1. `crates/storage/src/provider.rs`
2. `crates/storage/src/parquet_provider.rs`
3. `crates/storage/src/object_store_provider.rs`
4. `crates/storage/src/vector_index.rs`
5. `crates/storage/src/qdrant_provider.rs`
6. `crates/storage/src/catalog.rs`
7. `crates/client/src/session.rs`
8. `crates/client/src/dataframe.rs`
