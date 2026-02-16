# Storage and Catalog (v1)

This page documents the implemented v1 storage/catalog behavior in FFQ.

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
        filters: Vec<String>,
    ) -> Result<StorageExecNode>;
}
```

Notes:
1. `estimate_stats` is used for planning/heuristics (`rows`, `bytes`).
2. `scan` returns an `ExecNode` that produces Arrow `RecordBatch` stream.
3. Current v1 parquet scan keeps `projection/filters` in node state; aggressive pushdown is limited.

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

## Optional Object Store Behavior (`s3`)

Surface exists behind feature `s3`:
- `crates/storage/src/object_store_provider.rs`
- `crates/storage/Cargo.toml` feature `s3`

Current state (v1 as implemented):
1. `ObjectStoreProvider` exists and implements `StorageProvider`.
2. `scan` currently returns `Unsupported` (experimental placeholder).
3. `estimate_stats` still returns table stats if provided.

Implication: object-store wiring is intentionally non-default and currently not a complete scan path.

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
4. `schema`: optional persisted Arrow schema; required for analyzer-driven SQL paths.
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

### Example 1: register parquet table and query immediately

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
        schema: None, // for robust SQL analysis, provide schema when available
        stats: Default::default(),
        options: Default::default(),
    },
);

let rows = engine
    .sql("SELECT l_orderkey FROM lineitem LIMIT 10")?
    .collect()
    .await?;
```

### Example 2: multi-file parquet table via `paths`

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
