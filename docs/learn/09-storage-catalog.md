# LEARN-10: Storage, Catalog, and Table Resolution

This chapter explains how FFQ resolves table metadata at runtime: `TableDef`, schema requirements, catalog persistence, profile manifests, and parquet-provider behavior.

## 1) Core Components

Storage/catalog core code:

1. `crates/storage/src/catalog.rs`
2. `crates/storage/src/provider.rs`
3. `crates/storage/src/parquet_provider.rs`

Client/session wiring:

1. `crates/client/src/session.rs`
2. `crates/client/src/dataframe.rs`

## 2) `TableDef` Contract

`TableDef` is the canonical table metadata unit.

Key fields:

1. `name`
2. `uri` (single path, optional)
3. `paths` (multiple paths, optional)
4. `format`
5. `schema` (optional in type, required for v1 analyzer path)
6. `stats` (`rows`, `bytes`)
7. `options` (format/provider-specific metadata)

### 2.1 Path resolution rules

`TableDef::data_paths()`:

1. if `paths` is non-empty, use `paths`
2. else if `uri` is non-empty, use `[uri]`
3. else error (`table must define either uri or paths`)

### 2.2 Schema requirement in v1

Although `schema` is optional in struct shape, v1 planning/analyzer requires schema for column resolution.

`TableDef::schema_ref()`:

1. returns schema when present
2. errors when absent (`v1 analyzer needs a schema to resolve columns`)

Practical consequence:

1. register tables with explicit schema for reliable planning
2. missing schema may only surface later as planning/analyzer failure

## 3) Catalog Load/Save Model

`Catalog` is an in-memory map of `name -> TableDef`.

Core operations:

1. `register_table(TableDef)`
2. `get(name) -> TableDef`
3. `load(path)` / `save(path)`

### 3.1 File formats

Supported catalog formats:

1. JSON (`.json`)
2. TOML (`.toml`)

Unsupported or extension-less paths fail with explicit config errors.

### 3.2 Input shapes accepted

Catalog parser accepts either shape:

1. plain table list: `[{...}, {...}]`
2. wrapped object: `{ "tables": [...] }` (and TOML equivalent)

### 3.3 Persistence behavior

Save path:

1. `Catalog::save_to_json` / `save_to_toml`
2. sorted table list by name (`Catalog::tables()`)
3. atomic write strategy (`write_atomically`)

Atomic write strategy (high-level):

1. write staged temp file
2. if target exists: rename target to backup, then staged -> target
3. on failure, attempt rollback

This reduces partial-write risk for catalog updates.

## 4) Session-Level Catalog Resolution

Session startup (`Session::new`):

1. reads `FFQ_CATALOG_PATH` (default `./ffq_tables/tables.json`)
2. loads catalog file if it exists
3. otherwise starts empty catalog

Session persistence:

1. `Session::persist_catalog()` writes current catalog to configured file
2. write APIs (`save_as_table`) update catalog then persist

Managed-table path convention:

1. `managed_table_path(name)` resolves relative to catalog file directory

## 5) Table Resolution During Planning

`DataFrame` builds an optimizer/analyzer context from current catalog snapshot:

1. `SchemaProvider::table_schema` -> `TableDef::schema_ref()`
2. `OptimizerContext::table_stats` -> `TableDef.stats`
3. `OptimizerContext::table_metadata` -> `format + options`

Why this matters:

1. analyzer resolves column names/types from schema
2. optimizer uses stats for join strategy decisions
3. optimizer uses `format/options` for specialized rewrites (for example vector/qdrant paths)

## 6) `StorageProvider` Trait and Provider Selection

Trait (`crates/storage/src/provider.rs`):

1. `estimate_stats(table) -> Stats`
2. `scan(table, projection, filters) -> StorageExecNode`

v1 concrete implementation:

1. `ParquetProvider`

Optional/feature-gated:

1. object-store provider (`s3` feature)
2. qdrant provider (`qdrant` feature, index-backed vector path)

## 7) Parquet Provider Behavior (v1)

`ParquetProvider::scan(...)`:

1. validates `table.format == parquet` (case-insensitive)
2. resolves data paths from `TableDef`
3. constructs `ParquetScanNode`

`ParquetScanNode::execute(...)`:

1. opens each parquet file
2. decodes Arrow record batches
3. streams batches through `StreamAdapter`

Important current behavior:

1. projection and filters are stored on node but not fully pushed down in reader yet (best-effort v1 path)
2. reads are eager per file, then streamed as batch results

Failure modes:

1. wrong table format for parquet provider
2. missing path metadata
3. file open/decode errors

## 8) Profile Manifests

Profile manifests are prebuilt catalog files for known fixture sets.

Examples in repo:

1. `tests/fixtures/catalog/tpch_dbgen_sf1_parquet.tables.json`
2. `tests/fixtures/catalog/tpch_dbgen_sf1_parquet.tables.toml`
3. `tests/fixtures/catalog/tables.json` (integration fixture profile)

Use pattern:

1. set `FFQ_CATALOG_PATH` to desired profile
2. create engine/session
3. query tables without manual register calls

This is useful for benchmark/integration repeatability.

## 9) Practical Rules for Reliable Table Resolution

For predictable v1 behavior:

1. always provide schema in `TableDef`
2. ensure `uri`/`paths` are valid in the execution environment
3. keep `FFQ_CATALOG_PATH` stable across restarts if you expect persisted tables
4. set meaningful `stats` when you want optimizer join strategy decisions to be realistic
5. keep `format` and `options` accurate for provider/rewrite routing

## 10) Typical Misconfigurations and Symptoms

Common issues:

1. missing schema -> analyzer/planning failures on column resolution
2. empty `uri` and `paths` -> config error from `data_paths()`
3. wrong catalog extension -> load/save rejected
4. incorrect runtime paths in profile manifest -> file IO errors at scan time
5. format mismatch (`format != parquet` with parquet provider path) -> unsupported format error

## 11) Mental Model Summary

Think of storage/catalog in FFQ as:

1. `Catalog` = durable name->table metadata map
2. `TableDef` = single source of truth for schema/path/format/options/stats
3. planner consumes schema/stats/options from catalog snapshot
4. storage provider consumes path/format to produce executable scan node
5. write APIs update data files and then persist catalog state
