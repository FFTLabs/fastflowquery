# API Contract + SemVer (v2)

- Status: draft
- Owner: @ffq-api
- Last Verified Commit: TBD
- Last Verified Date: TBD

## Scope

This page is the v2 source of truth for public API compatibility.

It defines:

1. stable `ffq-client` API surface (`Engine`, `DataFrame`, `GroupedDataFrame`)
2. feature-gated public APIs
3. deprecation and SemVer policy
4. CI checks that enforce the contract
5. a breaking-change decision matrix contributors can use before merging

Primary references:

1. `crates/client/src/lib.rs`
2. `crates/client/src/engine.rs`
3. `crates/client/src/dataframe.rs`
4. `docs/dev/api-semver-policy.md`
5. `.github/workflows/api-semver.yml`

## Public Surface Freeze (v2)

The following exported types are the v2 contract baseline:

1. `ffq_client::Engine`
2. `ffq_client::DataFrame`
3. `ffq_client::GroupedDataFrame`
4. `ffq_client::WriteMode`
5. extension traits/interfaces re-exported for users:
   - `ffq_client::ScalarUdf`
   - `ffq_client::PhysicalOperatorFactory`

### Stable `Engine` API (v2)

Core methods considered contract-stable:

1. `Engine::new`
2. `Engine::config`
3. `Engine::register_table`
4. `Engine::register_table_checked`
5. `Engine::sql`
6. `Engine::sql_with_params`
7. `Engine::table`
8. `Engine::list_tables`
9. `Engine::table_schema`
10. `Engine::table_schema_with_origin`
11. `Engine::shutdown`
12. `Engine::prometheus_metrics`

Stable extensibility methods:

1. `Engine::register_optimizer_rule`
2. `Engine::deregister_optimizer_rule`
3. `Engine::register_scalar_udf`
4. `Engine::register_numeric_udf_type`
5. `Engine::deregister_scalar_udf`
6. `Engine::register_physical_operator_factory`
7. `Engine::deregister_physical_operator_factory`
8. `Engine::list_physical_operator_factories`

### Stable `DataFrame` API (v2)

1. `DataFrame::logical_plan`
2. `DataFrame::filter`
3. `DataFrame::join`
4. `DataFrame::groupby`
5. `DataFrame::explain`
6. `DataFrame::collect_stream`
7. `DataFrame::collect`
8. `DataFrame::write_parquet`
9. `DataFrame::write_parquet_with_mode`
10. `DataFrame::save_as_table`
11. `DataFrame::save_as_table_with_mode`

### Stable `GroupedDataFrame` API (v2)

1. `GroupedDataFrame::agg`

## Feature-Gated Public API

The contract includes the following feature-gated additions.
Removing or changing them incompatibly is also a breaking change when the feature is enabled.

### `vector`

1. `Engine::hybrid_search`

### `profiling`

1. `Engine::serve_metrics_exporter`

### `ffi`

1. C ABI entrypoints under `crates/client/src/ffi.rs`
2. consumer-facing C header/API examples under `include/`

### `python`

1. Python bindings under `crates/client/src/python.rs`
2. wheel and packaging workflow (`.github/workflows/python-wheels.yml`)

## Runtime Selection Contract

`Engine::new` behavior is stable in v2:

1. build without `distributed` feature: embedded runtime only
2. build with `distributed` feature:
   - if coordinator endpoint is configured (`EngineConfig` or env), distributed runtime is used
   - otherwise embedded runtime is used

## Deprecation Policy

Policy reference: `docs/dev/api-semver-policy.md`.

Contract rules:

1. breaking API changes are allowed only in major releases
2. deprecations are introduced first (with migration note), then removed in the next major
3. renames/removals without a deprecation window are not allowed in v2 minors/patches

Contributor requirement for deprecations:

1. mark symbol with `#[deprecated]`
2. add migration guidance in docs/changelog
3. keep old path functional until the next major line

## Breaking-Change Decision Matrix

Use this table to classify a change.

| Change type | Breaking in v2? | Notes |
|---|---|---|
| Remove public method/type/enum variant | yes | major-only |
| Rename public method/type | yes | major-only unless old alias kept + deprecated |
| Change method signature (args/return/asyncness) | yes | major-only |
| Strengthen trait bounds on public API | yes | major-only |
| Narrow accepted input behavior | yes | major-only unless bug/security fix explicitly documented |
| Add new optional method/type | no | minor/patch allowed |
| Add new enum variant | potentially | treat as breaking if downstream exhaustive matching is expected |
| Add field to public struct with public constructors | potentially | evaluate case-by-case; prefer non-breaking builders/accessors |
| Deprecate symbol without removal | no | requires migration path |
| Internal refactor without API shape/behavior change | no | patch allowed |

## CI Enforcement

### Public API contract tests

Workflow: `.github/workflows/api-semver.yml` (job `public-api-contract`).

Command:

```bash
cargo test -p ffq-client --test public_api_contract
```

Purpose:

1. validates that the expected v2 API shape and core flows remain present (`Engine::new`, `sql`, `collect_stream`, `collect`)
2. validates vector convenience API existence when `vector` is enabled

### SemVer diff checks

Workflow: `.github/workflows/api-semver.yml` (job `semver-check`).

Command used in CI:

```bash
cargo semver-checks check-release \
  --manifest-path crates/client/Cargo.toml \
  --baseline-rev origin/<base-branch>
```

Purpose:

1. detects incompatible public API changes against PR base branch
2. fails PR when an unintended breaking change is introduced

## Contributor Checklist (Before Merge)

1. Is the changed symbol in the stable surface above?
2. If yes, is behavior/signature still compatible?
3. If not compatible, is this a planned major-version change?
4. If deprecating, did you add migration guidance?
5. Do `public_api_contract` and `semver-checks` pass in CI?

If any answer fails, the change is not v2-compatible.

## Reproducible Local Verification

```bash
cargo test -p ffq-client --test public_api_contract
cargo install cargo-semver-checks --locked
cargo semver-checks check-release --manifest-path crates/client/Cargo.toml --baseline-rev origin/main
```

Expected:

1. contract test passes
2. semver check reports no breaking change unless intentionally planned
