# Migration Guide: v1 -> v2

- Status: draft
- Owner: @ffq-docs
- Last Verified Commit: TBD
- Last Verified Date: TBD

This guide is an operational migration runbook for users and contributors moving from v1 docs/workflows to v2.

## Scope

Covered here:

1. behavior and API contract changes
2. config and feature-flag changes
3. command and workflow changes
4. documentation map (`v1 page -> v2 page`)
5. migration checklist and pitfalls

## High-Level Migration Summary

What stays compatible:

1. core `Engine` / `DataFrame` usage is still library-first
2. embedded runtime remains default
3. distributed mode remains feature-gated and endpoint-driven
4. legacy one-shot CLI forms still work

What is now explicit in v2:

1. API compatibility contract + SemVer policy and CI gating
2. feature matrix as a documented v2 runtime contract
3. capability-aware scheduling for distributed custom operators
4. dedicated FFI + Python binding runbooks
5. explicit testing/validation checklist by subsystem

## API and Behavior Changes

## 1) Public API Contracting

v1:

1. API stability assumptions were mostly implicit in docs/tests

v2:

1. stable surface is explicitly documented in `docs/v2/api-contract.md`
2. SemVer policy is explicit (`docs/dev/api-semver-policy.md`)
3. CI checks public API/semver (`.github/workflows/api-semver.yml`)

Migration action:

1. treat changes to `Engine`/`DataFrame` methods as contract changes requiring SemVer review

## 2) Distributed Custom Operator Routing

v1:

1. custom operator behavior existed but deployment guidance was sparse

v2:

1. worker heartbeat advertises `custom_operator_capabilities`
2. coordinator filters assignments by required operator names
3. process-local registry constraints are documented and test-backed

Migration action:

1. if using custom operators in distributed mode, add worker bootstrap registration before production rollout
2. follow `docs/v2/custom-operators-deployment.md`

## 3) Schema Inference Operationalization

v1:

1. schema inference existed but migration guidance was fragmented

v2:

1. quickstart/testing docs include explicit inference/drift/writeback policies
2. schema origin (`catalog-defined` vs `inferred`) is part of REPL usage guidance

Migration action:

1. decide policy explicitly:
   - `FFQ_SCHEMA_INFERENCE=off|on|strict|permissive`
   - `FFQ_SCHEMA_DRIFT_POLICY=fail|refresh`
   - `FFQ_SCHEMA_WRITEBACK=true|false`

## Config and Feature-Flag Changes

## Workspace + crate baseline

1. workspace edition is `2024`
2. workspace version line is `2.0.0`

## v2 feature matrix (client)

1. `core` (default)
2. `embedded` (legacy alias)
3. `minimal`
4. `distributed`
5. `s3`
6. `vector`
7. `qdrant`
8. `python`
9. `ffi`
10. `profiling`

Migration action:

1. update CI/build scripts to use the documented matrix combinations in `docs/v2/runtime-portability.md`

## Command Migration

## CLI usage

Preferred v2 one-shot SQL:

```bash
cargo run -p ffq-client -- query --sql "SELECT 1"
```

Still-supported legacy forms:

```bash
cargo run -p ffq-client -- "SELECT 1"
cargo run -p ffq-client -- --plan "SELECT 1"
```

Migration action:

1. migrate automation/docs to `query`/`repl` subcommand style for clarity

## REPL

v2 preferred:

```bash
cargo run -p ffq-client -- repl --catalog tests/fixtures/catalog/tables.json
```

Migration action:

1. move ad-hoc SQL shell docs/scripts to `docs/v2/repl.md` commands

## Validation/test command baseline

Use `docs/v2/testing.md` as source of truth. Minimal migration set:

```bash
make test-13.1
make test-13.2-parity
make ffi-example
make python-dev-install
```

## Documentation Map: v1 -> v2

| v1 page | v2 page |
|---|---|
| `docs/v1/README.md` | `docs/v2/README.md` |
| `docs/v1/architecture.md` | `docs/v2/architecture.md` |
| `docs/v1/quickstart.md` | `docs/v2/quickstart.md` |
| `docs/v1/client-runtime.md` | `docs/v2/client-runtime.md` |
| `docs/v1/distributed-runtime.md` | `docs/v2/distributed-runtime.md` + `docs/v2/control-plane.md` |
| `docs/v1/shuffle-stage-model.md` | `docs/v2/shuffle-stage-model.md` |
| `docs/v1/operators-core.md` | `docs/v2/operators-core.md` |
| `docs/v1/storage-catalog.md` | `docs/v2/storage-catalog.md` |
| *(new in v2)* SQL semantics support matrix | `docs/v2/sql-semantics.md` |
| `docs/v1/writes-dml.md` | `docs/v2/writes-dml.md` |
| `docs/v1/vector-rag.md` | `docs/v2/vector-rag.md` |
| `docs/v1/observability.md` | `docs/v2/observability.md` |
| `docs/v1/testing.md` | `docs/v2/testing.md` |
| `docs/v1/integration-13.2.md` | `docs/v2/integration-13.2.md` |
| `docs/v1/benchmarks.md` | `docs/v2/benchmarks.md` |
| `docs/v1/known-gaps.md` | `docs/v2/known-gaps.md` |
| *(new in v2)* | `docs/v2/api-contract.md` |
| *(new in v2)* | `docs/v2/runtime-portability.md` |
| *(new in v2)* | `docs/v2/ffi-python.md` |
| *(new in v2)* | `docs/v2/extensibility.md` |
| *(new in v2)* | `docs/v2/custom-operators-deployment.md` |
| *(new in v2)* | `docs/v2/migration-v1-to-v2.md` |

## Migration Checklist (Executable)

Run in order.

1. Update local branch and dependencies

```bash
cargo build --no-default-features
cargo build --features distributed,python,s3
```

2. Validate core correctness baseline

```bash
make test-13.1-core
make test-13.2-embedded
```

3. Validate distributed parity path

```bash
make test-13.2-parity
```

4. Validate bindings

```bash
make ffi-example
make python-dev-install
python -m pip install pyarrow
python - <<'PY'
import ffq
e = ffq.Engine()
e.register_table("lineitem", "tests/fixtures/parquet/lineitem.parquet")
assert e.sql("SELECT l_orderkey FROM lineitem LIMIT 1").collect().num_rows == 1
print("migration python smoke: OK")
PY
```

5. Validate extensibility paths (if used)

```bash
cargo test -p ffq-client --test udf_api
cargo test -p ffq-planner --test optimizer_custom_rule
cargo test -p ffq-client --test physical_registry
cargo test -p ffq-distributed --features grpc coordinator_assigns_custom_operator_tasks_only_to_capable_workers
```

6. Move team docs/scripts to v2 references

1. replace `docs/v1/...` links with `docs/v2/...`
2. use this page's map for direct replacements

Completion criteria:

1. all commands above exit `0`
2. no v1-only doc dependency remains in active contributor workflow

## Common Pitfalls

1. Using old docs as primary source:
   - fix: treat `docs/v2/*` as canonical for v2 behavior

2. Assuming custom operators register cluster-wide automatically:
   - fix: register per worker process; verify capability heartbeat

3. Mixing schema policies implicitly:
   - fix: set schema inference/drift/writeback env explicitly in automation

4. Treating API changes as internal refactors:
   - fix: check `docs/v2/api-contract.md` and semver gate before merging

5. Running distributed tests without healthy compose services:
   - fix: verify `docker compose -f docker/compose/ffq.yml ps` and endpoint env

6. Python collect failures due to missing `pyarrow`:
   - fix: install `pyarrow` or use `collect_ipc()`

## Related v2 Docs

1. `docs/v2/quickstart.md`
2. `docs/v2/testing.md`
3. `docs/v2/api-contract.md`
4. `docs/v2/runtime-portability.md`
5. `docs/v2/extensibility.md`
6. `docs/v2/custom-operators-deployment.md`
