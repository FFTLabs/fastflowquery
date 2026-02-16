# Client Runtime and Result Flow (v1)

This page documents how the client selects runtime mode and how `engine.sql(...).collect()` returns rows in embedded and distributed execution.

## Core Entry Points

1. `Engine::new(config)` -> creates a `Session` with runtime + catalog + planner.
2. `Engine::sql(query)` -> parses SQL and returns `DataFrame`.
3. `DataFrame::collect().await` -> executes plan and returns `Vec<RecordBatch>`.

Primary files:
1. `crates/client/src/engine.rs`
2. `crates/client/src/session.rs`
3. `crates/client/src/dataframe.rs`
4. `crates/client/src/runtime.rs`

## Runtime Selection (env/config)

Implemented in `Session::new` (`crates/client/src/session.rs`).

Selection rules:
1. If client is built **without** `distributed` feature:
- runtime is always `EmbeddedRuntime`.
2. If client is built **with** `distributed` feature:
- if `FFQ_COORDINATOR_ENDPOINT` is set, runtime is `DistributedRuntime(endpoint)`.
- otherwise runtime falls back to `EmbeddedRuntime`.

Environment variables used by session bootstrap:
1. `FFQ_COORDINATOR_ENDPOINT` -> distributed control-plane endpoint (for example `http://127.0.0.1:50051`).
2. `FFQ_CATALOG_PATH` -> catalog file path (default `./ffq_tables/tables.json`).

`.env` loading:
1. `dotenvy::dotenv()` is called on session creation for best-effort env hydration.

## Exact `engine.sql(...).collect()` Flow

### Step-by-step pipeline

1. `Engine::sql(query)`
- Calls planner frontend (`plan_sql`) and returns `DataFrame` with logical plan.

2. `DataFrame::collect().await`
- Calls `execute_with_schema()`.

3. `DataFrame::execute_with_schema()`
- Takes catalog snapshot under read lock.
- Runs optimizer + analyzer via `PlannerFacade::optimize_analyze(...)`.
- Builds physical plan via `PlannerFacade::create_physical_plan(...)`.
- Constructs `QueryContext` from engine config (`batch_size_rows`, `mem_budget_bytes`, `spill_dir`).
- Calls `session.runtime.execute(physical, ctx, catalog_snapshot)`.
- Collects returned stream into `Vec<RecordBatch>`.

4. `collect()` return
- Returns only batches (`Vec<RecordBatch>`), schema is internal to `execute_with_schema()`.

## Embedded Mode Result Flow

Runtime implementation: `EmbeddedRuntime` in `crates/client/src/runtime.rs`.

Execution path:
1. `EmbeddedRuntime::execute(...)` creates local trace ids (`query_id`, `stage_id=0`, `task_id=0`).
2. Calls recursive `execute_plan(...)` on physical plan.
3. Operators run in-process:
- scan/filter/project/join/aggregate/topk/limit/sink.
4. Resulting batches are wrapped into `StreamAdapter` and returned as `SendableRecordBatchStream`.
5. `DataFrame::execute_with_schema()` collects stream into `Vec<RecordBatch>`.

What returns rows:
1. The embedded runtime directly materializes result batches from operator outputs.
2. No network roundtrip is involved.

## Distributed Mode Result Flow

Runtime implementation: `DistributedRuntime` in `crates/client/src/runtime.rs`.

Execution path:
1. Serialize physical plan to JSON bytes.
2. Generate numeric query id string.
3. Connect `ControlPlaneClient` to `FFQ_COORDINATOR_ENDPOINT`.
4. Submit query via `SubmitQuery { query_id, physical_plan_json }`.
5. Poll query status via `GetQueryStatus` every 50ms until terminal state:
- `Succeeded` -> continue,
- `Failed`/`Canceled` -> return error,
- timeout after bounded polls -> return error.
6. On success, fetch result stream via `FetchQueryResults`.
7. Concatenate streamed chunks into one IPC payload buffer.
8. Decode IPC bytes to `(schema, batches)` via `decode_record_batches_ipc(...)`.
9. Wrap decoded batches into `StreamAdapter` and return stream.
10. `DataFrame::execute_with_schema()` collects stream into `Vec<RecordBatch>`.

What returns rows:
1. Rows come from coordinator-owned result payload registered by workers (`RegisterQueryResults`).
2. Client returns decoded Arrow batches after `FetchQueryResults` completes.

## Query Submission and Result Publication (distributed detail)

Server-side linkage:
1. Worker executes assigned task fragment.
2. If task is final sink stage, worker encodes output batches to IPC.
3. Worker calls `RegisterQueryResults(query_id, ipc_payload)`.
4. Coordinator stores payload and serves it through `FetchQueryResults` stream.

This is why `engine.sql(...).collect()` in distributed mode can return real rows instead of an empty stream.

## Error and Terminal Behavior

Embedded mode:
1. Operator or storage failures propagate directly as execution errors.

Distributed mode:
1. `GetQueryStatus` terminal state drives client behavior:
- `Succeeded` -> fetch results,
- `Failed` -> return `distributed query failed: ...`,
- `Canceled` -> return `distributed query canceled: ...`.
2. Missing/invalid result stream or IPC decode errors also propagate as execution errors.

## Minimal Mode Comparison

1. Embedded:
- lowest overhead,
- synchronous in-process execution path,
- direct batch return.

2. Distributed:
- remote coordinator/worker orchestration,
- submit + poll + stream result lifecycle,
- same logical/physical planning pipeline, different runtime transport.

## Operational Checklist

1. For embedded execution:
- no distributed endpoint required.

2. For distributed execution:
- build with `--features distributed`.
- set `FFQ_COORDINATOR_ENDPOINT`.
- ensure coordinator + workers are running and connected.

3. In both modes:
- keep `FFQ_CATALOG_PATH` stable for consistent table resolution.

## References

1. `crates/client/src/engine.rs`
2. `crates/client/src/session.rs`
3. `crates/client/src/dataframe.rs`
4. `crates/client/src/runtime.rs`
5. `crates/distributed/src/grpc.rs`
6. `crates/distributed/src/worker.rs`
7. `crates/client/tests/distributed_runtime_roundtrip.rs` (distributed vs embedded parity for join+agg and join projection)
