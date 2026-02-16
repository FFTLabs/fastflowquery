# LEARN-06: Distributed Stage Model and Shuffle

This chapter explains why distributed execution is split into stages, how shuffle data is partitioned and transported, and how attempts/cleanup keep the system correct under retries.

## 1) Why Stages Exist

A distributed query cannot run as one monolithic task because some operators require data redistribution (for example hash aggregate finalization or shuffle join).

FFQ uses stage boundaries so:

1. upstream tasks can write partitioned intermediate output
2. downstream tasks can read only the partitions they need
3. scheduler can gate downstream stages on upstream completion

## 2) Stage Cutting Rule

Implementation: `crates/distributed/src/stage.rs`

Rule:

1. every `Exchange::ShuffleRead` cuts a stage boundary
2. operators upstream of that `ShuffleRead` belong to a parent stage
3. downstream operators stay in the current stage

Result:

1. stage DAG with explicit parent/child edges
2. deterministic stage structure from physical plan shape

## 3) Stage DAG to Scheduling

Coordinator behavior (`crates/distributed/src/coordinator.rs`):

1. on `SubmitQuery`, deserialize physical plan and build stage DAG
2. initialize per-stage task runtime state
3. only schedule stages whose parent stages are fully succeeded (`runnable_stages`)

Worker behavior (`crates/distributed/src/worker.rs`):

1. workers pull tasks via `GetTask`
2. each assignment includes `query_id`, `stage_id`, `task_id`, `attempt`
3. worker executes stage fragment and reports status

## 4) Shuffle Partitioning Model (Map/Reduce)

At physical planning time, joins/aggregates inject exchange nodes with partition specs.

For hash partitioning:

1. map side hashes partition keys to `reduce_partition`
2. each map task writes one file per reduce partition
3. reduce side reads only its partition from every map task

This is the map/reduce contract in FFQ v1 shuffle.

## 5) Shuffle File and Index Layout

Path contract (`crates/shuffle/src/layout.rs`):

1. payload: `shuffle/{query}/{stage}/{map_task}/{attempt}/part-{reduce}.ipc`
2. per-attempt map index:
   - `.../index.json`
   - `.../index.bin`

Index metadata (`MapTaskIndex`):

1. query/stage/map_task/attempt identity
2. `created_at_ms`
3. partition entries (`reduce_partition`, `file`, `bytes`, `rows`, `batches`)

Payload format:

1. Arrow IPC stream (`.ipc`) per partition file

## 6) Write and Read Data Flow

### Write side

Writer (`crates/shuffle/src/writer.rs`):

1. write each partition payload as IPC stream
2. gather stats (bytes/rows/batches)
3. emit deterministic index metadata

### Read side

Reader (`crates/shuffle/src/reader.rs`):

1. resolve index (prefers `index.bin`, falls back to `index.json`)
2. fetch partition bytes (direct or chunked)
3. decode IPC stream back into `RecordBatch` list

gRPC path:

1. `FetchShufflePartition` streams chunked bytes
2. receiver reassembles chunks and decodes IPC

## 7) Attempt Semantics and Retry Safety

Attempt is part of both storage path and coordinator registry key.

Identity:

1. `(query_id, stage_id, map_task, attempt, reduce_partition)` identifies one partition output

Coordinator registry:

1. `register_map_output` stores metadata by exact attempt key
2. `fetch_shuffle_partition_chunks` requires requested attempt to be registered
3. unknown attempt -> explicit planning error

This prevents accidentally reading unregistered outputs.

## 8) Stale Attempt Ignore Rules

Reader latest-attempt APIs:

1. `latest_attempt(...)` selects max attempt id under map-task directory
2. `read_partition_latest(...)` and `fetch_partition_chunks_latest(...)` read that attempt only

Worker-side shuffle read path uses latest-attempt reads for stage input in current v1 path.

Worker shuffle gRPC service (`crates/distributed/src/grpc.rs`) supports:

1. `attempt == 0` sentinel meaning "use latest attempt"

Effect:

1. stale older attempts are ignored by latest-read flows
2. retries can safely produce new attempt outputs without corrupting latest execution

## 9) TTL Cleanup Model

Implementation: `ShuffleWriter::cleanup_expired_attempts(...)` in `crates/shuffle/src/writer.rs`.

Policy:

1. traverse `shuffle/` by query -> stage -> map_task
2. keep latest attempt directory unconditionally
3. evaluate older attempts only
4. remove older attempt if:
   - `index.json` exists
   - `created_at_ms + ttl <= now`

Guarantees:

1. latest attempt is never removed by cleanup
2. cleanup is safe to run repeatedly
3. return value reports removed attempt directory count

## 10) Determinism Contract

Distributed shuffle/stage determinism in v1 relies on:

1. deterministic stage cuts from `ShuffleRead`
2. deterministic path naming from IDs `(query, stage, map_task, attempt, reduce)`
3. deterministic partition metadata mapping in index
4. deterministic latest-attempt selection (max attempt id)
5. deterministic cleanup rule (never delete latest, only expired older attempts)

## 11) Failure Modes and What They Mean

Common failures:

1. `map output not registered for requested attempt`
   - coordinator has no registry entry for that attempt
2. `no shuffle attempts found for map task`
   - reader cannot find attempt directories
3. IPC decode/read failures
   - corrupt/incomplete payload or wrong file
4. query id numeric conversion failure in shuffle path
   - v1 shuffle layout currently expects numeric `query_id`

## 12) Mental Model Summary

Think of distributed shuffle as:

1. stage producer writes partitioned IPC files + index
2. coordinator tracks which attempt is valid/registered
3. stage consumer reads registered/latest partition bytes
4. retries create new attempt directories; stale attempts are ignored and later TTL-cleaned

For a stricter reference contract, see `docs/v1/shuffle-stage-model.md`.
