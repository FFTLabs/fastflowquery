# Shuffle and Stage Model (v1)

This document describes the implemented v1 behavior for stage cutting, shuffle layout, index metadata, retry attempts, stale-attempt handling, and TTL cleanup.

## Stage Cutting Model

Implementation: `crates/distributed/src/stage.rs`.

Rule:
1. A new stage boundary is introduced at `Exchange::ShuffleRead`.
2. Upstream of each `ShuffleRead` is assigned to a parent stage.
3. Stage DAG edges connect upstream producer stage -> downstream consumer stage.

Operational implications:
1. Operators like `PartialHashAggregate` and `ShuffleWrite` are placed in upstream stages.
2. Operators like `ShuffleRead` and `FinalHashAggregate` are placed in downstream stages.
3. Coordinator schedules tasks per stage based on parent completion.

Reference test:
1. `cuts_stage_at_shuffle_read` in `crates/distributed/src/stage.rs`.

## Shuffle File Path Contract

Implementation: `crates/shuffle/src/layout.rs`.

Canonical partition payload path:
1. `shuffle/{query_id}/{stage_id}/{map_task}/{attempt}/part-{reduce_partition}.ipc`

Related paths:
1. Map task attempt directory:
- `shuffle/{query_id}/{stage_id}/{map_task}/{attempt}`
2. Map task base directory:
- `shuffle/{query_id}/{stage_id}/{map_task}`
3. Index paths:
- `.../index.json`
- `.../index.bin`

Notes:
1. `query_id` used in shuffle path is numeric (`u64`) in current v1 implementation.
2. Payload format is Arrow IPC stream (`.ipc`).

## Shuffle Write/Read Roundtrip

Writer implementation: `crates/shuffle/src/writer.rs`.
Reader implementation: `crates/shuffle/src/reader.rs`.

Write flow:
1. Partition output batches are written to `part-{reduce}.ipc` files.
2. Per-partition metadata (bytes/rows/batches) is collected.
3. A map-task index is emitted (`index.json` and `index.bin`).

Read flow:
1. Reader resolves attempt and partition.
2. Payload can be read directly or fetched as chunked bytes.
3. Chunked payloads are reassembled and decoded via IPC reader.

Deterministic expectation:
1. Writing then reading returns equivalent batch content for the selected attempt/partition.
2. Chunking does not change decoded results.

Reference tests:
1. `writes_index_and_reads_partition_from_streamed_chunks` in `crates/shuffle/src/writer.rs`.

## Index Metadata Contract

Layout struct: `MapTaskIndex` in `crates/shuffle/src/layout.rs`.

Fields:
1. `query_id: u64`
2. `stage_id: u64`
3. `map_task: u64`
4. `attempt: u32`
5. `created_at_ms: u64`
6. `partitions: Vec<ShufflePartitionMeta>`

Per-partition metadata (`ShufflePartitionMeta`):
1. `reduce_partition`
2. `file` (relative path)
3. `bytes`
4. `rows`
5. `batches`

Binary index (`index.bin`) details:
1. Magic: `FFQI`
2. Version: `u32` (v1 = `1`)
3. Payload length + JSON payload bytes

Reader behavior:
1. Prefer `index.bin` when present.
2. Fallback to `index.json`.

## Retry Attempts and Stale-Attempt Handling

Attempt id semantics:
1. Attempt id is part of shuffle path and registry key.
2. Coordinator map output registry key includes `(query_id, stage_id, map_task, attempt)`.

Coordinator behavior (`crates/distributed/src/coordinator.rs`):
1. `register_map_output` stores outputs by exact attempt key.
2. `fetch_shuffle_partition_chunks` requires requested attempt to be registered.
3. Unknown attempt fetch fails with planning error (`map output not registered for requested attempt`).

Reader-side latest-attempt behavior (`crates/shuffle/src/reader.rs`):
1. `latest_attempt(...)` selects max attempt id under map-task directory.
2. `read_partition_latest(...)` and `fetch_partition_chunks_latest(...)` use latest attempt.

Stale-attempt ignore rules (v1):
1. When reading via `*_latest`, older attempts are ignored.
2. Worker shuffle read path uses latest-attempt APIs for stage input in current v1 worker execution path.
3. Worker shuffle service gRPC also supports `attempt == 0` as "latest" sentinel in `crates/distributed/src/grpc.rs`.

Reference test:
1. `ignores_old_attempts_and_cleans_up_by_ttl` in `crates/shuffle/src/writer.rs`.

## TTL Cleanup (Worker-Side)

Implementation: `ShuffleWriter::cleanup_expired_attempts` in `crates/shuffle/src/writer.rs`.

Cleanup policy:
1. Traverse `shuffle/` tree by query/stage/map-task.
2. For each map-task:
- keep latest attempt directory unconditionally,
- evaluate older attempts only.
3. If older attempt has `index.json` with `created_at_ms` and is older than TTL, remove attempt directory.

Behavior guarantees:
1. Latest attempt is never removed by TTL cleanup pass.
2. Cleanup is idempotent across repeated runs.
3. Cleanup result reports number of removed attempt directories.

## Determinism and Contract Summary

v1 shuffle/stage deterministic contract:
1. Stage boundaries are deterministic from physical plan shape (`ShuffleRead` cut rule).
2. Shuffle file paths are deterministic from `(query_id, stage_id, map_task, attempt, reduce_partition)`.
3. Index metadata deterministically maps reduce partitions to payload files and stats.
4. Latest-attempt read APIs deterministically choose max attempt id and ignore stale attempts.
5. TTL cleanup deterministically preserves latest attempt and removes only expired older attempts.

## Relevant References

1. `crates/distributed/src/stage.rs`
2. `crates/shuffle/src/layout.rs`
3. `crates/shuffle/src/writer.rs`
4. `crates/shuffle/src/reader.rs`
5. `crates/distributed/src/coordinator.rs`
6. `crates/distributed/src/grpc.rs`
7. `crates/distributed/src/worker.rs`
