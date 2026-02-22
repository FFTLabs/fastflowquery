# Shuffle & Distributed Execution v2 (Learner)

- Status: draft
- Owner: @ffq-runtime
- Last Verified Commit: 7888e4c
- Last Verified Date: 2026-02-21

This chapter explains EPIC 7 from a concept-first perspective.

## What EPIC 7 changes conceptually

EPIC 7 is about reducing distributed shuffle latency and making the control/data path safer and more observable.

Implemented/high-signal areas today:

1. pipelined shuffle (MVP) with committed-byte readiness
2. range/chunk fetch protocol for incremental reads
3. stream epochs + committed offsets for retry/epoch safety
4. coordinator backpressure windows and streaming metrics
5. TTFR benchmark + regression gate
6. partial speculative execution for stragglers

Still open/partial:

1. shuffle compression
2. full zero-copy/copy minimization path
3. centralized memory/spill manager
4. full locality-aware scheduling

## Pipelined shuffle mental model

Classic shuffle waits for the whole map stage to finish before reducers start.

Pipelined shuffle (MVP) changes this:

1. maps publish partition progress (`committed_offset`)
2. coordinator tracks per-partition stream metadata
3. reducers can start once required partitions are readable enough
4. reducers fetch only readable byte ranges
5. coordinator throttles map publish/reduce fetch windows with backpressure signals

This primarily improves TTFR (time to first row), not only total runtime.

## Stream metadata and epoch safety

Three fields matter:

1. `stream_epoch`
2. `committed_offset`
3. `finalized`

Why they exist:

1. `committed_offset` prevents reading uncommitted bytes
2. `stream_epoch` rejects stale reads after retries/re-registration
3. `finalized` gives unambiguous EOF semantics

## Chunk-range fetch and incremental consumption

Reducers use range fetch requests:

1. `start_offset`
2. `max_bytes`
3. `min_stream_epoch`

This allows:

1. incremental polling without re-reading everything
2. safe EOF-marker responses when no new bytes are readable yet
3. reconstruction from out-of-order range fetch requests (validated in tests)

## Backpressure and observability

Reducers report queue/in-flight pressure.

Coordinator responds with recommended windows:

1. map publish window
2. reduce fetch window

Streaming metrics expose pipeline behavior:

1. `first_chunk_ms`
2. `first_reduce_row_ms`
3. `stream_lag_ms`
4. `backpressure_events`
5. `stream_buffered_bytes`
6. `stream_active_count`

These metrics are what you use to debug “pipelining enabled but no TTFR win”.

## Speculative execution (partial)

Speculative execution launches a duplicate attempt for a straggling task.

Current behavior:

1. coordinator detects stragglers from runtime distribution
2. speculative attempt may be launched on another worker
3. attempt race resolution preserves query correctness

Current limitation:

1. locality-aware placement is still limited (not a full locality scheduler)

## Where to read next (implementation docs)

1. `docs/v2/distributed-runtime.md`
2. `docs/v2/control-plane.md`
3. `docs/v2/adaptive-shuffle-tuning.md`
4. `docs/v2/shuffle-stage-model.md`
5. `docs/v2/benchmarks.md`

## Validation checklist

```bash
cargo test -p ffq-distributed --features grpc coordinator_allows_pipelined_reduce_assignment_when_partition_ready
cargo test -p ffq-distributed --features grpc coordinator_backpressure_throttles_assignment_windows
cargo test -p ffq-distributed --features grpc worker_shuffle_fetch_supports_range_and_returns_chunk_offsets_and_watermark
cargo test -p ffq-distributed --features grpc worker_shuffle_service_enforces_stream_guardrails
cargo test -p ffq-distributed --features grpc coordinator_launches_speculative_attempt_for_straggler_and_accepts_older_success
make bench-v2-pipelined-shuffle
make bench-v2-pipelined-shuffle-gate CANDIDATE=<candidate.json>
```
