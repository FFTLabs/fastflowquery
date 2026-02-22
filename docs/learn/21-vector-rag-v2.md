# Vector / RAG v2 (Learner Addendum)

- Status: draft
- Owner: @ffq-vector
- Last Verified Commit: 7888e4c
- Last Verified Date: 2026-02-21

This chapter extends `docs/learn/10-vector-rag-internals.md` with EPIC 9 v2 additions that are now implemented.

## Why this addendum exists

The original learner chapter explains vector routing and qdrant rewrite/fallback well, but EPIC 9 adds newer v2 API/planner/runtime surface:

1. `HybridVectorScan` logical node
2. `VectorKnnExec` physical node knobs (`metric`, `ef_search`, `prefilter`)
3. batched hybrid query API (`hybrid_search_batch`)
4. pluggable embedding provider API (`EmbeddingProvider`)

## 9.1 Hybrid node and score behavior (partial)

Planner/runtime now support a hybrid logical node:

1. `LogicalPlan::HybridVectorScan`
2. lowered to `PhysicalPlan::VectorKnn(VectorKnnExec)`

Why this matters:

1. it represents vector retrieval directly in logical/physical planning instead of only implicit SQL top-k rewrites
2. explain output can show vector retrieval intent and tuning details (`metric`, `ef_search`, `prefilter`, query count/dim)

## 9.2 Connector-aware prefilter pushdown (implemented subset)

Current implementation is connector-aware mainly for qdrant:

1. optimizer translates a supported SQL filter subset into provider prefilter payload
2. unsupported filters trigger safe fallback (no rewrite), preserving correctness

Important nuance:

1. this is not yet a generalized multi-provider capability negotiation framework
2. it is a practical qdrant-focused subset with explicit fallback semantics

## 9.3 `VectorKnnExec` knobs and overrides

`VectorKnnExec` carries:

1. `k`
2. `metric`
3. `ef_search`
4. `prefilter`
5. `provider`

Knobs can come from:

1. table/optimizer options
2. DataFrame per-query overrides (`VectorKnnOverrides`)
3. direct hybrid plan construction APIs

This is the main v2 tuning surface for latency/recall tradeoffs in index-backed retrieval.

## 9.4 Batched query mode

`Engine::hybrid_search_batch(...)` allows multiple query vectors in one logical request.

Conceptually:

1. one API call produces a single hybrid logical node with multiple query vectors
2. planner/analyzer validate the vector batch shape
3. runtime/provider path can execute batched retrieval more efficiently than repeated one-query calls

Current state:

1. API and logical node wiring exist
2. public API contract tests cover availability
3. broader throughput benchmarking is still limited

## 9.5 Embedding provider plugin API

FFQ now exposes an embedding provider trait instead of forcing a vendor:

1. `EmbeddingProvider`
2. `Engine::embed_texts(&provider, texts)`

Built-in examples:

1. `SampleEmbeddingProvider` (deterministic, tests/examples)
2. `HttpEmbeddingProvider` (feature `embedding-http`)

Design intent:

1. keep vendor/model integration outside the engine core
2. let users supply local, remote, or custom providers

Current limitation:

1. embedding caching is not yet implemented

## What to read next

1. `docs/v2/vector-rag.md`
2. `docs/v2/api-contract.md`
3. `crates/client/src/engine.rs`
4. `crates/client/src/embedding.rs`

## Validation checklist

```bash
make test-13.1-vector
cargo test -p ffq-client --test embedded_two_phase_retrieval --features vector
cargo test -p ffq-client --test qdrant_routing --features "vector,qdrant"
cargo test -p ffq-client --test public_api_contract --features vector
```
