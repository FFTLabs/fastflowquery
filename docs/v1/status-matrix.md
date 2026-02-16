# Plan v1 -> Implementation Status Matrix

Source plan: `tickets/eng/Plan_v1.md`.

Status legend:
- `done`: implemented and validated with code + tests.
- `partial`: implemented in core path, but scope/acceptance is only partially covered.
- `not started`: no meaningful implementation found yet.

| Plan section | Status | Evidence (code) | Evidence (tests) |
|---|---|---|---|
| `0) v1 Deliverables (Definition of Done translated into tasks)` | partial | `crates/client/src/engine.rs`, `crates/client/src/runtime.rs`, `crates/distributed/src/coordinator.rs`, `crates/common/src/metrics.rs` | `crates/client/tests/distributed_runtime_roundtrip.rs`, `crates/client/tests/embedded_hash_join.rs` |
| `1) Repo & Feature Flags (skeleton first)` | done | `Cargo.toml`, `crates/client/Cargo.toml`, `crates/distributed/Cargo.toml`, `crates/common/Cargo.toml` | `crates/client/tests/distributed_runtime_roundtrip.rs` |
| `1.1 Cargo workspace + crates` | done | `Cargo.toml`, `crates/*/Cargo.toml` | `crates/planner/tests/physical_plan_serde.rs` |
| `1.2 Feature flags (v1)` | done | `crates/client/Cargo.toml`, `crates/distributed/Cargo.toml`, `crates/storage/Cargo.toml`, `crates/common/Cargo.toml` | `crates/client/tests/qdrant_routing.rs`, `crates/client/tests/embedded_vector_topk.rs` |
| `2) Public API & Lifecycle (Engine Core)` | done | `crates/client/src/engine.rs`, `crates/client/src/session.rs` | `crates/client/tests/embedded_parquet_scan.rs` |
| `2.1 API shape` | done | `crates/client/src/engine.rs`, `crates/client/src/dataframe.rs`, `crates/common/src/config.rs` | `crates/client/tests/dataframe_write_api.rs` |
| `2.2 Runtime abstraction` | done | `crates/client/src/runtime.rs` | `crates/client/tests/distributed_runtime_roundtrip.rs` |
| `3) SQL & DataFrame Frontend (v1 subset)` | done | `crates/planner/src/sql_frontend.rs`, `crates/client/src/dataframe.rs` | `crates/client/tests/embedded_parquet_scan.rs` |
| `3.1 SQL parsing + parameter binding (for RAG query vector)` | done | `crates/planner/src/sql_frontend.rs`, `crates/client/src/engine.rs` | `crates/planner/src/sql_frontend.rs` |
| `3.2 Minimal DataFrame API` | done | `crates/client/src/dataframe.rs` | `crates/client/tests/dataframe_write_api.rs` |
| `4) Logical Plan + Analyzer + Optimizer (rule-based)` | done | `crates/planner/src/logical_plan.rs`, `crates/planner/src/analyzer.rs`, `crates/planner/src/optimizer.rs` | `crates/planner/src/optimizer.rs`, `crates/planner/src/sql_frontend.rs` |
| `4.1 LogicalPlan types (exactly your list)` | done | `crates/planner/src/logical_plan.rs` | `crates/planner/src/sql_frontend.rs` |
| `4.2 Analyzer` | done | `crates/planner/src/analyzer.rs` | `crates/planner/src/analyzer.rs` |
| `4.3 Optimizer passes (in order, v1)` | partial | `crates/planner/src/optimizer.rs` | `crates/planner/src/optimizer.rs` |
| `5) Physical Planner + Operator Graph` | done | `crates/planner/src/physical_plan.rs`, `crates/planner/src/physical_planner.rs` | `crates/planner/tests/physical_plan_serde.rs` |
| `5.1 PhysicalPlan nodes` | done | `crates/planner/src/physical_plan.rs` | `crates/planner/tests/physical_plan_serde.rs` |
| `5.2 Planning rules (v1)` | done | `crates/planner/src/physical_planner.rs` | `crates/client/tests/embedded_hash_aggregate.rs`, `crates/client/tests/embedded_hash_join.rs` |
| `6) Execution Core (columnar, streaming, backpressure)` | partial | `crates/execution/src/stream.rs`, `crates/client/src/runtime.rs`, `crates/distributed/src/worker.rs` | `crates/client/tests/embedded_hash_aggregate.rs` |
| `6.1 ExecNode interface` | partial | `crates/storage/src/provider.rs`, `crates/execution/src/stream.rs` | `crates/client/tests/embedded_parquet_scan.rs` |
| `6.2 Expressions (v1 + vector)` | done | `crates/execution/src/expressions/mod.rs` | `crates/client/tests/embedded_vector_topk.rs` |
| `7) Storage & Catalog (portable)` | done | `crates/storage/src/provider.rs`, `crates/storage/src/catalog.rs`, `crates/storage/src/parquet_provider.rs` | `crates/client/tests/embedded_parquet_scan.rs` |
| 7.1 `StorageProvider` trait | partial | `crates/storage/src/provider.rs`, `crates/storage/src/parquet_provider.rs`, `crates/storage/src/object_store_provider.rs` | `crates/client/tests/embedded_parquet_scan.rs` |
| `7.2 Catalog` | done | `crates/storage/src/catalog.rs` | `crates/client/tests/embedded_parquet_scan.rs` |
| `8) Joins & Aggregations (including minimal spill)` | done | `crates/client/src/runtime.rs`, `crates/distributed/src/worker.rs` | `crates/client/tests/embedded_hash_aggregate.rs`, `crates/client/tests/embedded_hash_join.rs` |
| `8.1 HashAggregate (two-phase)` | done | `crates/client/src/runtime.rs`, `crates/distributed/src/worker.rs` | `crates/client/tests/embedded_hash_aggregate.rs` |
| `8.2 HashJoin` | done | `crates/client/src/runtime.rs`, `crates/distributed/src/worker.rs` | `crates/client/tests/embedded_hash_join.rs` |
| 9) Shuffle & Stage Model (only when `distributed` feature is enabled) | done | `crates/distributed/src/stage.rs`, `crates/shuffle/src/layout.rs`, `crates/shuffle/src/writer.rs`, `crates/shuffle/src/reader.rs` | `crates/distributed/src/stage.rs`, `crates/shuffle/src/writer.rs` |
| `9.1 Exchange as a stage boundary` | done | `crates/distributed/src/stage.rs` | `crates/distributed/src/stage.rs` |
| `9.2 Shuffle format` | done | `crates/shuffle/src/layout.rs`, `crates/shuffle/src/writer.rs`, `crates/shuffle/src/reader.rs` | `crates/shuffle/src/writer.rs` |
| `9.3 Retry + cleanup` | done | `crates/shuffle/src/writer.rs`, `crates/distributed/src/coordinator.rs` | `crates/shuffle/src/writer.rs` |
| `10) Distributed Runtime (Coordinator/Worker) — minimal but correct` | done | `crates/distributed/src/grpc.rs`, `crates/distributed/src/coordinator.rs`, `crates/distributed/src/worker.rs` | `crates/client/tests/distributed_runtime_roundtrip.rs`, `crates/distributed/src/coordinator.rs` |
| `10.1 Protos (tonic)` | done | `crates/distributed/proto/ffq_distributed.proto`, `crates/distributed/src/grpc.rs` | `crates/client/tests/distributed_runtime_roundtrip.rs` |
| `10.2 Coordinator` | done | `crates/distributed/src/coordinator.rs` | `crates/distributed/src/coordinator.rs` |
| `10.3 Worker` | done | `crates/distributed/src/worker.rs` | `crates/client/tests/distributed_runtime_roundtrip.rs` |
| `Ticket 10.4.1 - Real plan execution` | done | `crates/distributed/src/worker.rs` | `crates/client/tests/distributed_runtime_roundtrip.rs` |
| `Ticket 10.4.2 — Distributed Client Runtime Returns Real Results` | done | `crates/client/src/runtime.rs`, `crates/distributed/src/grpc.rs`, `crates/distributed/src/coordinator.rs` | `crates/client/tests/distributed_runtime_roundtrip.rs` |
| Ticket 10.5 — SQL DML: `INSERT INTO ... SELECT ...` | done | `crates/planner/src/sql_frontend.rs`, `crates/planner/src/analyzer.rs`, `crates/planner/src/logical_plan.rs` | `crates/planner/src/sql_frontend.rs`, `crates/planner/src/analyzer.rs` |
| `Ticket 10.6 — Logical/Physical Sink Operators` | done | `crates/planner/src/logical_plan.rs`, `crates/planner/src/physical_plan.rs`, `crates/planner/src/physical_planner.rs`, `crates/client/src/runtime.rs` | `crates/client/tests/embedded_parquet_sink.rs` |
| `Ticket 10.7 — User-Facing Write API in Client` | done | `crates/client/src/dataframe.rs` | `crates/client/tests/dataframe_write_api.rs` |
| `Ticket 10.8 — Durable Table Writes + Commit Semantics` | done | `crates/client/src/dataframe.rs`, `crates/storage/src/catalog.rs` | `crates/client/tests/dataframe_write_api.rs` |
| `11) RAG / Hybrid Search v1 implementation plan` | done | `crates/execution/src/expressions/mod.rs`, `crates/planner/src/optimizer.rs`, `crates/storage/src/vector_index.rs` | `crates/client/tests/embedded_vector_topk.rs`, `crates/client/tests/qdrant_routing.rs` |
| 11.1 Vector data type + expressions (feature `vector`) | done | `crates/execution/src/expressions/mod.rs`, `crates/planner/src/sql_frontend.rs` | `crates/client/tests/embedded_vector_topk.rs` |
| `11.2 Brute-force rerank operator (without global ORDER BY)` | done | `crates/planner/src/physical_plan.rs`, `crates/client/src/runtime.rs`, `crates/distributed/src/worker.rs` | `crates/client/tests/embedded_vector_topk.rs` |
| 11.3 Optional: Qdrant connector (feature `qdrant`) | done | `crates/storage/src/vector_index.rs`, `crates/storage/src/qdrant_provider.rs`, `crates/planner/src/physical_plan.rs` | `crates/client/tests/qdrant_routing.rs` |
| `Ticket 11.3.1 — Planner Context Exposes Table Metadata` | done | `crates/client/src/dataframe.rs`, `crates/planner/src/optimizer.rs` | `crates/client/tests/qdrant_routing.rs` |
| `Ticket 11.3.2 — Qdrant Rewrite Rule (TopKByScore -> VectorTopK)` | done | `crates/planner/src/optimizer.rs` | `crates/client/tests/qdrant_routing.rs` |
| `Ticket 11.3.3 — Projection Contract for VectorTopK Results` | done | `crates/planner/src/optimizer.rs` | `crates/client/tests/qdrant_routing.rs` |
| `Ticket 11.3.4 — Filter Pushdown to Qdrant (v1 subset)` | done | `crates/planner/src/optimizer.rs`, `crates/storage/src/qdrant_provider.rs` | `crates/client/tests/qdrant_routing.rs` |
| `Ticket 11.3.5 — Safe Fallback Semantics` | done | `crates/planner/src/optimizer.rs` | `crates/client/tests/qdrant_routing.rs` |
| `Ticket 11.3.6 — End-to-End Qdrant Routing Tests` | done | `crates/client/src/runtime.rs`, `crates/distributed/src/worker.rs` | `crates/client/tests/qdrant_routing.rs` |
| `11.4 Two-phase retrieval pipeline (plan pattern)` | done | `crates/planner/src/optimizer.rs`, `crates/client/src/runtime.rs`, `crates/distributed/src/worker.rs` | `crates/client/tests/embedded_two_phase_retrieval.rs`, `crates/client/tests/distributed_runtime_roundtrip.rs` |
| `12) Observability (tracing + metrics + profiling)` | done | `crates/client/src/runtime.rs`, `crates/distributed/src/worker.rs`, `crates/common/src/metrics.rs`, `crates/common/src/metrics_exporter.rs` | `crates/common/src/metrics.rs`, `crates/common/src/metrics_exporter.rs` |
| `12.1 Tracing` | done | `crates/client/src/runtime.rs`, `crates/distributed/src/coordinator.rs`, `crates/distributed/src/worker.rs` | `crates/client/tests/distributed_runtime_roundtrip.rs` |
| `12.2 Metrics (Prometheus)` | done | `crates/common/src/metrics.rs`, `crates/client/src/engine.rs`, `crates/distributed/src/coordinator.rs`, `crates/distributed/src/worker.rs` | `crates/common/src/metrics.rs` |
| `12.3 Profiling hooks` | done | `crates/common/src/metrics_exporter.rs`, `crates/client/src/engine.rs`, `crates/client/src/runtime.rs`, `crates/distributed/src/worker.rs` | `crates/common/src/metrics_exporter.rs` |
| `13) Test plan & benchmarks (v1)` | partial | `crates/client/tests`, `crates/planner/tests` | `crates/client/tests/embedded_hash_join.rs`, `crates/client/tests/distributed_runtime_roundtrip.rs` |
| `13.1 Correctness` | done | `crates/planner/src/optimizer.rs`, `crates/client/src/runtime.rs`, `crates/client/tests/support/mod.rs`, `Makefile`, `.github/workflows/correctness-13_1.yml` | `crates/client/tests/embedded_hash_aggregate.rs`, `crates/client/tests/embedded_hash_join.rs`, `crates/client/tests/distributed_runtime_roundtrip.rs`, `crates/client/tests/embedded_vector_topk.rs`, `crates/planner/tests/optimizer_golden.rs` |
| `Ticket 13.1.1 — Deterministic Test Harness Utilities` | done | `crates/client/tests/support/mod.rs` | `crates/client/tests/embedded_hash_join.rs`, `crates/client/tests/embedded_hash_aggregate.rs` |
| `Ticket 13.1.2 — Optimizer Golden Snapshot Framework` | done | `crates/planner/tests/optimizer_golden.rs` | `crates/planner/tests/snapshots/optimizer/*.snap` |
| `Ticket 13.1.3 — Golden Tests: Core Rule Coverage` | done | `crates/planner/tests/optimizer_golden.rs` | `crates/planner/tests/snapshots/optimizer/*.snap` |
| `Ticket 13.1.4 — Join Correctness Determinism (Embedded)` | done | `crates/client/tests/support/mod.rs` | `crates/client/tests/embedded_hash_join.rs`, `crates/client/tests/snapshots/join/*.snap` |
| `Ticket 13.1.5 — Aggregate Correctness Determinism (Embedded)` | done | `crates/client/tests/support/mod.rs` | `crates/client/tests/embedded_hash_aggregate.rs`, `crates/client/tests/snapshots/aggregate/*.snap` |
| `Ticket 13.1.6 — Join/Agg Determinism Parity (Distributed vs Embedded)` | done | `crates/client/src/runtime.rs`, `crates/distributed/src/worker.rs` | `crates/client/tests/distributed_runtime_roundtrip.rs` |
| `Ticket 13.1.7 — Vector Kernel Unit Tests (cosine/l2/dot)` | done | `crates/execution/src/expressions/mod.rs` | `crates/execution/src/expressions/mod.rs` |
| `Ticket 13.1.8 — Vector Ranking Correctness Tests` | done | `crates/client/src/runtime.rs` | `crates/client/tests/embedded_vector_topk.rs` |
| `Ticket 13.1.9 — CI/Command Matrix for Correctness Suite` | done | `Makefile`, `.github/workflows/correctness-13_1.yml` | `docs/v1/testing.md` |
| `Ticket 13.1.10 — Correctness Contract Doc Page` | done | `docs/v1/testing.md` | `docs/v1/testing.md` |
| `13.2 Integration` | partial | `crates/client/tests/distributed_runtime_roundtrip.rs` | `crates/client/tests/distributed_runtime_roundtrip.rs` |
| `13.3 Bench` | not started | `Makefile` | `tickets/eng/Plan_v1.md` |
| `14) Implementation as vertical slices (v1, aligned to the feature list)` | partial | `crates/client/src/engine.rs`, `crates/client/src/runtime.rs`, `crates/distributed/src/worker.rs` | `crates/client/tests/embedded_parquet_scan.rs`, `crates/client/tests/distributed_runtime_roundtrip.rs` |
| `Slice 1: Engine core + embedded execution` | done | `crates/client/src/engine.rs`, `crates/client/src/runtime.rs`, `crates/storage/src/parquet_provider.rs` | `crates/client/tests/embedded_parquet_scan.rs` |
| `Slice 2: Aggregation + join (embedded)` | done | `crates/client/src/runtime.rs` | `crates/client/tests/embedded_hash_aggregate.rs`, `crates/client/tests/embedded_hash_join.rs` |
| `Slice 3: Distributed optional (Coordinator/Worker)` | done | `crates/distributed/src/grpc.rs`, `crates/distributed/src/coordinator.rs`, `crates/distributed/src/worker.rs` | `crates/client/tests/distributed_runtime_roundtrip.rs` |
| `Slice 4: RAG v1` | done | `crates/execution/src/expressions/mod.rs`, `crates/planner/src/optimizer.rs`, `crates/storage/src/qdrant_provider.rs` | `crates/client/tests/embedded_vector_topk.rs`, `crates/client/tests/qdrant_routing.rs`, `crates/client/tests/embedded_two_phase_retrieval.rs` |
| `Slice 5: Hardening` | partial | `crates/distributed/src/coordinator.rs`, `crates/client/src/dataframe.rs`, `crates/common/src/metrics.rs` | `crates/client/tests/dataframe_write_api.rs`, `crates/common/src/metrics.rs` |

## Notes

1. This matrix is tied to current repository state and is intended to be updated as part of each new ticket.
2. `13.3 Bench` is marked `not started` because there is no dedicated benchmark harness/page/target yet.
