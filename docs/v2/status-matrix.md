# Plan v2 -> Implementation Status Matrix

- Status: verified
- Owner: @ffq-docs
- Last Verified Commit: dd45319
- Last Verified Date: 2026-02-19

Source plan: `tickets/eng/Plan_v2.md`.

Status legend:
- `done`: implemented and validated with code + tests/docs/workflows.
- `partial`: implemented in part; acceptance criteria not fully closed.
- `not started`: no meaningful implementation evidence yet.

| Plan heading | Status | Evidence (code/workflow/docs) | Evidence (tests) | Gap note |
|---|---|---|---|---|
| `v2 Deliverables (short, to keep scope crisp)` | partial | `crates/client/src/ffi.rs`, `crates/client/src/python.rs`, `crates/client/src/engine.rs`, `crates/distributed/src/coordinator.rs` | `crates/client/tests/public_api_contract.rs`, `crates/client/tests/udf_api.rs`, `crates/client/tests/physical_registry.rs` | AQE + native hybrid node deliverables are not complete. |
| `EPIC 1 — Runtime & Portability (library-first stays core)` | partial | `Cargo.toml`, `crates/client/Cargo.toml`, `.github/workflows/feature-matrix.yml`, `crates/distributed/src/coordinator.rs` | `crates/distributed/src/coordinator.rs` (unit tests), `crates/client/tests/distributed_runtime_roundtrip.rs` | Release artifact pipeline is deferred. |
| `1.1 Stabilize single-binary & feature flags` | done | `Cargo.toml`, `crates/client/Cargo.toml`, `.github/workflows/feature-matrix.yml` | `.github/workflows/feature-matrix.yml` CI matrix commands | Release publishing itself is tracked under EPIC 11. |
| `1.2 Harden distributed runtime (cluster-ready, but optional)` | partial | `crates/distributed/src/coordinator.rs`, `crates/distributed/src/worker.rs`, `crates/distributed/proto/ffq_distributed.proto` | `crates/distributed/src/coordinator.rs` (`coordinator_requeues_tasks_from_stale_worker`, capability routing test) | End-to-end worker-kill recovery acceptance is not fully documented/closed. |
| `EPIC 2 — Public API, FFI & Python Bindings` | done | `crates/client/src/engine.rs`, `crates/client/src/ffi.rs`, `crates/client/src/python.rs`, `docs/dev/api-semver-policy.md` | `crates/client/tests/public_api_contract.rs`, `crates/client/tests/udf_api.rs`, `crates/client/tests/physical_registry.rs` | - |
| `2.1 Versioned API surface + SemVer rules` | done | `docs/dev/api-semver-policy.md`, `.github/workflows/api-semver.yml`, `crates/client/src/engine.rs` | `crates/client/tests/public_api_contract.rs` | - |
| 2.2 Stable C ABI (`ffi` feature) | done | `crates/client/src/ffi.rs`, `include/ffq_ffi.h`, `docs/dev/ffi-c-api.md`, `examples/c/ffi_example.c` | `make ffi-example` path in `Makefile` | - |
| `2.3 Python bindings (mandatory for v2)` | done | `crates/client/src/python.rs`, `pyproject.toml`, `docs/dev/python-bindings.md`, `.github/workflows/python-wheels.yml` | wheel smoke in `.github/workflows/python-wheels.yml` | - |
| `2.4 Pluggable hooks + UDF API` | done | `crates/client/src/engine.rs`, `crates/client/src/planner_facade.rs`, `crates/execution/src/udf.rs`, `crates/execution/src/physical_registry.rs` | `crates/client/tests/udf_api.rs`, `crates/planner/tests/optimizer_custom_rule.rs`, `crates/client/tests/physical_registry.rs` | - |
| `EPIC 3 — SQL & Semantics Extensions` | not started | Gap: no EPIC-3 implementation tracked yet. | Gap | No outer join/CASE/CTE/window v2 implementation evidence. |
| `3.1 Outer joins` | not started | Gap | Gap | No join-type extension evidence. |
| `3.2 CASE expressions` | not started | Gap | Gap | No CASE implementation evidence. |
| `3.3 CTEs & subqueries (MVP)` | not started | Gap | Gap | No CTE/subquery MVP evidence. |
| `3.4 Window functions (MVP)` | not started | Gap | Gap | No window exec evidence. |
| `EPIC 4 — AQE (Adaptive Query Execution)` | not started | Gap | Gap | AQE plumbing not implemented. |
| `4.1 Runtime stats plumbing` | not started | Gap | Gap | No adaptive stats pipeline evidence. |
| `4.2 Adaptive join choice` | not started | Gap | Gap | No adaptive subtree swap evidence. |
| `4.3 Adaptive shuffle partitions (MVP)` | not started | Gap | Gap | No adaptive partition count evidence. |
| `4.4 Skew handling (MVP)` | not started | Gap | Gap | No skew mitigation evidence. |
| `EPIC 5 — Join System v2` | not started | Gap | Gap | v2 join system work not started. |
| `5.1 Radix-partitioned hash join` | not started | Gap | Gap | No radix join evidence. |
| `5.2 Bloom filter pushdown` | not started | Gap | Gap | No bloom pushdown evidence. |
| `5.3 Sort-merge join (targeted)` | not started | Gap | Gap | No SMJ evidence. |
| `5.4 Semi/anti joins (optional)` | not started | Gap | Gap | No semi/anti join evidence. |
| `EPIC 6 — Aggregation v2` | not started | Gap | Gap | v2 agg roadmap not started. |
| `6.1 Streaming hash agg + robust spill` | not started | Gap | Gap | No v2 streaming spill redesign evidence. |
| `6.2 Distinct aggregation (two-phase)` | not started | Gap | Gap | No two-phase distinct evidence. |
| `6.3 Optional: approx aggregates / grouping sets` | not started | Gap | Gap | No approx/grouping sets evidence. |
| `EPIC 7 — Shuffle & Distributed Execution v2` | partial | `crates/distributed/src/worker.rs`, `crates/distributed/src/coordinator.rs`, `crates/distributed/src/grpc.rs` | `crates/distributed/src/coordinator.rs` tests, `crates/distributed/src/grpc.rs` tests | Capability-aware scheduling and pipelined-shuffle MVP are implemented; compression/zero-copy/speculation/memory-manager tracks remain open. |
| `7.1 Shuffle compression` | not started | Gap | Gap | No shuffle compression evidence. |
| `7.2 Pipelined shuffle (MVP)` | done | `crates/distributed/src/coordinator.rs`, `crates/distributed/src/worker.rs`, `crates/distributed/src/grpc.rs`, `crates/distributed/proto/ffq_distributed.proto` | `coordinator_allows_pipelined_reduce_assignment_when_partition_ready`, `coordinator_pipeline_requires_committed_offset_threshold_before_scheduling`, `coordinator_backpressure_throttles_assignment_windows`, `worker_shuffle_fetch_respects_committed_watermark_and_emits_eof_marker` | MVP closes control-plane/worker streaming readiness, incremental fetch cursors, and backpressure windows; deeper transport optimization remains under `7.3`. |
| `7.3 Fewer copies / network path` | not started | Gap | Gap | No copy-minimization benchmark evidence. |
| `7.4 Speculative execution + better scheduling` | not started | Gap | Gap | No speculative execution evidence. |
| `7.5 Memory/Spill Manager` | not started | Gap | Gap | No centralized memory manager evidence. |
| `EPIC 8 — Storage & IO v2` | not started | Gap | Gap | v2 storage roadmap not implemented. |
| `8.1 Partitioned tables + partition pruning` | not started | Gap | Gap | No partition-pruning evidence. |
| `8.2 Statistics collection` | not started | Gap | Gap | No file-stats optimizer integration evidence. |
| `8.3 File-level caching` | not started | Gap | Gap | No cache layer evidence. |
| `8.4 Object storage “production-grade”` | not started | Gap | Gap | No production hardening evidence for object storage. |
| `EPIC 9 — RAG / Hybrid Search v2 (True Hybrid Engine)` | not started | Gap | Gap | v1 vector paths exist; v2 hybrid node work not started. |
| `9.1 Hybrid plan node + score column` | not started | Gap | Gap | No `HybridVectorScan`/`VectorKnnExec` evidence. |
| `9.2 Prefilter pushdown (connector-aware)` | not started | Gap | Gap | No v2 connector capability negotiation evidence. |
| 9.3 `VectorKnnExec` knobs | not started | Gap | Gap | No v2 knob surface evidence. |
| `9.4 Batched query mode` | not started | Gap | Gap | No batched vector query API evidence. |
| `9.5 Stable embedding API (provider/plugin)` | not started | Gap | Gap | No embedding provider trait evidence. |
| `EPIC 10 — Observability & Developer UX v2` | not started | Gap | Gap | v1 observability exists; v2 UX scope not started. |
| `10.1 Dashboard endpoint / Web UI MVP` | not started | Gap | Gap | No dashboard endpoint evidence. |
| `10.2 Explain: logical/physical/adaptive` | not started | Gap | Gap | No adaptive explain evidence. |
| `10.3 Profiling artifacts` | not started | Gap | Gap | No per-query profile artifact flow evidence. |
| `EPIC 11 — Release Pipeline (Deferred)` | partial | `.github/workflows/python-wheels.yml`, `docs/dev/python-bindings.md` | wheel smoke in workflow | Deferred epic; only wheel workflow pieces exist. |
| `11.1 Release Contract + Versioning Policy` | not started | Gap | Gap | No `docs/release/README.md` contract page yet. |
| `11.2 Server Binary Packaging Workflow` | not started | Gap | Gap | No dedicated release-binaries workflow yet. |
| `11.3 Crate Publish Pipeline` | not started | Gap | Gap | No publish orchestration script/workflow yet. |
| `11.4 Python Binding Crate Scaffold` | partial | `crates/client/src/python.rs`, `pyproject.toml` | `.github/workflows/python-wheels.yml` | Uses current crate integration; dedicated `crates/python` scaffold not present. |
| `11.5 Python Wheels CI Build` | done | `.github/workflows/python-wheels.yml`, `docs/dev/python-bindings.md` | workflow smoke install/run | - |
| `11.6 Unified Release Orchestration` | not started | Gap | Gap | No unified `release.yml` orchestration evidence. |
| `11.7 GitHub Release Publishing` | not started | Gap | Gap | No GH release asset pipeline evidence. |
| `11.8 PyPI Publish (Optional Toggle)` | not started | Gap | Gap | No PyPI publish lane evidence. |
| `11.9 Release Verification + Smoke Tests` | partial | `.github/workflows/python-wheels.yml` | wheel smoke | Full cross-artifact smoke suite not implemented. |
| `11.10 Operator Runbook + Troubleshooting` | not started | Gap | Gap | No release runbook docs yet. |
| `Implementation as vertical slices (v2 order)` | partial | `crates/client/src/ffi.rs`, `crates/client/src/python.rs`, `crates/execution/src/physical_registry.rs`, `crates/distributed/src/coordinator.rs` | `crates/client/tests/udf_api.rs`, `crates/client/tests/physical_registry.rs` | Slice 1 mostly done; slices 2-9 largely not started. |

## Notes

1. This matrix is tied to current repository state and should be updated as each v2 ticket lands.
2. Headings are mapped from `tickets/eng/Plan_v2.md` and appear once each in the table above.

## DOCV2-17 Audit Record

Structured audit executed for v2 standalone guarantee:

1. required v2 page existence check: `python3 scripts/validate-docs-v2.py` -> pass
2. markdown link/anchor integrity check (v2 docs + root entry docs): pass
3. Plan_v2 heading coverage lint vs this matrix: pass
4. root/contributor entrypoint policy update (`Readme.md`, `Contributing.md`): complete
5. learner-track synchronization for v2 runtime/control-plane/extensibility: complete

### Closures (this audit)

1. v2 docs guardrail CI added: `.github/workflows/docs-v2-guardrails.yml`
2. local guardrail command added: `make docs-v2-guardrails`
3. migration, quickstart, testing, API, runtime, bindings, extensibility, deployment docs now exist in `docs/v2/*`
4. contributor policy explicitly requires v2 doc updates on behavior/API/config/runtime changes

### Unresolved gaps (tracked)

1. `docs/v2/distributed-capabilities.md` is still placeholder (`TBD` sections) and should be completed.
2. Many `docs/v2/*` metadata headers still have `Last Verified Commit/Date: TBD`; process-level follow-up is needed to keep verification metadata current.
3. Plan_v2 epics not implemented in code (for example EPIC 3+, most of EPIC 4-11) remain intentionally documented as `not started`/`partial`.

### Sign-off

Sign-off for implemented scope:

1. v2 documentation is self-sufficient for currently implemented v2 scope (EPIC 1/2 plus completed docs tracks), without requiring `docs/v1/*` for execution or contributor workflow.
2. unresolved items above are explicitly tracked and do not block standalone use of implemented scope.
