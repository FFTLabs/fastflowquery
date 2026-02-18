# Known Gaps, Risks, and Next Steps

This page tracks current v1 limitations and deferred work.  
Each gap includes impact, workaround, and a proposed follow-up ticket.

## Gap Register

| Gap | Impact | Current workaround | Proposed next ticket |
|---|---|---|---|
| SQL subset is intentionally narrow (`SELECT` + `INSERT INTO ... SELECT`) | Many common SQL constructs are unavailable, which limits portability of existing queries. | Rewrite queries to v1 subset and use DataFrame API for some compositions. | `V2-SQL-01` Expand SQL coverage (CTE/subquery/order-by generalization/set ops). |
| `SELECT *` is unsupported | Existing exploratory queries fail unless all columns are listed. | Use explicit projection columns. | `V2-SQL-02` Add wildcard expansion in analyzer/planner. |
| Join support is `INNER JOIN` equi-join only | Left/right/full joins and non-equi predicates cannot run. | Pre-filter and rewrite to inner equi-join where possible. | `V2-JOIN-01` Add outer joins and non-equi join support. |
| Global ORDER BY is not implemented (only vector top-k pattern) | Non-vector sorted result workloads are blocked. | Restrict to `ORDER BY cosine_similarity(...) DESC LIMIT k` for vector ranking. | `V2-EXEC-01` Add full sort operator and planner lowering. |
| Optimizer remains rule-based with conservative pruning around aggregates | Suboptimal plans and unnecessary column/materialization cost on larger workloads. | Tune table stats/options and rely on existing pushdown passes. | `V2-OPT-01` Add cost-based planning and stronger aggregate/projection pruning. |
| Distributed worker does not execute `CoalesceBatches` | Some physical plans that include this node cannot run in distributed mode. | Avoid generating/distributing plans that require it. | `V2-DIST-01` Implement `CoalesceBatches` in distributed worker executor. |
| Distributed shuffle path requires numeric `query_id` for layout | Runtime coupling creates fragility when integrating external query-id formats. | Use numeric IDs in distributed query submission path. | `V2-DIST-02` Decouple shuffle layout from numeric query ID constraint. |
| Scheduler/blacklisting is basic | Less robust behavior under noisy worker failures and skewed cluster conditions. | Manual operator oversight and conservative deployment. | `V2-DIST-03` Add robust scheduling policies, adaptive blacklisting, and recovery heuristics. |
| Object store provider is experimental and scan is not implemented | `s3`/cloud table reads are not production-ready. | Use parquet local paths for v1 correctness flows. | `V2-STORAGE-01` Implement object-store scan/read path with auth and retries. |
| Catalog persistence is local-file based (`tables.json/toml`) | Single-node metadata authority; weak multi-process coordination. | Use one catalog owner process and managed restart flow. | `V2-CATALOG-01` Add durable catalog backend and concurrency controls. |
| Vector rewrite contract is strict (`id, score, payload` projection only) | Useful projections can fall back to brute-force unexpectedly. | Use supported projection or two-phase retrieval path. | `V2-VECTOR-01` Support projection enrichment from payload/doc lookup in rewrite path. |
| Qdrant filter pushdown supports only equality + `AND` | Range/OR/complex predicates skip index rewrite and can degrade performance. | Keep filter subset simple or accept brute-force fallback. | `V2-VECTOR-02` Extend predicate translator to broader qdrant filter subset. |
| Qdrant UUID IDs are unsupported | Some index datasets cannot be queried through current connector. | Use numeric point IDs for v1 collections. | `V2-VECTOR-03` Add UUID id support in `VectorTopK` data contract and connector. |
| Official benchmark scope is limited to TPC-H Q1/Q3 | Release/perf reporting does not yet cover broader official TPC-H query families. | Use current official Q1/Q3 path for v1, and run synthetic matrices for broader stress coverage. | `V2-PERF-01` Extend official benchmark suite beyond Q1/Q3 with deterministic contracts. |
| Metrics label cardinality includes query/task IDs | Long-running environments can produce high-cardinality Prometheus series. | Use short retention and selective scrape environments for v1. | `V2-OBS-01` Add configurable metrics cardinality controls/sampling. |
| Security and multi-tenant hardening are minimal | Distributed runtime is not suitable for untrusted/multi-tenant production use. | Run in trusted network and controlled environments only. | `V2-SEC-01` Add authn/authz, TLS, quotas, and tenant isolation controls. |

## Risk Summary

1. Highest near-term operational risk: distributed scheduler/coordinator hardening and numeric query-id coupling.
2. Highest product-surface risk: limited SQL + global sort absence for non-vector analytical workflows.
3. Highest scale risk: limited official benchmark coverage (Q1/Q3 only) and high-cardinality metrics defaults.

## Suggested Sequencing (v2)

1. Stabilize distributed execution hardening (`V2-DIST-*`).
2. Expand SQL and core operator coverage (`V2-SQL-*`, `V2-EXEC-01`, `V2-JOIN-01`).
3. Improve storage/catalog durability and connectors (`V2-STORAGE-*`, `V2-CATALOG-*`).
4. Expand vector capabilities and connector compatibility (`V2-VECTOR-*`).
5. Add benchmark and observability scalability controls (`V2-PERF-*`, `V2-OBS-*`).
