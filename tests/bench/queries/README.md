# Benchmark Query Pack (13.3.3)

Canonical benchmark SQL files:

1. `canonical/tpch_q1.sql`
2. `canonical/tpch_q3.sql`
3. `rag_topk_bruteforce.sql`
4. `rag_topk_qdrant.sql` (optional qdrant path)
5. `rag_topk_bruteforce.template.sql` (RAG matrix variants)
6. `rag_topk_qdrant.template.sql` (optional qdrant matrix variants)
7. `window/window_narrow_partitions.sql`
8. `window/window_wide_partitions.sql`
9. `window/window_skewed_keys.sql`
10. `window/window_many_expressions.sql`

Benchmark runners should load these files directly so query text stays centralized and versioned.

TPC-H adaptation notes are embedded in the canonical SQL headers and are part of the contract for
official v1 benchmark runs.
