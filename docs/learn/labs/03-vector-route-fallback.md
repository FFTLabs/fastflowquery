# Lab 03: Vector Query Route vs Fallback

Goal: observe when FFQ uses index-backed vector routing (`VectorTopK`) and when it safely falls back to brute-force (`TopKByScore`).

## Prerequisites

1. Build with vector features available.
2. No live qdrant instance required for these planner-routing checks.

## Steps

1. Run qdrant-routing tests (positive + negative projection):

```bash
cargo test -p ffq-client --test qdrant_routing --features "vector,qdrant" -- --nocapture
```

2. Run embedded vector top-k tests (including fallback on parquet):

```bash
cargo test -p ffq-client --test embedded_vector_topk --features vector -- --nocapture
```

3. Optionally run vector optimizer golden snapshots:

```bash
cargo test -p ffq-planner --test optimizer_golden --features vector
```

## Expected Output

1. `qdrant_routing` assertions confirm:
   - supported projection uses `VectorTopK` and `rewrite=index_applied`.
   - unsupported projection falls back with `rewrite=index_fallback`.
2. `embedded_vector_topk` confirms deterministic ranking and fallback path on parquet tables.
3. Golden tests pass without snapshot drift (unless intentional planner changes were made).

## Troubleshooting

1. Feature mismatch errors:
   - ensure `--features "vector,qdrant"` for routing tests.
2. Snapshot diffs in optimizer tests:
   - inspect diffs carefully.
   - only bless when change is intentional:
     - `BLESS=1 cargo test -p ffq-planner --test optimizer_golden --features vector`
3. Unexpected fallback behavior:
   - confirm query shape and projection contract (`id`, `score`, `payload` only for index rewrite).
