# Hands-On Labs

These labs are practical exercises for FFQ learners. They are designed to be run in order and map directly to core v1 capabilities.

## Lab List

1. `docs/learn/labs/01-single-node-query.md`
   - Run and inspect a single-node embedded query over parquet.
2. `docs/learn/labs/02-distributed-run.md`
   - Run the same integration path through coordinator + 2 workers.
3. `docs/learn/labs/03-vector-route-fallback.md`
   - Observe vector route decisions (`index_applied` vs `index_fallback`).
4. `docs/learn/labs/04-official-benchmark-correctness-gate.md`
   - Run official TPC-H benchmark flow and verify correctness gating.

## General Prerequisites

1. Run commands from repository root.
2. Rust toolchain installed (`cargo`).
3. Docker + Compose installed for distributed labs.
4. Python 3 available for helper scripts.

Quick checks:

```bash
cargo --version
docker --version
docker compose version
python3 --version
```

## Output Convention

For each lab, treat these as success criteria:

1. Command exits with status `0`.
2. Expected artifact/test output appears.
3. Troubleshooting checks return expected health/status values.
