#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

FIXTURE_ROOT="${FFQ_BENCH_FIXTURE_ROOT:-${ROOT_DIR}/tests/bench/fixtures}"
QUERY_ROOT="${FFQ_BENCH_QUERY_ROOT:-${ROOT_DIR}/tests/bench/queries}"
OUT_DIR="${FFQ_BENCH_OUT_DIR:-${ROOT_DIR}/tests/bench/results}"
WARMUP="${FFQ_BENCH_WARMUP:-1}"
ITERATIONS="${FFQ_BENCH_ITERATIONS:-3}"

export TZ=UTC
export LC_ALL=C

mkdir -p "${OUT_DIR}"

echo "Running embedded benchmark baseline (13.3)"
echo "Fixture root: ${FIXTURE_ROOT}"
echo "Query root:   ${QUERY_ROOT}"
echo "Output dir:   ${OUT_DIR}"
echo "Warmup:       ${WARMUP}"
echo "Iterations:   ${ITERATIONS}"

cargo run -p ffq-client --example run_bench_13_3 --features vector -- \
  --fixture-root "${FIXTURE_ROOT}" \
  --query-root "${QUERY_ROOT}" \
  --out-dir "${OUT_DIR}" \
  --warmup "${WARMUP}" \
  --iterations "${ITERATIONS}"
