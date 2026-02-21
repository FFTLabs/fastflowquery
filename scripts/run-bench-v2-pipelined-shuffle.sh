#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${FFQ_BENCH_OUT_DIR:-${ROOT_DIR}/tests/bench/results}"

ROWS="${FFQ_PIPE_TTFR_ROWS:-300000}"
SHUFFLE_PARTITIONS="${FFQ_PIPE_TTFR_SHUFFLE_PARTITIONS:-64}"
WARMUP="${FFQ_PIPE_TTFR_WARMUP:-1}"
ITERATIONS="${FFQ_PIPE_TTFR_ITERATIONS:-3}"

echo "Running v2 pipelined-shuffle TTFR benchmark"
echo "rows=${ROWS} shuffle_partitions=${SHUFFLE_PARTITIONS} warmup=${WARMUP} iterations=${ITERATIONS}"

mkdir -p "${OUT_DIR}"

cargo run -p ffq-client --example bench_pipelined_shuffle_ttfr --features distributed -- \
  --out-dir "${OUT_DIR}" \
  --rows "${ROWS}" \
  --shuffle-partitions "${SHUFFLE_PARTITIONS}" \
  --warmup "${WARMUP}" \
  --iterations "${ITERATIONS}"
