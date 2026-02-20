#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export FFQ_BENCH_MODE="${FFQ_BENCH_MODE:-embedded}"
export FFQ_BENCH_INCLUDE_WINDOW=1
export FFQ_BENCH_INCLUDE_RAG=0
export FFQ_BENCH_WINDOW_MATRIX="${FFQ_BENCH_WINDOW_MATRIX:-narrow;wide;skewed;many_exprs}"

echo "Running v2 window benchmark matrix"
echo "Mode:           ${FFQ_BENCH_MODE}"
echo "Window matrix:  ${FFQ_BENCH_WINDOW_MATRIX}"
echo "Include RAG:    ${FFQ_BENCH_INCLUDE_RAG}"

exec ./scripts/run-bench-13.3.sh
