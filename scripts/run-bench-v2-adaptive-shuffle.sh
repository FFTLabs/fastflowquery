#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export FFQ_BENCH_MODE="${FFQ_BENCH_MODE:-embedded}"
export FFQ_BENCH_INCLUDE_WINDOW=0
export FFQ_BENCH_INCLUDE_RAG=0
export FFQ_BENCH_INCLUDE_ADAPTIVE_SHUFFLE=1
export FFQ_BENCH_ADAPTIVE_SHUFFLE_MATRIX="${FFQ_BENCH_ADAPTIVE_SHUFFLE_MATRIX:-tiny;large;skewed;mixed}"

echo "Running v2 adaptive-shuffle benchmark matrix"
echo "Mode:                    ${FFQ_BENCH_MODE}"
echo "Adaptive shuffle matrix: ${FFQ_BENCH_ADAPTIVE_SHUFFLE_MATRIX}"
echo "Include window:          ${FFQ_BENCH_INCLUDE_WINDOW}"
echo "Include RAG:             ${FFQ_BENCH_INCLUDE_RAG}"

exec ./scripts/run-bench-13.3.sh
