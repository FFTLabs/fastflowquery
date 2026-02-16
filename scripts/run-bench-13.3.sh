#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

FIXTURE_ROOT="${FFQ_BENCH_FIXTURE_ROOT:-${ROOT_DIR}/tests/bench/fixtures}"
QUERY_ROOT="${FFQ_BENCH_QUERY_ROOT:-${ROOT_DIR}/tests/bench/queries}"
OUT_DIR="${FFQ_BENCH_OUT_DIR:-${ROOT_DIR}/tests/bench/results}"
WARMUP="${FFQ_BENCH_WARMUP:-1}"
ITERATIONS="${FFQ_BENCH_ITERATIONS:-3}"
MODE="${FFQ_BENCH_MODE:-embedded}"
RAG_MATRIX="${FFQ_BENCH_RAG_MATRIX:-1000,16,10,1.0;5000,32,10,0.8;10000,64,10,0.2}"
THREADS="${FFQ_BENCH_THREADS:-1}"
BATCH_SIZE_ROWS="${FFQ_BENCH_BATCH_SIZE_ROWS:-8192}"
MEM_BUDGET_BYTES="${FFQ_BENCH_MEM_BUDGET_BYTES:-536870912}"
SHUFFLE_PARTITIONS="${FFQ_BENCH_SHUFFLE_PARTITIONS:-64}"
SPILL_DIR="${FFQ_BENCH_SPILL_DIR:-${ROOT_DIR}/target/tmp/bench_spill}"
MAX_CV_PCT="${FFQ_BENCH_MAX_CV_PCT:-30.0}"
KEEP_SPILL="${FFQ_BENCH_KEEP_SPILL:-0}"

export TZ=UTC
export LC_ALL=C
export FFQ_BENCH_THREADS="${THREADS}"
export FFQ_BENCH_BATCH_SIZE_ROWS="${BATCH_SIZE_ROWS}"
export FFQ_BENCH_MEM_BUDGET_BYTES="${MEM_BUDGET_BYTES}"
export FFQ_BENCH_SHUFFLE_PARTITIONS="${SHUFFLE_PARTITIONS}"
export FFQ_BENCH_SPILL_DIR="${SPILL_DIR}"
export FFQ_BENCH_KEEP_SPILL="${KEEP_SPILL}"
export FFQ_BENCH_MAX_CV_PCT="${MAX_CV_PCT}"
export TOKIO_WORKER_THREADS="${THREADS}"
export RAYON_NUM_THREADS="${THREADS}"

mkdir -p "${OUT_DIR}"

echo "Running benchmark baseline (13.3)"
echo "Mode:         ${MODE}"
echo "Fixture root: ${FIXTURE_ROOT}"
echo "Query root:   ${QUERY_ROOT}"
echo "Output dir:   ${OUT_DIR}"
echo "Warmup:       ${WARMUP}"
echo "Iterations:   ${ITERATIONS}"
echo "Threads:      ${THREADS}"
echo "Batch rows:   ${BATCH_SIZE_ROWS}"
echo "Mem budget:   ${MEM_BUDGET_BYTES}"
echo "Shuffle parts:${SHUFFLE_PARTITIONS}"
echo "Spill dir:    ${SPILL_DIR}"
echo "Max CV %:     ${MAX_CV_PCT}"
if [[ "${MODE}" == "embedded" ]]; then
  echo "RAG matrix:   ${RAG_MATRIX}"
fi

if [[ "${MODE}" == "distributed" ]]; then
  : "${FFQ_COORDINATOR_ENDPOINT:?FFQ_COORDINATOR_ENDPOINT must be set for distributed benchmark mode}"
  echo "Coordinator:  ${FFQ_COORDINATOR_ENDPOINT}"
  if [[ -n "${FFQ_WORKER1_ENDPOINT:-}" || -n "${FFQ_WORKER2_ENDPOINT:-}" ]]; then
    echo "Worker 1:     ${FFQ_WORKER1_ENDPOINT:-<unset>}"
    echo "Worker 2:     ${FFQ_WORKER2_ENDPOINT:-<unset>}"
  fi

  python3 - "${FFQ_COORDINATOR_ENDPOINT}" "${FFQ_WORKER1_ENDPOINT:-}" "${FFQ_WORKER2_ENDPOINT:-}" <<'PY'
import socket
import sys
import time
from urllib.parse import urlparse

def wait_for(endpoint: str, timeout_s: float = 60.0):
    if not endpoint:
        return
    u = urlparse(endpoint)
    host = u.hostname
    port = u.port
    if not host:
        raise SystemExit(f"Invalid endpoint: {endpoint}")
    if port is None:
        port = 443 if u.scheme == "https" else 80
    deadline = time.time() + timeout_s
    last_err = None
    while time.time() < deadline:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2.0)
        try:
            sock.connect((host, port))
            return
        except OSError as e:
            last_err = e
            time.sleep(1.0)
        finally:
            sock.close()
    raise SystemExit(
        f"Endpoint not reachable at {host}:{port} after {timeout_s:.0f}s: {last_err}."
    )

for ep in sys.argv[1:]:
    wait_for(ep)
PY

  cargo run -p ffq-client --example run_bench_13_3 --features distributed -- \
    --mode distributed \
    --fixture-root "${FIXTURE_ROOT}" \
    --query-root "${QUERY_ROOT}" \
    --out-dir "${OUT_DIR}" \
    --warmup "${WARMUP}" \
    --iterations "${ITERATIONS}" \
    --threads "${THREADS}" \
    --batch-size-rows "${BATCH_SIZE_ROWS}" \
    --mem-budget-bytes "${MEM_BUDGET_BYTES}" \
    --shuffle-partitions "${SHUFFLE_PARTITIONS}" \
    --spill-dir "${SPILL_DIR}" \
    --max-cv-pct "${MAX_CV_PCT}"
else
  export FFQ_BENCH_RAG_MATRIX="${RAG_MATRIX}"
  cargo run -p ffq-client --example run_bench_13_3 --features vector -- \
    --mode embedded \
    --fixture-root "${FIXTURE_ROOT}" \
    --query-root "${QUERY_ROOT}" \
    --out-dir "${OUT_DIR}" \
    --warmup "${WARMUP}" \
    --iterations "${ITERATIONS}" \
    --threads "${THREADS}" \
    --batch-size-rows "${BATCH_SIZE_ROWS}" \
    --mem-budget-bytes "${MEM_BUDGET_BYTES}" \
    --shuffle-partitions "${SHUFFLE_PARTITIONS}" \
    --spill-dir "${SPILL_DIR}" \
    --max-cv-pct "${MAX_CV_PCT}"
fi
