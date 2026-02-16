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

export TZ=UTC
export LC_ALL=C

mkdir -p "${OUT_DIR}"

echo "Running benchmark baseline (13.3)"
echo "Mode:         ${MODE}"
echo "Fixture root: ${FIXTURE_ROOT}"
echo "Query root:   ${QUERY_ROOT}"
echo "Output dir:   ${OUT_DIR}"
echo "Warmup:       ${WARMUP}"
echo "Iterations:   ${ITERATIONS}"
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
    --iterations "${ITERATIONS}"
else
  export FFQ_BENCH_RAG_MATRIX="${RAG_MATRIX}"
  cargo run -p ffq-client --example run_bench_13_3 --features vector -- \
    --mode embedded \
    --fixture-root "${FIXTURE_ROOT}" \
    --query-root "${QUERY_ROOT}" \
    --out-dir "${OUT_DIR}" \
    --warmup "${WARMUP}" \
    --iterations "${ITERATIONS}"
fi
