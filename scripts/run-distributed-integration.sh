#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export FFQ_COORDINATOR_ENDPOINT="${FFQ_COORDINATOR_ENDPOINT:-http://127.0.0.1:50051}"
export FFQ_WORKER1_ENDPOINT="${FFQ_WORKER1_ENDPOINT:-http://127.0.0.1:50061}"
export FFQ_WORKER2_ENDPOINT="${FFQ_WORKER2_ENDPOINT:-http://127.0.0.1:50062}"
export FFQ_INTEGRATION_TMP_DIR="${FFQ_INTEGRATION_TMP_DIR:-${ROOT_DIR}/target/tmp/integration_distributed}"
export RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}"
export TZ=UTC
export LC_ALL=C

rm -rf "${FFQ_INTEGRATION_TMP_DIR}"
mkdir -p "${FFQ_INTEGRATION_TMP_DIR}"

cleanup() {
  if [[ "${FFQ_KEEP_INTEGRATION_TMP:-0}" != "1" ]]; then
    rm -rf "${FFQ_INTEGRATION_TMP_DIR}"
  fi
}
trap cleanup EXIT

echo "Running distributed integration suite against ${FFQ_COORDINATOR_ENDPOINT}"
echo "Health targets: ${FFQ_COORDINATOR_ENDPOINT}, ${FFQ_WORKER1_ENDPOINT}, ${FFQ_WORKER2_ENDPOINT}"

python3 - "${FFQ_COORDINATOR_ENDPOINT}" "${FFQ_WORKER1_ENDPOINT}" "${FFQ_WORKER2_ENDPOINT}" <<'PY'
import socket
import sys
import time
from urllib.parse import urlparse

def wait_for(endpoint: str, timeout_s: float = 60.0):
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
        f"Endpoint not reachable at {host}:{port} after {timeout_s:.0f}s: {last_err}. "
        "Ensure docker compose services are healthy."
    )

for ep in sys.argv[1:]:
    wait_for(ep)
PY

cargo test -p ffq-client --test integration_distributed --features distributed -- --ignored --nocapture
