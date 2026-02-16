#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export FFQ_COORDINATOR_ENDPOINT="${FFQ_COORDINATOR_ENDPOINT:-http://127.0.0.1:50051}"
echo "Running distributed integration suite against ${FFQ_COORDINATOR_ENDPOINT}"

python3 - "${FFQ_COORDINATOR_ENDPOINT}" <<'PY'
import socket
import sys
from urllib.parse import urlparse

endpoint = sys.argv[1]
u = urlparse(endpoint)
host = u.hostname
port = u.port or 50051
if not host:
    raise SystemExit(f"Invalid FFQ_COORDINATOR_ENDPOINT: {endpoint}")
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(2.0)
try:
    sock.connect((host, port))
except OSError as e:
    raise SystemExit(
        f"Coordinator endpoint not reachable at {host}:{port}: {e}. "
        "Start docker compose first."
    )
finally:
    sock.close()
PY

cargo test -p ffq-client --test integration_distributed --features distributed -- --ignored --nocapture
