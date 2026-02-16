#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

OUT_DIR="${1:-tests/bench/fixtures}"

echo "Generating deterministic benchmark fixtures into ${OUT_DIR}"
cargo run -p ffq-client --example generate_bench_fixtures -- "${OUT_DIR}"
