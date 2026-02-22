#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

PARQUET_PATH="${1:-${ROOT_DIR}/tests/fixtures/parquet/lineitem.parquet}"

if [[ ! -f "${PARQUET_PATH}" ]]; then
  echo "missing parquet fixture: ${PARQUET_PATH}" >&2
  exit 2
fi

echo "Building ffq-client cdylib with ffi feature..."
cargo build -p ffq-client --features ffi

LIB_DIR="${ROOT_DIR}/target/debug"
OUT_BIN="${ROOT_DIR}/target/ffi_example_c"
SRC="${ROOT_DIR}/examples/c/ffi_example.c"
INCLUDE="${ROOT_DIR}/include"

case "$(uname -s)" in
  Darwin)
    cc "${SRC}" -I"${INCLUDE}" -L"${LIB_DIR}" -lffq_client -Wl,-rpath,"${LIB_DIR}" -o "${OUT_BIN}"
    ;;
  Linux)
    cc "${SRC}" -I"${INCLUDE}" -L"${LIB_DIR}" -lffq_client -Wl,-rpath,"${LIB_DIR}" -o "${OUT_BIN}"
    ;;
  *)
    echo "unsupported platform for this helper script: $(uname -s)" >&2
    exit 2
    ;;
esac

echo "Running ffi C example..."
"${OUT_BIN}" "${PARQUET_PATH}"
