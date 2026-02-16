#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

TPCH_DBGEN_TBL_DIR="${TPCH_DBGEN_TBL_DIR:-${ROOT_DIR}/tests/bench/fixtures/tpch_dbgen_sf1}"
TPCH_DBGEN_PARQUET_DIR="${TPCH_DBGEN_PARQUET_DIR:-${ROOT_DIR}/tests/bench/fixtures/tpch_dbgen_sf1_parquet}"

echo "Converting TPC-H dbgen .tbl -> parquet"
echo "Input:  ${TPCH_DBGEN_TBL_DIR}"
echo "Output: ${TPCH_DBGEN_PARQUET_DIR}"

cargo run -p ffq-client --example convert_tpch_tbl_to_parquet -- \
  --input-dir "${TPCH_DBGEN_TBL_DIR}" \
  --output-dir "${TPCH_DBGEN_PARQUET_DIR}"
