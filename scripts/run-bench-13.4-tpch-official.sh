#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

MODE="${FFQ_BENCH_MODE:-embedded}"
FIXTURE_ROOT="${FFQ_BENCH_FIXTURE_ROOT:-${ROOT_DIR}/tests/bench/fixtures}"
TPCH_SUBDIR="${FFQ_BENCH_TPCH_SUBDIR:-tpch_dbgen_sf1_parquet}"
QUERY_ROOT="${FFQ_BENCH_QUERY_ROOT:-${ROOT_DIR}/tests/bench/queries}"
OUT_DIR="${FFQ_BENCH_OUT_DIR:-${ROOT_DIR}/tests/bench/results/official_tpch}"
WARMUP="${FFQ_BENCH_WARMUP:-1}"
ITERATIONS="${FFQ_BENCH_ITERATIONS:-3}"
THREADS="${FFQ_BENCH_THREADS:-1}"
BATCH_SIZE_ROWS="${FFQ_BENCH_BATCH_SIZE_ROWS:-8192}"
MEM_BUDGET_BYTES="${FFQ_BENCH_MEM_BUDGET_BYTES:-536870912}"
SHUFFLE_PARTITIONS="${FFQ_BENCH_SHUFFLE_PARTITIONS:-64}"
SPILL_DIR="${FFQ_BENCH_SPILL_DIR:-${ROOT_DIR}/target/tmp/bench_spill_official_tpch}"
MAX_CV_PCT="${FFQ_BENCH_MAX_CV_PCT:-30.0}"
KEEP_SPILL="${FFQ_BENCH_KEEP_SPILL:-0}"

for required in \
  "${FIXTURE_ROOT}/${TPCH_SUBDIR}/customer.parquet" \
  "${FIXTURE_ROOT}/${TPCH_SUBDIR}/orders.parquet" \
  "${FIXTURE_ROOT}/${TPCH_SUBDIR}/lineitem.parquet"
do
  if [[ ! -f "${required}" ]]; then
    echo "Missing official TPC-H parquet fixture: ${required}" >&2
    echo "Run: make tpch-dbgen-parquet" >&2
    exit 1
  fi
done

mkdir -p "${OUT_DIR}"
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

echo "Running official TPC-H benchmark (13.4.5)"
echo "Mode:         ${MODE}"
echo "Fixture root: ${FIXTURE_ROOT}"
echo "TPC-H dir:    ${TPCH_SUBDIR}"
echo "Query root:   ${QUERY_ROOT}"
echo "Output dir:   ${OUT_DIR}"
echo "Warmup:       ${WARMUP}"
echo "Iterations:   ${ITERATIONS}"
echo "Threads:      ${THREADS}"

if [[ "${MODE}" == "distributed" ]]; then
  : "${FFQ_COORDINATOR_ENDPOINT:?FFQ_COORDINATOR_ENDPOINT must be set for distributed benchmark mode}"
  echo "Coordinator:  ${FFQ_COORDINATOR_ENDPOINT}"
  cargo run -p ffq-client --example run_bench_13_3 --features distributed -- \
    --mode distributed \
    --fixture-root "${FIXTURE_ROOT}" \
    --tpch-subdir "${TPCH_SUBDIR}" \
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
  cargo run -p ffq-client --example run_bench_13_3 -- \
    --mode embedded \
    --fixture-root "${FIXTURE_ROOT}" \
    --tpch-subdir "${TPCH_SUBDIR}" \
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
