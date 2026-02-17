#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

TPCH_DBGEN_SRC_DIR="${TPCH_DBGEN_SRC_DIR:-${ROOT_DIR}/target/tpch-dbgen-src}"
TPCH_DBGEN_OUTPUT_DIR="${TPCH_DBGEN_OUTPUT_DIR:-${ROOT_DIR}/tests/bench/fixtures/tpch_dbgen_sf1}"
TPCH_SCALE="${TPCH_SCALE:-1}"

"${ROOT_DIR}/scripts/build-tpch-dbgen.sh"

DBGEN_BIN="${TPCH_DBGEN_SRC_DIR}/dbgen"
if [[ ! -x "${DBGEN_BIN}" ]]; then
  echo "dbgen binary missing at ${DBGEN_BIN}"
  exit 1
fi
DISTS_FILE="${TPCH_DBGEN_SRC_DIR}/dists.dss"
if [[ ! -f "${DISTS_FILE}" ]]; then
  echo "dists file missing at ${DISTS_FILE}"
  exit 1
fi

mkdir -p "${TPCH_DBGEN_OUTPUT_DIR}"
rm -f "${TPCH_DBGEN_OUTPUT_DIR}"/*.tbl "${TPCH_DBGEN_OUTPUT_DIR}/manifest.json"

echo "Generating TPC-H SF${TPCH_SCALE} .tbl files into ${TPCH_DBGEN_OUTPUT_DIR}"
(
  cd "${TPCH_DBGEN_OUTPUT_DIR}"
  "${DBGEN_BIN}" -b "${DISTS_FILE}" -s "${TPCH_SCALE}" -f
)

# Normalize permissions for CI runners where generated files may be non-readable
# to subsequent manifest/validation steps.
find "${TPCH_DBGEN_OUTPUT_DIR}" -maxdepth 1 -type f -name "*.tbl" -exec chmod u+rw {} +

required_tables=(
  customer
  lineitem
  nation
  orders
  part
  partsupp
  region
  supplier
)

for t in "${required_tables[@]}"; do
  if [[ ! -f "${TPCH_DBGEN_OUTPUT_DIR}/${t}.tbl" ]]; then
    echo "missing generated table: ${t}.tbl"
    exit 1
  fi
done

python3 - "${TPCH_DBGEN_OUTPUT_DIR}" "${TPCH_SCALE}" <<'PY'
import hashlib
import json
import os
import pathlib
import sys

out = pathlib.Path(sys.argv[1])
scale = sys.argv[2]
files = []
for p in sorted(out.glob("*.tbl")):
    # row count in dbgen .tbl files is line count
    with p.open("rb") as f:
        data = f.read()
    rows = data.count(b"\n")
    sha256 = hashlib.sha256(data).hexdigest()
    files.append({"file": p.name, "rows": rows, "sha256": sha256, "bytes": len(data)})

manifest = {
    "fixture": "tpch_dbgen_sf1" if scale == "1" else f"tpch_dbgen_sf{scale}",
    "generator": "dbgen",
    "scale_factor": float(scale),
    "source_repo": os.environ.get("TPCH_DBGEN_REPO", "https://github.com/electrum/tpch-dbgen.git"),
    "source_ref": os.environ.get("TPCH_DBGEN_REF", "32f1c1b92d1664dba542e927d23d86ffa57aa253"),
    "files": files,
}

(out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
PY

echo "Generation complete."
echo "Output: ${TPCH_DBGEN_OUTPUT_DIR}"
echo "Manifest: ${TPCH_DBGEN_OUTPUT_DIR}/manifest.json"
