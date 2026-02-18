#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="${FFQ_REPL_SMOKE_TMP_DIR:-${ROOT_DIR}/target/tmp/repl_smoke}"
CATALOG_PATH="${TMP_DIR}/catalog.json"
OUT_PATH="${TMP_DIR}/repl.out"

LINEITEM_PATH="${ROOT_DIR}/tests/fixtures/parquet/lineitem.parquet"
ORDERS_PATH="${ROOT_DIR}/tests/fixtures/parquet/orders.parquet"

mkdir -p "${TMP_DIR}"

if [[ ! -f "${LINEITEM_PATH}" || ! -f "${ORDERS_PATH}" ]]; then
  echo "missing parquet fixtures under tests/fixtures/parquet; run integration fixture generation first" >&2
  exit 1
fi

cat > "${CATALOG_PATH}" <<EOF
{
  "tables": [
    {
      "name": "lineitem",
      "uri": "${LINEITEM_PATH}",
      "format": "parquet",
      "schema": {
        "fields": [
          {
            "name": "l_orderkey",
            "data_type": "Int64",
            "nullable": false,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
          },
          {
            "name": "l_partkey",
            "data_type": "Int64",
            "nullable": false,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
          }
        ],
        "metadata": {}
      }
    },
    {
      "name": "orders",
      "uri": "${ORDERS_PATH}",
      "format": "parquet",
      "schema": {
        "fields": [
          {
            "name": "o_orderkey",
            "data_type": "Int64",
            "nullable": false,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
          },
          {
            "name": "o_custkey",
            "data_type": "Int64",
            "nullable": false,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
          }
        ],
        "metadata": {}
      }
    }
  ]
}
EOF

echo "Running REPL smoke test with catalog ${CATALOG_PATH}"
cd "${ROOT_DIR}"
printf '\\help\n\\tables\n\\mode csv\nSELECT l_orderkey FROM lineitem LIMIT 2;\n\\q\n' \
  | cargo run --offline -p ffq-client -- repl --catalog "${CATALOG_PATH}" \
  > "${OUT_PATH}" 2>&1

grep -q "REPL commands:" "${OUT_PATH}"
grep -q "lineitem" "${OUT_PATH}"
grep -q "orders" "${OUT_PATH}"
grep -q "mode csv" "${OUT_PATH}"
grep -q "l_orderkey" "${OUT_PATH}"
grep -q "^1$" "${OUT_PATH}"
grep -q "^2$" "${OUT_PATH}"

echo "REPL smoke test passed"
