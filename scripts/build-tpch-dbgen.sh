#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

# Electrum's tpch-dbgen mirror is commonly used for CI/dev automation.
TPCH_DBGEN_REPO="${TPCH_DBGEN_REPO:-https://github.com/electrum/tpch-dbgen.git}"
# Pinned ref for reproducibility. Override if needed.
TPCH_DBGEN_REF="${TPCH_DBGEN_REF:-f20ca9f}" # short commit hash
TPCH_DBGEN_SRC_DIR="${TPCH_DBGEN_SRC_DIR:-${ROOT_DIR}/target/tpch-dbgen-src}"

if [[ ! -d "${TPCH_DBGEN_SRC_DIR}/.git" ]]; then
  echo "Cloning tpch-dbgen into ${TPCH_DBGEN_SRC_DIR}"
  git clone "${TPCH_DBGEN_REPO}" "${TPCH_DBGEN_SRC_DIR}"
fi

echo "Preparing tpch-dbgen source at ${TPCH_DBGEN_SRC_DIR}"
git -C "${TPCH_DBGEN_SRC_DIR}" fetch --all --tags --prune
git -C "${TPCH_DBGEN_SRC_DIR}" checkout "${TPCH_DBGEN_REF}"

MACHINE="${TPCH_DBGEN_MACHINE:-}"
if [[ -z "${MACHINE}" ]]; then
  case "$(uname -s)" in
    Darwin) MACHINE="MACOS" ;;
    Linux) MACHINE="LINUX" ;;
    *)
      echo "Unsupported OS for automatic MACHINE mapping: $(uname -s)"
      echo "Set TPCH_DBGEN_MACHINE manually (for example LINUX or MACOS)."
      exit 1
      ;;
  esac
fi

echo "Building dbgen with MACHINE=${MACHINE}"
make -C "${TPCH_DBGEN_SRC_DIR}" clean >/dev/null 2>&1 || true
make -C "${TPCH_DBGEN_SRC_DIR}" MACHINE="${MACHINE}" dbgen

if [[ ! -x "${TPCH_DBGEN_SRC_DIR}/dbgen" ]]; then
  echo "dbgen binary not found after build"
  exit 1
fi

echo "dbgen ready: ${TPCH_DBGEN_SRC_DIR}/dbgen"
echo "repo=${TPCH_DBGEN_REPO}"
echo "ref=${TPCH_DBGEN_REF}"
