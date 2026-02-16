#!/usr/bin/env python3
"""Validate official TPC-H dbgen SF1 manifests for reproducibility contracts."""

from __future__ import annotations

import json
import math
import os
import pathlib
import sys
from typing import Any

ROOT = pathlib.Path(__file__).resolve().parent.parent
TBL_MANIFEST = ROOT / "tests/bench/fixtures/tpch_dbgen_sf1/manifest.json"
PARQUET_MANIFEST = ROOT / "tests/bench/fixtures/tpch_dbgen_sf1_parquet/manifest.json"

EXPECTED_SOURCE_REPO = "https://github.com/electrum/tpch-dbgen.git"
EXPECTED_SOURCE_REF = os.environ.get("TPCH_DBGEN_REF", "f20ca9f")

EXPECTED_TBL_ROWS = {
    "customer.tbl": 150_000,
    "lineitem.tbl": 6_001_215,
    "nation.tbl": 25,
    "orders.tbl": 1_500_000,
    "part.tbl": 200_000,
    "partsupp.tbl": 800_000,
    "region.tbl": 5,
    "supplier.tbl": 10_000,
}

EXPECTED_PARQUET = {
    "customer.parquet": {
        "rows": 150_000,
        "schema": [
            "c_custkey:Int64:false",
            "c_mktsegment:Utf8:false",
        ],
    },
    "orders.parquet": {
        "rows": 1_500_000,
        "schema": [
            "o_orderkey:Int64:false",
            "o_custkey:Int64:false",
            "o_orderdate:Utf8:false",
            "o_shippriority:Int64:false",
        ],
    },
    "lineitem.parquet": {
        "rows": 6_001_215,
        "schema": [
            "l_orderkey:Int64:false",
            "l_quantity:Float64:false",
            "l_extendedprice:Float64:false",
            "l_discount:Float64:false",
            "l_tax:Float64:false",
            "l_returnflag:Utf8:false",
            "l_linestatus:Utf8:false",
            "l_shipdate:Utf8:false",
        ],
    },
}


def fail(msg: str) -> None:
    raise SystemExit(f"manifest validation failed: {msg}")


def load_json(path: pathlib.Path) -> dict[str, Any]:
    if not path.exists():
        fail(f"missing manifest: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        fail(f"invalid json in {path}: {exc}")


def validate_tbl_manifest(payload: dict[str, Any]) -> None:
    if payload.get("fixture") != "tpch_dbgen_sf1":
        fail(f"unexpected tbl fixture: {payload.get('fixture')!r}")
    if payload.get("generator") != "dbgen":
        fail(f"unexpected tbl generator: {payload.get('generator')!r}")
    scale = payload.get("scale_factor")
    if not isinstance(scale, (int, float)) or not math.isclose(float(scale), 1.0):
        fail(f"unexpected tbl scale_factor: {scale!r}")
    if payload.get("source_repo") != EXPECTED_SOURCE_REPO:
        fail(
            "unexpected source_repo: "
            f"{payload.get('source_repo')!r} != {EXPECTED_SOURCE_REPO!r}"
        )
    if payload.get("source_ref") != EXPECTED_SOURCE_REF:
        fail(
            "unexpected source_ref: "
            f"{payload.get('source_ref')!r} != {EXPECTED_SOURCE_REF!r}"
        )

    files = payload.get("files")
    if not isinstance(files, list):
        fail("tbl manifest 'files' must be a list")
    got_rows = {}
    for item in files:
        if not isinstance(item, dict):
            fail("tbl manifest file entry must be object")
        name = item.get("file")
        rows = item.get("rows")
        sha = item.get("sha256")
        size = item.get("bytes")
        if not isinstance(name, str):
            fail(f"invalid tbl file name entry: {item!r}")
        if not isinstance(rows, int):
            fail(f"invalid row count for {name}: {rows!r}")
        if not isinstance(sha, str) or len(sha) != 64:
            fail(f"invalid sha256 for {name}: {sha!r}")
        if not isinstance(size, int) or size <= 0:
            fail(f"invalid byte size for {name}: {size!r}")
        got_rows[name] = rows

    if set(got_rows.keys()) != set(EXPECTED_TBL_ROWS.keys()):
        fail(
            "tbl file set mismatch: "
            f"actual={sorted(got_rows.keys())} expected={sorted(EXPECTED_TBL_ROWS.keys())}"
        )
    for name, expected_rows in EXPECTED_TBL_ROWS.items():
        if got_rows[name] != expected_rows:
            fail(
                f"tbl row-count mismatch for {name}: "
                f"actual={got_rows[name]} expected={expected_rows}"
            )


def validate_parquet_manifest(payload: dict[str, Any]) -> None:
    if payload.get("fixture") != "tpch_dbgen_sf1_parquet":
        fail(f"unexpected parquet fixture: {payload.get('fixture')!r}")
    if payload.get("source_format") != "tpch_dbgen_tbl":
        fail(f"unexpected source_format: {payload.get('source_format')!r}")
    files = payload.get("files")
    if not isinstance(files, list):
        fail("parquet manifest 'files' must be a list")

    got = {}
    for item in files:
        if not isinstance(item, dict):
            fail("parquet manifest file entry must be object")
        name = item.get("file")
        rows = item.get("rows")
        schema = item.get("schema")
        if not isinstance(name, str):
            fail(f"invalid parquet file name entry: {item!r}")
        if not isinstance(rows, int):
            fail(f"invalid parquet row count for {name}: {rows!r}")
        if not isinstance(schema, list) or not all(isinstance(x, str) for x in schema):
            fail(f"invalid parquet schema for {name}: {schema!r}")
        got[name] = {"rows": rows, "schema": schema}

    if set(got.keys()) != set(EXPECTED_PARQUET.keys()):
        fail(
            "parquet file set mismatch: "
            f"actual={sorted(got.keys())} expected={sorted(EXPECTED_PARQUET.keys())}"
        )
    for name, expected in EXPECTED_PARQUET.items():
        if got[name]["rows"] != expected["rows"]:
            fail(
                f"parquet row-count mismatch for {name}: "
                f"actual={got[name]['rows']} expected={expected['rows']}"
            )
        if got[name]["schema"] != expected["schema"]:
            fail(
                f"parquet schema mismatch for {name}: "
                f"actual={got[name]['schema']} expected={expected['schema']}"
            )


def main() -> int:
    tbl = load_json(TBL_MANIFEST)
    parquet = load_json(PARQUET_MANIFEST)
    validate_tbl_manifest(tbl)
    validate_parquet_manifest(parquet)
    print("TPC-H official manifest contract validation: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
