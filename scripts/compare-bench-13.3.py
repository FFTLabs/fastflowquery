#!/usr/bin/env python3
"""Compare benchmark artifacts and gate on regressions.

Contract:
- Exit 0 on pass.
- Exit non-zero on regression threshold breach or invalid candidate rows.
- Print offending tuple/metric details.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class RowKey:
    mode: str
    query_id: str
    variant: str
    backend: str
    n_docs: Optional[int]
    effective_dim: Optional[int]
    top_k: Optional[int]
    filter_selectivity: Optional[float]

    @staticmethod
    def from_row(mode: str, row: dict) -> "RowKey":
        return RowKey(
            mode=mode,
            query_id=str(row.get("query_id", "")),
            variant=str(row.get("variant", "")),
            backend=str(row.get("backend", "sql_baseline")),
            n_docs=_opt_int(row.get("n_docs")),
            effective_dim=_opt_int(row.get("effective_dim")),
            top_k=_opt_int(row.get("top_k")),
            filter_selectivity=_opt_float(row.get("filter_selectivity")),
        )

    def render(self) -> str:
        return (
            f"mode={self.mode} query_id={self.query_id} variant={self.variant} "
            f"backend={self.backend} n={self.n_docs} dim={self.effective_dim} "
            f"k={self.top_k} sel={self.filter_selectivity}"
        )


def _opt_int(v: object) -> Optional[int]:
    if v is None or v == "":
        return None
    return int(v)


def _opt_float(v: object) -> Optional[float]:
    if v is None or v == "":
        return None
    return float(v)


def _load_artifact(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _resolve_json(path_like: str) -> Path:
    p = Path(path_like)
    if p.is_file():
        return p
    if p.is_dir():
        candidates = sorted(p.glob("*.json"), key=lambda x: x.stat().st_mtime)
        if not candidates:
            raise SystemExit(f"No *.json files found under directory: {p}")
        return candidates[-1]
    raise SystemExit(f"Path does not exist: {p}")


def _rows_by_key(artifact: dict) -> Dict[RowKey, dict]:
    mode = str(artifact.get("mode", "unknown"))
    rows = artifact.get("results", [])
    if not isinstance(rows, list):
        raise SystemExit("Invalid artifact: 'results' must be a list")
    out: Dict[RowKey, dict] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = RowKey.from_row(mode, row)
        out[key] = row
    return out


def _pct_increase(base: float, cand: float) -> float:
    if base <= 0.0:
        return 0.0 if cand <= 0.0 else float("inf")
    return (cand - base) / base


def compare(
    baseline: dict,
    candidate: dict,
    threshold: float,
    fail_on_missing_candidate: bool,
) -> Tuple[List[str], List[str]]:
    """Returns (failures, warnings)."""
    failures: List[str] = []
    warnings: List[str] = []

    base_rows = _rows_by_key(baseline)
    cand_rows = _rows_by_key(candidate)

    for key, cand in cand_rows.items():
        if not bool(cand.get("success", False)):
            failures.append(
                f"[candidate_failed] {key.render()} error={cand.get('error')!r}"
            )

    for key, base in base_rows.items():
        cand = cand_rows.get(key)
        if cand is None:
            msg = f"[missing_in_candidate] {key.render()}"
            if fail_on_missing_candidate:
                failures.append(msg)
            else:
                warnings.append(msg)
            continue

        base_rows_out = int(base.get("rows_out", 0))
        cand_rows_out = int(cand.get("rows_out", 0))
        if base_rows_out != cand_rows_out:
            failures.append(
                f"[rows_out_mismatch] {key.render()} baseline={base_rows_out} "
                f"candidate={cand_rows_out}"
            )

        base_elapsed = float(base.get("elapsed_ms", 0.0))
        cand_elapsed = float(cand.get("elapsed_ms", 0.0))
        increase = _pct_increase(base_elapsed, cand_elapsed)
        if increase > threshold:
            failures.append(
                f"[elapsed_regression] {key.render()} baseline_ms={base_elapsed:.3f} "
                f"candidate_ms={cand_elapsed:.3f} increase_pct={increase*100:.2f} "
                f"threshold_pct={threshold*100:.2f}"
            )

    for key in cand_rows:
        if key not in base_rows:
            warnings.append(f"[missing_in_baseline] {key.render()}")

    return failures, warnings


def _print_lines(title: str, lines: Iterable[str]) -> None:
    lines = list(lines)
    if not lines:
        return
    print(title)
    for line in lines:
        print(f"  - {line}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare benchmark artifacts and fail on regressions."
    )
    parser.add_argument("--baseline", required=True, help="Baseline JSON path or directory")
    parser.add_argument("--candidate", required=True, help="Candidate JSON path or directory")
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.10,
        help="Allowed elapsed regression ratio (default: 0.10 = 10%%)",
    )
    parser.add_argument(
        "--warn-on-missing-candidate",
        action="store_true",
        help="Warn (instead of fail) when a baseline tuple is missing in candidate",
    )
    args = parser.parse_args()

    if args.threshold < 0:
        raise SystemExit("--threshold must be >= 0")

    baseline_path = _resolve_json(args.baseline)
    candidate_path = _resolve_json(args.candidate)
    baseline = _load_artifact(baseline_path)
    candidate = _load_artifact(candidate_path)

    failures, warnings = compare(
        baseline=baseline,
        candidate=candidate,
        threshold=args.threshold,
        fail_on_missing_candidate=not args.warn_on_missing_candidate,
    )

    print(f"Baseline:  {baseline_path}")
    print(f"Candidate: {candidate_path}")
    print(f"Threshold: {args.threshold*100:.2f}%")
    _print_lines("Warnings:", warnings)
    _print_lines("Failures:", failures)

    if failures:
        print(f"\nResult: FAIL ({len(failures)} issue(s))")
        return 2
    print("\nResult: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
