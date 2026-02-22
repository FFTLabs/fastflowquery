#!/usr/bin/env python3
"""Validate pipelined-shuffle TTFR benchmark thresholds."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Any


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Check pipelined-shuffle TTFR benchmark artifact against thresholds. "
            "Fails if TTFR improvement is too small or runtime/throughput regressions exceed bounds."
        )
    )
    parser.add_argument("--candidate", required=True, help="Candidate benchmark JSON artifact path")
    parser.add_argument(
        "--threshold-file",
        default="tests/bench/thresholds/pipelined_shuffle_ttfr_thresholds.json",
        help="Threshold JSON file path",
    )
    args = parser.parse_args()

    candidate = _load_json(Path(args.candidate))
    thresholds = _load_json(Path(args.threshold_file))

    min_ttfr_improvement_pct = float(thresholds.get("min_ttfr_improvement_pct", 10.0))
    max_total_runtime_regression_pct = float(thresholds.get("max_total_runtime_regression_pct", 10.0))
    max_throughput_regression_pct = float(thresholds.get("max_throughput_regression_pct", 10.0))

    ttfr_improvement_pct = float(candidate.get("ttfr_improvement_pct", 0.0))
    total_runtime_regression_pct = float(candidate.get("total_runtime_regression_pct", 0.0))
    throughput_regression_pct = float(candidate.get("throughput_regression_pct", 0.0))

    failures = []
    if ttfr_improvement_pct < min_ttfr_improvement_pct:
        failures.append(
            f"TTFR improvement too small: {ttfr_improvement_pct:.2f}% < {min_ttfr_improvement_pct:.2f}%"
        )
    if total_runtime_regression_pct > max_total_runtime_regression_pct:
        failures.append(
            "Total runtime regression too high: "
            f"{total_runtime_regression_pct:.2f}% > {max_total_runtime_regression_pct:.2f}%"
        )
    if throughput_regression_pct > max_throughput_regression_pct:
        failures.append(
            "Throughput regression too high: "
            f"{throughput_regression_pct:.2f}% > {max_throughput_regression_pct:.2f}%"
        )

    print("Pipelined-shuffle TTFR gate")
    print(f"candidate: {args.candidate}")
    print(
        "metrics: "
        f"ttfr_improvement_pct={ttfr_improvement_pct:.2f}, "
        f"total_runtime_regression_pct={total_runtime_regression_pct:.2f}, "
        f"throughput_regression_pct={throughput_regression_pct:.2f}"
    )
    print(
        "thresholds: "
        f"min_ttfr_improvement_pct={min_ttfr_improvement_pct:.2f}, "
        f"max_total_runtime_regression_pct={max_total_runtime_regression_pct:.2f}, "
        f"max_throughput_regression_pct={max_throughput_regression_pct:.2f}"
    )

    if failures:
        for f in failures:
            print(f"[FAIL] {f}")
        return 1

    print("[OK] all thresholds satisfied")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
