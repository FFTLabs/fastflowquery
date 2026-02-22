#!/usr/bin/env python3
"""Validate v2 docs guardrails.

Checks:
1. Required `docs/v2/*.md` pages listed in `docs/v2/README.md` exist.
2. Markdown links in v2 docs (and root entry docs) resolve.
3. Every heading in `tickets/eng/Plan_v2.md` is mapped in
   `docs/v2/status-matrix.md` table's "Plan heading" column.
4. For every `done`/`partial` status-matrix row, at least one docs markdown file
   is referenced and all referenced repository paths exist.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DOCS_V2_README = ROOT / "docs/v2/README.md"
DOCS_V2_STATUS = ROOT / "docs/v2/status-matrix.md"
PLAN_V2 = ROOT / "tickets/eng/Plan_v2.md"


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def strip_fenced_code(text: str) -> str:
    out: list[str] = []
    in_fence = False
    for line in text.splitlines():
        if line.strip().startswith("```"):
            in_fence = not in_fence
            continue
        if not in_fence:
            out.append(line)
    return "\n".join(out)


def canonical(s: str) -> str:
    s = s.replace("—", "-").replace("–", "-")
    s = s.replace("`", "").replace("*", "")
    s = re.sub(r"\s+", " ", s.strip())
    return s.lower()


def gh_slug(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s


def markdown_headings(path: Path) -> set[str]:
    text = strip_fenced_code(read_text(path))
    out: set[str] = set()
    for line in text.splitlines():
        m = re.match(r"^\s{0,3}#{1,6}\s+(.+?)\s*$", line)
        if not m:
            continue
        heading = m.group(1).strip()
        # Remove trailing heading hashes ("## title ##")
        heading = re.sub(r"\s+#+\s*$", "", heading).strip()
        out.add(gh_slug(heading))
    return out


def required_v2_pages() -> set[Path]:
    text = read_text(DOCS_V2_README)
    # Pull from the required page matrix rows.
    rels = set(re.findall(r"`(docs/v2/[^`]+\.md)`", text))
    return {ROOT / rel for rel in rels}


def check_required_pages(errors: list[str]) -> None:
    pages = required_v2_pages()
    if not pages:
        errors.append("no required docs/v2 pages found in docs/v2/README.md")
        return
    for page in sorted(pages):
        if not page.exists():
            errors.append(f"missing required v2 page: {page.relative_to(ROOT)}")


def markdown_link_targets(text: str) -> list[str]:
    text = strip_fenced_code(text)
    # Match inline markdown links/images: [x](target), ![x](target)
    links = re.findall(r"!?[^\]]*\]\(([^)]+)\)", text)
    out: list[str] = []
    for raw in links:
        target = raw.strip()
        if not target:
            continue
        # Strip optional title: path "title"
        if " " in target and not target.startswith("<"):
            target = target.split(" ", 1)[0].strip()
        target = target.strip("<>")
        out.append(target)
    return out


def is_external(target: str) -> bool:
    return target.startswith(("http://", "https://", "mailto:", "data:"))


def docs_link_files() -> list[Path]:
    files = sorted((ROOT / "docs/v2").glob("*.md"))
    files.extend([ROOT / "Readme.md", ROOT / "Contributing.md"])
    return files


def check_links(errors: list[str]) -> None:
    heading_cache: dict[Path, set[str]] = {}
    for md in docs_link_files():
        text = read_text(md)
        for target in markdown_link_targets(text):
            if is_external(target):
                continue
            if target.startswith("#"):
                slug = target[1:]
                slugs = heading_cache.setdefault(md, markdown_headings(md))
                if slug and slug not in slugs:
                    errors.append(
                        f"{md.relative_to(ROOT)}: broken anchor link '{target}'"
                    )
                continue

            path_part, anchor = (target.split("#", 1) + [""])[:2]
            resolved = (md.parent / path_part).resolve()
            if not resolved.exists():
                errors.append(
                    f"{md.relative_to(ROOT)}: broken link '{target}' -> "
                    f"{resolved.relative_to(ROOT) if resolved.is_relative_to(ROOT) else resolved}"
                )
                continue
            if anchor and resolved.suffix.lower() == ".md":
                slugs = heading_cache.setdefault(resolved, markdown_headings(resolved))
                if anchor not in slugs:
                    errors.append(
                        f"{md.relative_to(ROOT)}: broken anchor '{anchor}' in '{target}'"
                    )


def plan_headings() -> set[str]:
    text = read_text(PLAN_V2)
    out: set[str] = set()
    for line in text.splitlines():
        m = re.match(r"^\s{0,3}#{1,6}\s+(.+?)\s*$", line)
        if not m:
            continue
        heading = m.group(1).strip()
        heading = re.sub(r"\s+#+\s*$", "", heading).strip()
        # Skip extremely generic title line if present
        if canonical(heading) in {"plan v2", "v2 plan"}:
            continue
        out.add(canonical(heading))
    return out


def mapped_plan_headings() -> set[str]:
    return {canonical(row["heading"]) for row in status_matrix_rows()}


def status_matrix_rows() -> list[dict[str, str]]:
    text = read_text(DOCS_V2_STATUS)
    rows: list[dict[str, str]] = []
    for line in text.splitlines():
        if not line.startswith("|"):
            continue
        cols = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cols) < 5:
            continue
        heading = cols[0]
        if heading.lower() in {"plan heading", "---"} or not heading:
            continue
        rows.append(
            {
                "heading": heading,
                "status": cols[1].strip().lower(),
                "evidence_docs_code": cols[2],
                "evidence_tests": cols[3],
                "gap_note": cols[4],
            }
        )
    return rows


def extract_repo_paths(text: str) -> set[str]:
    out: set[str] = set()
    for m in re.finditer(r"`([^`]+)`", text):
        candidate = m.group(1).strip()
        if "/" in candidate:
            out.add(candidate)
    for m in re.finditer(r"(?<![A-Za-z0-9_.-])([.]?/?(?:docs|crates|scripts|tickets|include|examples|\.github)/[A-Za-z0-9_./-]+)", text):
        out.add(m.group(1).strip())
    return out


def check_status_matrix_traceability(errors: list[str]) -> None:
    allowed_statuses = {"done", "partial", "not started"}
    for row in status_matrix_rows():
        heading = row["heading"]
        status = row["status"]
        if status not in allowed_statuses:
            errors.append(
                f"docs/v2/status-matrix.md: invalid status '{status}' for heading '{heading}'"
            )
            continue

        refs = extract_repo_paths(row["evidence_docs_code"]) | extract_repo_paths(
            row["evidence_tests"]
        )
        for ref in sorted(refs):
            path = ROOT / ref
            if not path.exists():
                errors.append(
                    f"docs/v2/status-matrix.md: heading '{heading}' references missing path '{ref}'"
                )

        if status in {"done", "partial"}:
            docs_refs = [
                ref
                for ref in refs
                if ref.startswith("docs/") and ref.endswith(".md")
            ]
            if not docs_refs:
                errors.append(
                    "docs/v2/status-matrix.md: "
                    f"heading '{heading}' is '{status}' but has no docs reference in evidence columns"
                )


def check_plan_coverage(errors: list[str]) -> None:
    plan = plan_headings()
    mapped = mapped_plan_headings()
    missing = sorted(h for h in plan if h not in mapped)
    if missing:
        errors.append("unmapped Plan_v2 headings in docs/v2/status-matrix.md:")
        for h in missing:
            errors.append(f"  - {h}")


def main() -> int:
    errors: list[str] = []
    check_required_pages(errors)
    check_links(errors)
    check_plan_coverage(errors)
    check_status_matrix_traceability(errors)

    if errors:
        print("docs-v2 guardrails: FAILED")
        for e in errors:
            print(f"- {e}")
        return 1

    print("docs-v2 guardrails: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
