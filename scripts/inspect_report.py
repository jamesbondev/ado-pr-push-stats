#!/usr/bin/env python3
"""
Ask a report JSON the questions the console summary cannot answer.

The console prints estate-wide totals, which tell you that a file type is large but not whether
one repository is responsible for it. That distinction decides the fix: a type spread across the
estate belongs in the default exclusion list, whereas a type confined to one repository belongs in
that repository's own configuration.

    python3 scripts/inspect_report.py 2-no-csv.json
    python3 scripts/inspect_report.py 2-no-csv.json --ext .csv --ext .json
    python3 scripts/inspect_report.py 2-no-csv.json --repo cbi/ReleaseData
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

REVIEWED_KEY = "top_extensions_by_changed_lines"
SKIPPED_KEY = "top_excluded_extensions_by_changed_lines"
SHARE_KEY = "excluded_share_of_changed_lines_percent"


def load(path: str) -> dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        sys.exit(f"No such report: {path}")
    except json.JSONDecodeError as exc:
        sys.exit(f"{path} is not valid JSON: {exc}")


def require_per_repository(report: dict[str, Any], path: str) -> dict[str, Any]:
    per_repo = report.get("per_repository")
    if not per_repo:
        sys.exit(f"{path} has no per_repository block. Re-run the collector to produce one.")
    return per_repo


def concentration(per_repo: dict[str, Any], ext: str, top: int) -> None:
    """Which repositories hold a file type, on both sides of the exclusion rules."""
    rows = []
    for name, entry in per_repo.items():
        skipped = (entry.get(SKIPPED_KEY) or {}).get(ext, 0)
        reviewed = (entry.get(REVIEWED_KEY) or {}).get(ext, 0)
        if skipped or reviewed:
            rows.append((skipped + reviewed, skipped, reviewed, name, entry.get("pull_requests", 0)))

    if not rows:
        print(f"{ext}: absent from every repository in this report.")
        print()
        return

    rows.sort(reverse=True)
    total = sum(r[0] for r in rows)
    print(f"{ext}: {total:,} changed lines across {len(rows)} repositories")

    # A type living in one or two repositories is that repository's problem, not a default.
    leader = rows[0]
    print(f"  top repository holds {100.0 * leader[0] / total:.1f}% of it")
    print(f"  {'repository':<48}{'total':>12}{'skipped':>12}{'reviewed':>12}   PRs")
    for combined, skipped, reviewed, name, prs in rows[:top]:
        print(f"  {name:<48}{combined:>12,}{skipped:>12,}{reviewed:>12,}   {prs:,}")
    if len(rows) > top:
        print(f"  ... and {len(rows) - top:,} more")
    print()


def most_skipped(per_repo: dict[str, Any], top: int) -> None:
    """Repositories where the reviewer looks at the least, which is where config is the payload."""
    rows = [
        (entry[SHARE_KEY], name, entry.get("pull_requests", 0))
        for name, entry in per_repo.items()
        if entry.get(SHARE_KEY) is not None and entry.get("pull_requests", 0) >= 10
    ]
    if not rows:
        print("No repository carries a skipped share. Re-run the collector to produce one.")
        print()
        return

    rows.sort(reverse=True)
    print(f"Most-skipped repositories (10 or more pull requests)")
    print(f"  {'repository':<48}{'skipped':>10}   PRs")
    for share, name, prs in rows[:top]:
        print(f"  {name:<48}{share:>9.1f}%   {prs:,}")
    print()


def detail(per_repo: dict[str, Any], name: str) -> None:
    entry = per_repo.get(name)
    if entry is None:
        matches = [k for k in per_repo if name.lower() in k.lower()]
        hint = f" Did you mean: {', '.join(matches[:5])}?" if matches else ""
        print(f"{name}: not in this report.{hint}")
        print()
        return

    share = entry.get(SHARE_KEY)
    print(f"{name}  ({entry.get('pull_requests', 0):,} pull requests)")
    if share is not None:
        print(f"  skipped share of changed lines: {share}%")
    for label, key in (("reviewed", REVIEWED_KEY), ("skipped", SKIPPED_KEY)):
        breakdown = entry.get(key) or {}
        print(f"  {label}:")
        if not breakdown:
            print("    (none)")
            continue
        for ext, lines in list(breakdown.items())[:12]:
            print(f"    {ext:<14}{lines:>12,}")
    print()


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("report", help="A JSON report written by --json.")
    parser.add_argument("--ext", action="append", default=[],
                        help="Trace a file type across repositories. Repeatable. Defaults to .csv.")
    parser.add_argument("--repo", action="append", default=[],
                        help="Full breakdown for one repository. Repeatable.")
    parser.add_argument("--top", type=int, default=10)
    args = parser.parse_args(argv)

    report = load(args.report)
    per_repo = require_per_repository(report, args.report)

    scenario = (report.get("config") or {}).get("extra_excluded_extensions")
    print(f"{args.report}: {report.get('pull_requests', {}).get('total', 0):,} pull requests"
          + (f", scenario excludes {', '.join(scenario)}" if scenario else ""))
    print()

    for ext in (args.ext or [".csv"]):
        concentration(per_repo, ext if ext.startswith(".") else f".{ext}", args.top)

    most_skipped(per_repo, args.top)

    for name in args.repo:
        detail(per_repo, name)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
