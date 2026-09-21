#!/usr/bin/env python3
"""Size distribution of the source files an Argus review could attach as related context.

Answers one question: at a given per-file ceiling, how often would a related file be left out
entirely rather than attached? Since PBI 267 that ceiling drops a file rather than truncating it,
so a repository of large files loses context silently.

Reads the bare clones pr_diff_stats.py already made, so it needs a populated --cache and no network.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from collections import defaultdict

from pr_diff_stats import argus_exclusion_reason

DEFAULT_CEILING = 8000


def percentile(values: list[int], fraction: float) -> int:
    if not values:
        return 0
    index = min(len(values) - 1, int(round(fraction * (len(values) - 1))))
    return values[index]


def sizes_in(clone: str) -> list[tuple[int, str]]:
    head = subprocess.run(
        ["git", "--git-dir", clone, "rev-parse", "--verify", "HEAD"],
        capture_output=True, text=True)
    if head.returncode != 0:
        return []

    listing = subprocess.run(
        ["git", "--git-dir", clone, "ls-tree", "-r", "-l", "HEAD"],
        capture_output=True, text=True, errors="replace")
    if listing.returncode != 0:
        return []

    out: list[tuple[int, str]] = []
    for line in listing.stdout.splitlines():
        meta, _, path = line.partition("\t")
        parts = meta.split()
        if len(parts) < 4 or parts[1] != "blob" or parts[3] == "-":
            continue
        if argus_exclusion_reason(path) is not None:
            continue
        out.append((int(parts[3]), path))
    return out


def report(rows: list[tuple[int, str]], ceiling: int, label: str) -> None:
    sizes = sorted(size for size, _ in rows)
    if not sizes:
        print(f"{label}: no reviewable source files")
        return

    over = [s for s in sizes if s > ceiling]
    print(f"{label}: {len(sizes)} reviewable files")
    print(f"  p50 {percentile(sizes, .50):>7}  p75 {percentile(sizes, .75):>7}  "
          f"p90 {percentile(sizes, .90):>7}  p95 {percentile(sizes, .95):>7}  "
          f"p99 {percentile(sizes, .99):>7}  max {sizes[-1]:>8}")
    print(f"  over the {ceiling} ceiling: {len(over)} ({100.0 * len(over) / len(sizes):.1f}%)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache", default="cache", help="The pr_diff_stats cache holding clones/.")
    parser.add_argument("--ceiling", type=int, default=DEFAULT_CEILING,
                        help="MaxRelatedFileCharacters. A file over this is left out entirely.")
    parser.add_argument("--per-repo", action="store_true",
                        help="Also break the distribution down by repository.")
    parser.add_argument("--top-extensions", type=int, default=8)
    args = parser.parse_args()

    clones_root = os.path.join(args.cache, "clones")
    if not os.path.isdir(clones_root):
        print(f"No clones under {clones_root}. Run pr_diff_stats.py first so the cache is populated.",
              file=sys.stderr)
        return 2

    clones = sorted(
        os.path.join(clones_root, name)
        for name in os.listdir(clones_root)
        if name.endswith(".git"))
    if not clones:
        print(f"No bare clones in {clones_root}.", file=sys.stderr)
        return 2

    everything: list[tuple[int, str]] = []
    by_extension: dict[str, list[int]] = defaultdict(list)

    for clone in clones:
        rows = sizes_in(clone)
        everything.extend(rows)
        for size, path in rows:
            name = path.rsplit("/", 1)[-1]
            extension = "." + name.rsplit(".", 1)[-1].lower() if "." in name else "(none)"
            by_extension[extension].append(size)
        if args.per_repo:
            report(rows, args.ceiling, os.path.basename(clone)[:-4])

    if args.per_repo:
        print()

    report(everything, args.ceiling, f"all {len(clones)} repositories")

    print()
    print("by extension, largest populations first:")
    ranked = sorted(by_extension.items(), key=lambda kv: -len(kv[1]))[:args.top_extensions]
    for extension, sizes in ranked:
        ordered = sorted(sizes)
        over = sum(1 for s in ordered if s > args.ceiling)
        print(f"  {extension:<8} n={len(ordered):<7} p50 {percentile(ordered, .50):>7}  "
              f"p90 {percentile(ordered, .90):>7}  over ceiling {100.0 * over / len(ordered):>5.1f}%")

    print()
    print("Read this as an upper bound on the population, not an exact one. A related file is chosen")
    print("by base type and constructor dependency, not from every source file, and sizes are bytes")
    print("rather than characters, so a file with non-ASCII content reads larger than the ceiling")
    print("counts it. Both lean towards over-reporting how often the ceiling bites.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
