#!/usr/bin/env python3
"""
Measures how large Azure DevOps pull requests actually are, in changed lines and in
estimated review tokens, so decisions about an AI reviewer's diff-size handling —
whether to summarise files, chunk the diff, or simply raise a budget — are made against
the organisation's real pull requests rather than an intuition about them.

Read-only. The only credential it wants is a PAT with `Code (read)`.

Output is deliberately non-identifying by default: numbers, size bands and file
extensions only. No file paths, no repository names, no branch names, no authors, no
code. `--include-repo-names` opts out of that if you want per-repository breakdowns.

Two modes:

  REST only (default)      File counts per pull request. Fast, no clone, no disk.
  --with-line-stats        Adds exact added/deleted line counts via `git diff --numstat`
                           against a shallow clone per repository. Slower, needs git and
                           temporary disk, and is the only way to get line counts:
                           Azure DevOps REST does not expose them.

Example:

    export AZDO_PAT=...
    python3 pr_diff_stats.py \
        --org https://dev.azure.com/contoso \
        --all-projects \
        --days 90 \
        --with-line-stats \
        --json pr-diff-stats.json

Then hand over `pr-diff-stats.json`.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Sequence

API_VERSION = "7.1"

# Argus's own thresholds, so the bands below line up with what the reviewer would do.
# Mirrored from appsettings.shared.json; override with the matching flags.
TRIAGE_THRESHOLD_TOKENS = 25_000
MAX_DIFF_TOKEN_BUDGET = 250_000
MAX_REVIEW_TOKENS = 500_000
MAX_CHUNKS = 3

# Argus estimates diff tokens at 4 characters per token. A changed line averages about
# 45 characters including the diff's own +/- prefix, so ~11 tokens per changed line.
# Crude by construction: it is the same crude number the reviewer itself budgets with.
CHARS_PER_TOKEN = 4
CHARS_PER_CHANGED_LINE = 45

# Files a reviewer should never spend tokens on. Counted separately rather than dropped,
# because "this pull request is huge but 90% of it is a lockfile" is the finding.
GENERATED_PATTERNS = [
    r"(^|/)package-lock\.json$", r"(^|/)yarn\.lock$", r"(^|/)pnpm-lock\.yaml$",
    r"(^|/)Cargo\.lock$", r"(^|/)poetry\.lock$", r"(^|/)Gemfile\.lock$",
    r"(^|/)go\.sum$", r"(^|/)composer\.lock$", r"(^|/)packages\.lock\.json$",
    r"\.min\.(js|css)$", r"\.map$", r"\.snap$",
    r"(^|/)(dist|build|out|bin|obj|node_modules|vendor)/",
    r"\.(designer|generated|g)\.(cs|vb|ts)$", r"_pb2?\.py$", r"\.pb\.go$",
    r"(^|/)migrations?/", r"\.(dll|exe|so|dylib|jar|zip|gz|png|jpe?g|gif|ico|pdf|woff2?|ttf)$",
]
GENERATED_RE = re.compile("|".join(GENERATED_PATTERNS), re.IGNORECASE)


class AdoError(RuntimeError):
    pass


class AdoClient:
    """Minimal REST client: GET + JSON, with retry on throttling and 5xx."""

    def __init__(self, org_url: str, pat: str, timeout: int = 60, max_retries: int = 5) -> None:
        self.org_url = org_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.pat = pat
        token = base64.b64encode(f":{pat}".encode()).decode()
        self._headers = {
            "Authorization": f"Basic {token}",
            "Accept": "application/json",
            "User-Agent": "argus-pr-diff-stats/1.0",
        }

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        query = dict(params or {})
        query.setdefault("api-version", API_VERSION)
        url = f"{self.org_url}/{path.lstrip('/')}?{urllib.parse.urlencode(query)}"

        last_error: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                request = urllib.request.Request(url, headers=self._headers)
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    if "text/html" in response.headers.get("Content-Type", ""):
                        raise AdoError(
                            "Azure DevOps returned an HTML sign-in page. The PAT in AZDO_PAT is "
                            "missing, expired, or lacks Code (read) scope on this organisation."
                        )
                    return json.loads(response.read())
            except urllib.error.HTTPError as exc:
                if exc.code in (401, 403):
                    raise AdoError(
                        f"HTTP {exc.code} on {url}. The PAT is rejected or lacks Code (read) scope."
                    ) from exc
                if exc.code == 404:
                    raise AdoError(f"HTTP 404 on {url}. Check the org/project name.") from exc
                if exc.code == 429 or exc.code >= 500:
                    last_error = exc
                    time.sleep(self._retry_delay(exc, attempt))
                    continue
                raise AdoError(f"HTTP {exc.code} on {url}: {exc.read()[:400]!r}") from exc
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                last_error = exc
                time.sleep(min(2**attempt, 30))

        raise AdoError(f"Gave up on {url} after {self.max_retries} attempts: {last_error}")

    @staticmethod
    def _retry_delay(exc: urllib.error.HTTPError, attempt: int) -> float:
        retry_after = exc.headers.get("Retry-After") if exc.headers else None
        if retry_after:
            try:
                return min(float(retry_after), 120.0)
            except ValueError:
                pass
        return min(2**attempt, 30)


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.replace("Z", "+00:00")
    if "." in text:
        head, _, tail = text.partition(".")
        frac = tail[:6].rstrip("+-")
        offset = tail[len(frac):]
        text = f"{head}.{frac}{offset}" if frac else head + offset
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def list_projects(client: AdoClient) -> list[str]:
    data = client.get("_apis/projects", {"$top": 1000})
    return sorted(p["name"] for p in data.get("value", []))


def list_repositories(client: AdoClient, project: str) -> list[dict[str, Any]]:
    data = client.get(f"{urllib.parse.quote(project)}/_apis/git/repositories")
    return [r for r in data.get("value", []) if not r.get("isDisabled")]


def list_pull_requests(
    client: AdoClient, project: str, repo_id: str, since: datetime,
    statuses: Sequence[str], page_size: int, max_prs: int,
) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for status in statuses:
        skip = 0
        while len(found) < max_prs:
            data = client.get(
                f"{urllib.parse.quote(project)}/_apis/git/repositories/{repo_id}/pullrequests",
                {
                    "searchCriteria.status": status,
                    "$top": page_size,
                    "$skip": skip,
                    "searchCriteria.queryTimeRangeType": "created",
                },
            )
            batch = data.get("value", [])
            if not batch:
                break
            stop = False
            for pr in batch:
                created = parse_time(pr.get("creationDate"))
                if created and created < since:
                    stop = True
                    continue
                found.append(pr)
            if stop or len(batch) < page_size:
                break
            skip += page_size
    return found[:max_prs]


def pr_file_changes(client: AdoClient, project: str, repo_id: str, pr_id: int) -> list[str]:
    """Changed file paths on the pull request's last iteration. Paths never leave this process."""
    iterations = client.get(
        f"{urllib.parse.quote(project)}/_apis/git/repositories/{repo_id}/pullrequests/{pr_id}/iterations"
    ).get("value", [])
    if not iterations:
        return []
    last = max(int(i["id"]) for i in iterations)
    changes = client.get(
        f"{urllib.parse.quote(project)}/_apis/git/repositories/{repo_id}/"
        f"pullrequests/{pr_id}/iterations/{last}/changes",
        {"$top": 5000},
    ).get("changeEntries", [])
    paths = []
    for entry in changes:
        item = entry.get("item") or {}
        if item.get("isFolder"):
            continue
        path = item.get("path")
        if path:
            paths.append(path.lstrip("/"))
    return paths


def auth_args(pat: str) -> list[str]:
    """
    Credentials for one git invocation. Passed per command rather than written into the clone's
    config: a PAT in a repository's config outlives the run and ends up in whatever the directory
    is later copied into.
    """
    header = base64.b64encode(f":{pat}".encode()).decode()
    return ["-c", f"http.extraheader=Authorization: Basic {header}"]


def clone_repo(remote_url: str, pat: str, dest: str, quiet: bool) -> bool:
    """
    Full bare clone. Deliberately not --filter=blob:none: git diff needs file contents, and a
    blobless clone fetches them lazily one round trip at a time, so a twenty-file pull request
    costs forty network calls. Paying once up front is far cheaper across a repository's history.
    """
    cmd = ["git", *auth_args(pat), "clone", "--bare", "--quiet", remote_url, dest]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0 and not quiet:
        print(f"    clone failed: {result.stderr.strip()[:160]}", file=sys.stderr)
    return result.returncode == 0


def have_commits(repo_dir: str, shas: Sequence[str]) -> bool:
    for sha in shas:
        probe = subprocess.run(
            ["git", "-C", repo_dir, "cat-file", "-e", f"{sha}^{{commit}}"],
            capture_output=True, text=True,
        )
        if probe.returncode != 0:
            return False
    return True


def fetch_missing(repo_dir: str, pat: str, shas: Sequence[str]) -> None:
    """
    One fetch for every commit the clone is missing, rather than one per pull request. Commits on
    branches deleted after the merge are the usual absentees; the rest are already present.
    """
    missing = [sha for sha in dict.fromkeys(shas) if not have_commits(repo_dir, [sha])]
    if not missing:
        return
    subprocess.run(
        ["git", "-C", repo_dir, *auth_args(pat), "fetch", "--quiet", "origin", *missing],
        capture_output=True, text=True,
    )


def numstat(repo_dir: str, base_sha: str, head_sha: str) -> list[tuple[int, int, str]] | None:
    """(added, deleted, path) per file. None when either commit is missing from the clone."""
    if not have_commits(repo_dir, [base_sha, head_sha]):
        return None
    result = subprocess.run(
        ["git", "-C", repo_dir, "diff", "--numstat", f"{base_sha}...{head_sha}"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        return None
    rows: list[tuple[int, int, str]] = []
    for line in result.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        added, deleted, path = parts
        # "-" marks a binary file; it has no line count, so it contributes files but no lines.
        rows.append((0 if added == "-" else int(added),
                     0 if deleted == "-" else int(deleted),
                     path))
    return rows


def extension_of(path: str) -> str:
    name = path.rsplit("/", 1)[-1]
    if "." not in name:
        return "(none)"
    return "." + name.rsplit(".", 1)[-1].lower()[:12]


def estimate_tokens(changed_lines: int) -> int:
    return int(changed_lines * CHARS_PER_CHANGED_LINE / CHARS_PER_TOKEN)


def percentile(values: Sequence[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    position = fraction * (len(ordered) - 1)
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    return float(ordered[low] + (ordered[high] - ordered[low]) * (position - low))


def summarise(values: Sequence[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0}
    return {
        "count": len(values),
        "min": round(min(values), 1),
        "p50": round(percentile(values, 0.50) or 0, 1),
        "p75": round(percentile(values, 0.75) or 0, 1),
        "p90": round(percentile(values, 0.90) or 0, 1),
        "p95": round(percentile(values, 0.95) or 0, 1),
        "p99": round(percentile(values, 0.99) or 0, 1),
        "max": round(max(values), 1),
        "mean": round(statistics.fmean(values), 1),
        "total": round(sum(values), 1),
    }


def band(value: int, edges: Sequence[int]) -> str:
    previous = 0
    for edge in edges:
        if value < edge:
            return f"{previous:,}-{edge - 1:,}"
        previous = edge
    return f"{previous:,}+"


def band_counts(values: Iterable[int], edges: Sequence[int]) -> dict[str, int]:
    """Counts per band, in ascending band order — a plain Counter sorts 10,000+ before 10-49."""
    labels = [band(0, edges)] + [band(edge, edges) for edge in edges]
    tally = Counter(band(v, edges) for v in values)
    return {label: tally.get(label, 0) for label in labels}


def build_report(rows: list[dict[str, Any]], config: dict[str, Any]) -> dict[str, Any]:
    have_lines = [r for r in rows if r.get("changed_lines") is not None]
    threshold = config["triage_threshold_tokens"]
    chunk_point = config["max_diff_token_budget"]

    files_all = [r["files"] for r in rows]
    files_reviewable = [r["reviewable_files"] for r in rows]

    report: dict[str, Any] = {
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "config": config,
        "pull_requests": {
            "total": len(rows),
            "with_line_counts": len(have_lines),
        },
        "files_per_pr": summarise(files_all),
        "reviewable_files_per_pr": summarise(files_reviewable),
        "files_per_pr_bands": band_counts(
            (r["files"] for r in rows), [1, 2, 4, 6, 11, 21, 51, 101]),
    }

    if have_lines:
        changed = [r["changed_lines"] for r in have_lines]
        reviewable = [r["reviewable_lines"] for r in have_lines]
        tokens = [estimate_tokens(r["reviewable_lines"]) for r in have_lines]
        generated_share = [
            round(100.0 * (r["changed_lines"] - r["reviewable_lines"]) / r["changed_lines"], 1)
            for r in have_lines if r["changed_lines"] > 0
        ]

        over = [t for t in tokens if t >= threshold]
        over_chunk = [t for t in tokens if t >= chunk_point]

        report["changed_lines_per_pr"] = summarise(changed)
        report["reviewable_lines_per_pr"] = summarise(reviewable)
        report["estimated_review_tokens_per_pr"] = summarise(tokens)
        report["generated_share_percent"] = summarise(generated_share)
        report["changed_lines_bands"] = band_counts(
            changed, [10, 50, 100, 250, 500, 1000, 2500, 5000, 10000])
        report["reviewable_token_bands"] = band_counts(
            tokens, [1000, 5000, 15000, 25000, 50000, 100000, 250000])
        report["thresholds"] = {
            "triage_threshold_tokens": threshold,
            "pull_requests_at_or_over_triage_threshold": len(over),
            "percent_at_or_over_triage_threshold": round(100.0 * len(over) / len(tokens), 2),
            "chunking_point_tokens": chunk_point,
            "pull_requests_at_or_over_chunking_point": len(over_chunk),
            "percent_at_or_over_chunking_point": round(100.0 * len(over_chunk) / len(tokens), 2),
        }
        # Is a big pull request big because of many files, or a few enormous ones? Summarising
        # whole files helps the first shape and barely dents the second.
        big = [r for r in have_lines if estimate_tokens(r["reviewable_lines"]) >= threshold]
        if big:
            report["shape_of_large_prs"] = {
                "count": len(big),
                "reviewable_files": summarise([r["reviewable_files"] for r in big]),
                "reviewable_lines": summarise([r["reviewable_lines"] for r in big]),
                "lines_in_largest_file": summarise(
                    [r["largest_file_lines"] for r in big if r.get("largest_file_lines") is not None]),
                "largest_file_share_percent": summarise([
                    round(100.0 * r["largest_file_lines"] / r["reviewable_lines"], 1)
                    for r in big
                    if r.get("largest_file_lines") is not None and r["reviewable_lines"] > 0
                ]),
            }

    extensions: Counter[str] = Counter()
    ext_lines: Counter[str] = Counter()
    for row in rows:
        extensions.update(row.get("extensions", {}))
        ext_lines.update(row.get("extension_lines", {}))
    report["top_extensions_by_file_count"] = dict(extensions.most_common(30))
    if ext_lines:
        report["top_extensions_by_changed_lines"] = dict(ext_lines.most_common(30))

    if config.get("include_repo_names"):
        per_repo: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            per_repo.setdefault(row["repo"], []).append(row)
        report["per_repository"] = {
            name: {
                "pull_requests": len(items),
                "files_per_pr": summarise([i["files"] for i in items]),
                "changed_lines_per_pr": summarise(
                    [i["changed_lines"] for i in items if i.get("changed_lines") is not None]),
            }
            for name, items in sorted(per_repo.items())
        }

    return report


def render(report: dict[str, Any]) -> str:
    out: list[str] = []
    add = out.append
    add("Pull request diff size")
    add("=" * 60)
    prs = report["pull_requests"]
    add(f"Pull requests sampled : {prs['total']:,}")
    add(f"With exact line counts: {prs['with_line_counts']:,}")
    add("")

    def table(title: str, stats: dict[str, Any], unit: str = "") -> None:
        if not stats or not stats.get("count"):
            return
        add(title)
        add(f"  p50 {stats['p50']:>10,.0f}{unit}   p90 {stats['p90']:>10,.0f}{unit}"
            f"   p99 {stats['p99']:>10,.0f}{unit}   max {stats['max']:>10,.0f}{unit}")
        add("")

    table("Files changed per pull request", report.get("files_per_pr"))
    table("Reviewable files (generated excluded)", report.get("reviewable_files_per_pr"))
    table("Changed lines per pull request", report.get("changed_lines_per_pr"))
    table("Reviewable changed lines", report.get("reviewable_lines_per_pr"))
    table("Estimated review tokens", report.get("estimated_review_tokens_per_pr"))

    thresholds = report.get("thresholds")
    if thresholds:
        add("Against the reviewer's thresholds")
        add(f"  at or over triage threshold ({thresholds['triage_threshold_tokens']:,} tokens): "
            f"{thresholds['pull_requests_at_or_over_triage_threshold']:,} "
            f"({thresholds['percent_at_or_over_triage_threshold']}%)")
        add(f"  at or over chunking point ({thresholds['chunking_point_tokens']:,} tokens): "
            f"{thresholds['pull_requests_at_or_over_chunking_point']:,} "
            f"({thresholds['percent_at_or_over_chunking_point']}%)")
        add("")

    for key, title in (("changed_lines_bands", "Changed lines, distribution"),
                       ("reviewable_token_bands", "Estimated tokens, distribution"),
                       ("files_per_pr_bands", "Files changed, distribution")):
        bands = report.get(key)
        if bands:
            add(title)
            width = max(bands.values()) or 1
            for label, count in bands.items():
                if count == 0:
                    continue
                bar = "#" * max(1, int(30 * count / width))
                add(f"  {label:>16}  {count:>6,}  {bar}")
            add("")

    shape = report.get("shape_of_large_prs")
    if shape:
        add(f"Shape of the {shape['count']:,} largest pull requests")
        add(f"  reviewable files      p50 {shape['reviewable_files']['p50']:,.0f}"
            f"   p90 {shape['reviewable_files']['p90']:,.0f}")
        if shape.get("largest_file_share_percent", {}).get("count"):
            add(f"  biggest file's share  p50 {shape['largest_file_share_percent']['p50']:.0f}%"
                f"   p90 {shape['largest_file_share_percent']['p90']:.0f}%")
        add("")

    exts = report.get("top_extensions_by_changed_lines") or report.get("top_extensions_by_file_count")
    if exts:
        add("Most-changed file types")
        for ext, count in list(exts.items())[:12]:
            add(f"  {ext:>12}  {count:>10,}")
        add("")

    return "\n".join(out)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure pull request diff sizes in an Azure DevOps organisation.")
    parser.add_argument("--org", required=True, help="https://dev.azure.com/<org>")
    parser.add_argument("--project", action="append", default=[], help="Repeatable.")
    parser.add_argument("--all-projects", action="store_true")
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--status", action="append", default=[],
                        help="Repeatable. Default: completed.")
    parser.add_argument("--exclude-repo", action="append", default=[])
    parser.add_argument("--max-prs-per-repo", type=int, default=200)
    parser.add_argument("--max-prs-total", type=int, default=3000)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--with-line-stats", action="store_true",
                        help="Clone each repository to get exact added/deleted line counts.")
    parser.add_argument("--keep-clones", metavar="DIR",
                        help="Reuse and keep clones in DIR instead of a temporary directory.")
    parser.add_argument("--include-repo-names", action="store_true",
                        help="Include a per-repository breakdown. Off by default.")
    parser.add_argument("--triage-threshold-tokens", type=int, default=TRIAGE_THRESHOLD_TOKENS)
    parser.add_argument("--max-diff-token-budget", type=int, default=MAX_DIFF_TOKEN_BUDGET)
    parser.add_argument("--json", metavar="PATH", help="Write the full report as JSON.")
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    pat = os.environ.get("AZDO_PAT")
    if not pat:
        print("AZDO_PAT is not set. Use a PAT with Code (read) scope.", file=sys.stderr)
        return 2
    if args.with_line_stats and not shutil.which("git"):
        print("--with-line-stats needs git on PATH.", file=sys.stderr)
        return 2

    client = AdoClient(args.org, pat)
    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    statuses = args.status or ["completed"]

    try:
        projects = list_projects(client) if args.all_projects else args.project
    except AdoError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    if not projects:
        print("Give --project NAME or --all-projects.", file=sys.stderr)
        return 2

    excluded = {name.lower() for name in args.exclude_repo}
    clone_root = args.keep_clones or tempfile.mkdtemp(prefix="pr-diff-stats-")
    os.makedirs(clone_root, exist_ok=True)
    rows: list[dict[str, Any]] = []

    try:
        for project in projects:
            try:
                repos = list_repositories(client, project)
            except AdoError as exc:
                if not args.quiet:
                    print(f"  {project}: {exc}", file=sys.stderr)
                continue

            for repo in repos:
                if len(rows) >= args.max_prs_total:
                    break
                if repo["name"].lower() in excluded:
                    continue
                try:
                    prs = list_pull_requests(client, project, repo["id"], since, statuses,
                                             args.page_size, args.max_prs_per_repo)
                except AdoError as exc:
                    if not args.quiet:
                        print(f"  {project}/{repo['name']}: {exc}", file=sys.stderr)
                    continue
                if not prs:
                    continue
                if not args.quiet:
                    print(f"  {project}/{repo['name']}: {len(prs)} pull requests", file=sys.stderr)

                repo_dir = None
                if args.with_line_stats:
                    repo_dir = os.path.join(clone_root, f"{project}__{repo['name']}.git")
                    if not os.path.isdir(repo_dir):
                        if not args.quiet:
                            print(f"    cloning {repo['name']}...", file=sys.stderr, flush=True)
                        if not clone_repo(repo["remoteUrl"], pat, repo_dir, args.quiet):
                            repo_dir = None

                    if repo_dir:
                        # One fetch for the whole repository rather than one per pull request.
                        wanted = [
                            sha
                            for pr in prs
                            for sha in (
                                (pr.get("lastMergeTargetCommit") or {}).get("commitId"),
                                (pr.get("lastMergeSourceCommit") or {}).get("commitId"),
                            )
                            if sha
                        ]
                        fetch_missing(repo_dir, pat, wanted)

                for index, pr in enumerate(prs, start=1):
                    if len(rows) >= args.max_prs_total:
                        break
                    if not args.quiet and index % 25 == 0:
                        print(f"    {repo['name']}: {index}/{len(prs)}", file=sys.stderr, flush=True)
                    try:
                        paths = pr_file_changes(client, project, repo["id"], pr["pullRequestId"])
                    except AdoError:
                        continue
                    if not paths:
                        continue

                    reviewable_paths = [p for p in paths if not GENERATED_RE.search(p)]
                    row: dict[str, Any] = {
                        "repo": f"{project}/{repo['name']}",
                        "files": len(paths),
                        "reviewable_files": len(reviewable_paths),
                        "extensions": dict(Counter(extension_of(p) for p in reviewable_paths)),
                        "changed_lines": None,
                        "reviewable_lines": None,
                        "largest_file_lines": None,
                        "extension_lines": {},
                    }

                    if repo_dir:
                        base = (pr.get("lastMergeTargetCommit") or {}).get("commitId")
                        head = (pr.get("lastMergeSourceCommit") or {}).get("commitId")
                        stats = numstat(repo_dir, base, head) if base and head else None
                        if stats:
                            row["changed_lines"] = sum(a + d for a, d, _ in stats)
                            keep = [(a, d, p) for a, d, p in stats if not GENERATED_RE.search(p)]
                            row["reviewable_lines"] = sum(a + d for a, d, _ in keep)
                            row["largest_file_lines"] = max((a + d for a, d, _ in keep), default=0)
                            row["extension_lines"] = dict(
                                Counter({extension_of(p): a + d for a, d, p in keep}))
                    rows.append(row)
    finally:
        if not args.keep_clones:
            shutil.rmtree(clone_root, ignore_errors=True)

    if not rows:
        print("No pull requests matched.", file=sys.stderr)
        return 1

    config = {
        "days": args.days,
        "statuses": statuses,
        "projects_scanned": len(projects),
        "line_stats": bool(args.with_line_stats),
        "include_repo_names": bool(args.include_repo_names),
        "triage_threshold_tokens": args.triage_threshold_tokens,
        "max_diff_token_budget": args.max_diff_token_budget,
        "chars_per_changed_line": CHARS_PER_CHANGED_LINE,
        "chars_per_token": CHARS_PER_TOKEN,
    }
    report = build_report(rows, config)
    if args.with_line_stats and report["pull_requests"]["with_line_counts"] == 0:
        print(
            "Line counts were requested but none were produced: every clone or commit lookup "
            "failed. Check that the PAT has Code (read) on these repositories.",
            file=sys.stderr,
        )
    print(render(report))
    if args.json:
        with open(args.json, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)
        print(f"Wrote {args.json}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
