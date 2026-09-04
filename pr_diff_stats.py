#!/usr/bin/env python3
"""
Measures how large Azure DevOps pull requests actually are, in changed lines and in
estimated review tokens, so decisions about an AI reviewer's diff-size handling —
whether to summarise files, chunk the diff, or simply raise a budget — are made against
the organisation's real pull requests rather than an intuition about them.

Read-only. The only credential it wants is a PAT with `Code (read)`.

Output carries no file paths, no branch names, no commit messages, no author or reviewer
identity and no code: numbers, size bands, file extensions and repository names. Pass
--anonymise-repos to replace the names with repo-1...repo-N before sharing outside the
organisation that ran it. `--include-repo-names` opts out of that if you want per-repository breakdowns.

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
import hashlib
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

# Argus's own figure, from TokenEstimator.CharsPerToken, applied to the assembled diff.
# Diff characters are measured rather than reconstructed from line counts, at the same three
# lines of context Argus's UnifiedDiffGenerator emits, so the estimate is of the same text.
CHARS_PER_TOKEN = 2.2
DIFF_CONTEXT_LINES = 3

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

# Ported from Argus so the two agree on what "reviewable" means:
# Argus.Application.Common.NonReviewableFiles and Argus.Engine.Classification.DiffFilter.
# A file excluded here is one the reviewer fetches, spends its file-cap slot and its character
# budget on, and then discards before a lens ever sees it.
ARGUS_CONFIGURATION_EXT = {".json", ".yaml", ".yml", ".toml", ".properties", ".xml"}
ARGUS_DOCUMENTATION_EXT = {".md", ".txt", ".rst", ".adoc", ".docx"}
ARGUS_IMAGE_EXT = {".png", ".jpg", ".svg", ".gif", ".ico"}
ARGUS_FONT_EXT = {".woff", ".ttf"}
ARGUS_LOCKFILES = {
    "package-lock.json", "packages.lock.json", "yarn.lock", "pnpm-lock.yaml",
    "cargo.lock", "poetry.lock", "gemfile.lock", "go.sum", "composer.lock",
}
ARGUS_GENERATED_SUFFIX = (".designer.cs", ".g.cs", ".pyc")
ARGUS_MINIFIED_SUFFIX = (".min.js", ".min.css")
ARGUS_PATH_SEGMENTS = ("/migrations/", "/migration/", "/fixtures/", "/testdata/", "/test_data/")


def argus_exclusion_reason(path: str) -> str | None:
    """Why Argus would not review this file, or None when it would."""
    lowered = path.lower()
    name = lowered.rsplit("/", 1)[-1]
    ext = "." + name.rsplit(".", 1)[-1] if "." in name else ""

    if name in ARGUS_LOCKFILES:
        return "lockfile"
    if lowered.endswith(ARGUS_MINIFIED_SUFFIX):
        return "minified"
    if lowered.endswith(ARGUS_GENERATED_SUFFIX) or ".generated." in lowered:
        return "generated"
    if "__pycache__" in lowered:
        return "generated"
    if ext in ARGUS_CONFIGURATION_EXT:
        return "configuration"
    if ext in ARGUS_DOCUMENTATION_EXT:
        return "documentation"
    if ext in ARGUS_IMAGE_EXT:
        return "image"
    if ext in ARGUS_FONT_EXT:
        return "font"
    # Argus keeps EF model snapshots, which are the one migration file worth reading.
    if any(seg in f"/{lowered}" for seg in ARGUS_PATH_SEGMENTS) and not name.endswith("modelsnapshot.cs"):
        return "generated"
    return None



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
    statuses: Sequence[str], page_size: int, max_prs: int | None,
) -> tuple[list[dict[str, Any]], bool]:
    """
    Every pull request created in the window, bounded server-side by minTime. Without that the
    service returns the repository's whole history and the client pages until it sees something
    old enough, which is what a per-repository quota was previously covering up.

    The bool says the per-repository cap truncated the listing, so a short sample is visible in
    the output rather than passing for a quiet repository.
    """
    found: list[dict[str, Any]] = []
    truncated = False
    for status in statuses:
        skip = 0
        while max_prs is None or len(found) < max_prs:
            data = client.get(
                f"{urllib.parse.quote(project)}/_apis/git/repositories/{repo_id}/pullrequests",
                {
                    "searchCriteria.status": status,
                    "$top": page_size,
                    "$skip": skip,
                    "searchCriteria.queryTimeRangeType": "created",
                    "searchCriteria.minTime": since.isoformat(),
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

        if max_prs is not None and len(found) >= max_prs:
            truncated = True

    return (found[:max_prs] if max_prs is not None else found), truncated


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


def commits_present(repo_dir: str, shas: Sequence[str]) -> set[str]:
    """
    Which of these exist locally as commits. One process for the whole repository: probing each
    sha separately cost a process spawn per pull request, and spawn plus git's repository
    discovery dominates the run on a large organisation.
    """
    unique = list(dict.fromkeys(shas))
    if not unique:
        return set()

    probe = subprocess.run(
        ["git", "-C", repo_dir, "cat-file", "--batch-check"],
        input="\n".join(f"{sha}^{{commit}}" for sha in unique),
        capture_output=True, text=True,
    )
    present: set[str] = set()
    for sha, line in zip(unique, probe.stdout.splitlines()):
        if " commit " in line:
            present.add(sha)
    return present


def fetch_missing(repo_dir: str, pat: str, shas: Sequence[str]) -> None:
    """
    One fetch for every commit the clone is missing, rather than one per pull request. Commits on
    branches deleted after the merge are the usual absentees; the rest are already present.
    """
    present = commits_present(repo_dir, shas)
    missing = [sha for sha in dict.fromkeys(shas) if sha not in present]
    if not missing:
        return
    subprocess.run(
        ["git", "-C", repo_dir, *auth_args(pat), "fetch", "--quiet", "origin", *missing],
        capture_output=True, text=True,
    )


def numstat(repo_dir: str, base_sha: str, head_sha: str) -> list[list[Any]] | None:
    """
    (added, deleted, diff_chars, path, is_binary) per file, or None when a commit is unreachable.

    One git invocation produces both the counts and the patch, so diff characters are measured at
    the same context width the reviewer sees rather than reconstructed from a per-line average.
    git fails cleanly on a missing object, so no separate existence probe is needed.
    """
    result = subprocess.run(
        ["git", "-C", repo_dir, "diff", "--numstat", "--patch",
         f"-U{DIFF_CONTEXT_LINES}", f"{base_sha}...{head_sha}"],
        capture_output=True, text=True, errors="replace",
    )
    if result.returncode != 0:
        return None

    stdout = result.stdout
    marker = "\ndiff --git "
    split_at = stdout.find(marker)
    numstat_block = stdout if split_at < 0 else stdout[:split_at]
    patch_block = "" if split_at < 0 else stdout[split_at + 1:]

    chars_by_path: dict[str, int] = {}
    for section in patch_block.split("\ndiff --git "):
        if not section:
            continue
        header = section.split("\n", 1)[0]
        # "a/path b/path" — take the b-side, which is the path numstat reports for anything but
        # a deletion, and fall back to the a-side for deletions.
        parts = header.split(" b/", 1)
        path = parts[1].strip() if len(parts) == 2 else header.removeprefix("a/").strip()
        chars_by_path[path] = chars_by_path.get(path, 0) + len(section)

    rows: list[list[Any]] = []
    for line in numstat_block.splitlines():
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        added, deleted, path = parts
        # "-" marks a binary file: no line count, and the patch carries no text either.
        is_binary = added == "-"
        rows.append([
            0 if is_binary else int(added),
            0 if is_binary else int(deleted),
            chars_by_path.get(path, 0),
            path,
            is_binary,
        ])
    return rows



# --------------------------------------------------------------------------- #
# Cache
# --------------------------------------------------------------------------- #

CACHE_FORMAT_VERSION = 1


def repo_slug(project: str, repo_name: str) -> str:
    """A filename that survives repository names containing characters Windows rejects."""
    raw = f"{project}__{repo_name}"
    safe = re.sub(r"[^A-Za-z0-9_.-]", "-", raw)[:120]
    digest = hashlib.sha1(raw.encode()).hexdigest()[:8]
    return f"{safe}-{digest}"


def write_json_atomically(path: str, payload: Any) -> None:
    """Written to a temporary neighbour and renamed, so an interrupted run leaves no half file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    os.replace(tmp, path)


class Cache:
    """
    Per-file rows for completed pull requests, one file per repository.

    Raw rather than summarised, and free of any window: the questions this tool exists to answer
    are all "what would a different rule have done to the same corpus", and only the rows can
    answer those without another hours-long run. The window is applied when reporting.

    A completed pull request's merge commits never move, so its rows are immutable and the entry
    never expires. Entries whose commits the service has since collected are recorded as
    unavailable, so a repository does not re-attempt them on every run forever.
    """

    def __init__(self, root: str) -> None:
        self.root = root
        self.manifest_path = os.path.join(root, "manifest.json")
        self.manifest: dict[str, Any] = {"format_version": CACHE_FORMAT_VERSION, "repos": {}}
        if os.path.isfile(self.manifest_path):
            try:
                loaded = json.load(open(self.manifest_path, encoding="utf-8"))
                if loaded.get("format_version") == CACHE_FORMAT_VERSION:
                    self.manifest = loaded
            except (OSError, json.JSONDecodeError):
                pass
        self.hits = 0
        self.misses = 0

    def clone_dir(self, project: str, repo_name: str) -> str:
        return os.path.join(self.root, "clones", f"{repo_slug(project, repo_name)}.git")

    def _repo_path(self, project: str, repo_name: str) -> str:
        return os.path.join(self.root, "repos", f"{repo_slug(project, repo_name)}.json")

    def load_repo(self, project: str, repo_name: str) -> dict[str, dict[str, Any]]:
        path = self._repo_path(project, repo_name)
        if not os.path.isfile(path):
            return {}
        try:
            return {str(e["pr"]): e for e in json.load(open(path, encoding="utf-8"))}
        except (OSError, json.JSONDecodeError, KeyError, TypeError):
            return {}

    def save_repo(
        self, project: str, repo_name: str, entries: dict[str, dict[str, Any]],
        repo_id: str, truncated: bool,
    ) -> None:
        """Written as each repository finishes, so an interrupted run keeps what it has earned."""
        write_json_atomically(self._repo_path(project, repo_name), list(entries.values()))
        self.manifest["repos"][f"{project}/{repo_name}"] = {
            "file": os.path.relpath(self._repo_path(project, repo_name), self.root),
            "repo_id": repo_id,
            "last_scanned_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "pull_requests": len(entries),
            "unavailable": sum(1 for e in entries.values() if e.get("unavailable")),
            "list_truncated": truncated,
        }
        write_json_atomically(self.manifest_path, self.manifest)

    def is_usable(self, entry: dict[str, Any], base: str | None, head: str | None,
                  retry_unavailable: bool) -> bool:
        if entry.get("status") != "completed":
            return False
        if entry.get("unavailable"):
            return not retry_unavailable
        if base and entry.get("base") != base:
            return False
        if head and entry.get("head") != head:
            return False
        return entry.get("files") is not None


def extension_of(path: str) -> str:
    name = path.rsplit("/", 1)[-1]
    if "." not in name:
        return "(none)"
    return "." + name.rsplit(".", 1)[-1].lower()[:12]


def estimate_tokens(diff_chars: int) -> int:
    return int(diff_chars / CHARS_PER_TOKEN)


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
        tokens = [estimate_tokens(r["reviewable_chars"] or 0) for r in have_lines]
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
        big = [r for r in have_lines if estimate_tokens(r["reviewable_chars"] or 0) >= threshold]
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

    # Always broken down per repository, because "which repositories drive which file types" is the
    # question the totals raise and cannot answer: a documentation repository and a service whose
    # pipelines churn look identical in an organisation-wide extension tally.
    per_repo: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        per_repo.setdefault(row["repo"], []).append(row)

    named = not config.get("anonymise_repos")
    ordered = sorted(per_repo.items(), key=lambda kv: (-len(kv[1]), kv[0]))
    report["per_repository"] = {}

    for index, (name, items) in enumerate(ordered, start=1):
        label = name if named else f"repo-{index}"
        extensions: Counter[str] = Counter()
        ext_lines: Counter[str] = Counter()
        for item in items:
            extensions.update(item.get("extensions", {}))
            ext_lines.update(item.get("extension_lines", {}))

        with_lines = [i for i in items if i.get("changed_lines") is not None]
        entry: dict[str, Any] = {
            "pull_requests": len(items),
            "files_per_pr": summarise([i["files"] for i in items]),
            "changed_lines_per_pr": summarise([i["changed_lines"] for i in with_lines]),
            "reviewable_lines_per_pr": summarise([i["reviewable_lines"] for i in with_lines]),
            "top_extensions_by_file_count": dict(extensions.most_common(15)),
        }
        if ext_lines:
            entry["top_extensions_by_changed_lines"] = dict(ext_lines.most_common(15))
            total_lines = sum(ext_lines.values())
            if total_lines > 0:
                # The share a single extension takes of a repository's whole change volume. A
                # repository that is 95% one type is a different proposition from one that is mixed,
                # and only this ratio separates them.
                dominant_ext, dominant_lines = ext_lines.most_common(1)[0]
                entry["dominant_extension"] = dominant_ext
                entry["dominant_extension_share_percent"] = round(
                    100.0 * dominant_lines / total_lines, 1)

        report["per_repository"][label] = entry

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

    per_repo = report.get("per_repository") or {}
    dominated = [
        (label, entry["dominant_extension"], entry["dominant_extension_share_percent"],
         entry["pull_requests"])
        for label, entry in per_repo.items()
        if entry.get("dominant_extension_share_percent", 0) >= 60
    ]
    if dominated:
        add("Repositories dominated by one file type")
        add("  A repository that is almost entirely one type may not want reviewing at all.")
        for label, ext, share, prs in sorted(dominated, key=lambda d: -d[3])[:10]:
            add(f"  {label:>12}  {ext:>10}  {share:>5.0f}% of changed lines  ({prs:,} pull requests)")
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
    # No total quota: it bounded the sample rather than the run, and did it by abandoning whole
    # repositories once the count was met. --days is the bound that means something.
    parser.add_argument("--max-prs-per-repo", type=int, default=None,
                        help="Sample cap per repository. Unlimited by default; when it binds, "
                             "the repository is marked list_truncated in the cache manifest.")
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--cache", metavar="DIR", default="cache",
                        help="Per-repository cache and clones. Kept between runs, so a later run "
                             "only fetches pull requests created since the last scan.")
    parser.add_argument("--no-fetch", action="store_true",
                        help="Report from the cache alone. No clone, no git, no diff fetch.")
    parser.add_argument("--retry-unavailable", action="store_true",
                        help="Retry pull requests whose merge commits were unreachable.")
    parser.add_argument("--anonymise-repos", action="store_true",
                        help="Replace repository names with repo-1...repo-N in the output.")
    parser.add_argument("--triage-threshold-tokens", type=int, default=TRIAGE_THRESHOLD_TOKENS)
    parser.add_argument("--max-diff-token-budget", type=int, default=MAX_DIFF_TOKEN_BUDGET)
    parser.add_argument("--json", metavar="PATH", help="Write the full report as JSON.")
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args(argv)


def within_window(entry: dict[str, Any], since: datetime) -> bool:
    created = parse_time(entry.get("created_utc"))
    return created is None or created >= since


def build_row(repo: str, entry: dict[str, Any]) -> dict[str, Any]:
    """
    One pull request's metrics, derived from cached rows. Everything here is arithmetic, so a
    changed rule set is re-scored without touching the network or git.
    """
    ado_order: list[str] = entry.get("ado_order") or []
    files: list[list[Any]] | None = entry.get("files")

    row: dict[str, Any] = {
        "repo": repo,
        "files": len(ado_order),
        "reviewable_files": sum(1 for p in ado_order if argus_exclusion_reason(p) is None),
        "extensions": dict(Counter(
            extension_of(p) for p in ado_order if argus_exclusion_reason(p) is None)),
        "changed_lines": None,
        "reviewable_lines": None,
        "largest_file_lines": None,
        "extension_lines": {},
        "reviewable_chars": None,
        "excluded_chars": None,
        "binary_files": None,
    }

    if not files:
        return row

    ext_lines: Counter[str] = Counter()
    changed = reviewable = largest = reviewable_chars = excluded_chars = binaries = 0

    for added, deleted, chars, path, is_binary in files:
        lines = added + deleted
        changed += lines
        if is_binary:
            binaries += 1
        if argus_exclusion_reason(path) is None:
            reviewable += lines
            reviewable_chars += chars
            largest = max(largest, lines)
            # Accumulated rather than built as a dict comprehension: duplicate keys in one of
            # those overwrite, so every extension reported only its last file in the pull request.
            ext_lines[extension_of(path)] += lines
        else:
            excluded_chars += chars

    row["changed_lines"] = changed
    row["reviewable_lines"] = reviewable
    row["largest_file_lines"] = largest
    row["extension_lines"] = dict(ext_lines)
    row["reviewable_chars"] = reviewable_chars
    row["excluded_chars"] = excluded_chars
    row["binary_files"] = binaries
    return row


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    pat = os.environ.get("AZDO_PAT")
    if not pat:
        print("AZDO_PAT is not set. Use a PAT with Code (read) scope.", file=sys.stderr)
        return 2
    if not args.no_fetch and not shutil.which("git"):
        print("git is needed on PATH unless --no-fetch is passed.", file=sys.stderr)
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
    cache = Cache(args.cache)
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
                if repo["name"].lower() in excluded:
                    continue
                try:
                    prs, listing_truncated = list_pull_requests(
                        client, project, repo["id"], since, statuses,
                        args.page_size, args.max_prs_per_repo)
                except AdoError as exc:
                    if not args.quiet:
                        print(f"  {project}/{repo['name']}: {exc}", file=sys.stderr)
                    continue
                if not prs:
                    continue
                if not args.quiet:
                    print(f"  {project}/{repo['name']}: {len(prs)} pull requests",
                          file=sys.stderr, flush=True)

                entries = cache.load_repo(project, repo["name"])
                pending = [
                    pr for pr in prs
                    if not cache.is_usable(
                        entries.get(str(pr["pullRequestId"]), {}),
                        (pr.get("lastMergeTargetCommit") or {}).get("commitId"),
                        (pr.get("lastMergeSourceCommit") or {}).get("commitId"),
                        args.retry_unavailable)
                ]
                cache.hits += len(prs) - len(pending)
                cache.misses += len(pending)

                repo_dir = None
                if pending and not args.no_fetch:
                    repo_dir = cache.clone_dir(project, repo["name"])
                    if not os.path.isdir(repo_dir):
                        if not args.quiet:
                            print(f"    cloning {repo['name']}...", file=sys.stderr, flush=True)
                        if not clone_repo(repo["remoteUrl"], pat, repo_dir, args.quiet):
                            repo_dir = None

                    if repo_dir:
                        # One fetch for the whole repository rather than one per pull request.
                        fetch_missing(repo_dir, pat, [
                            sha
                            for pr in pending
                            for sha in (
                                (pr.get("lastMergeTargetCommit") or {}).get("commitId"),
                                (pr.get("lastMergeSourceCommit") or {}).get("commitId"),
                            )
                            if sha
                        ])

                for index, pr in enumerate(pending, start=1):
                    if not args.quiet and index % 25 == 0:
                        print(f"    {repo['name']}: {index}/{len(pending)} new",
                              file=sys.stderr, flush=True)

                    pr_id = pr["pullRequestId"]
                    base = (pr.get("lastMergeTargetCommit") or {}).get("commitId")
                    head = (pr.get("lastMergeSourceCommit") or {}).get("commitId")

                    try:
                        ado_order = pr_file_changes(client, project, repo["id"], pr_id)
                    except AdoError:
                        continue
                    if not ado_order:
                        continue

                    files = numstat(repo_dir, base, head) if (repo_dir and base and head) else None
                    entries[str(pr_id)] = {
                        "pr": pr_id,
                        "status": pr.get("status", "completed"),
                        "created_utc": pr.get("creationDate"),
                        "base": base,
                        "head": head,
                        "ado_order": ado_order,
                        "files": files,
                        # Recorded rather than retried forever: once the service has collected the
                        # merge commits of a deleted branch, no later run can recover them.
                        "unavailable": None if files is not None else "commits_unreachable",
                    }

                cache.save_repo(project, repo["name"], entries, repo["id"], listing_truncated)
                rows.extend(
                    build_row(f"{project}/{repo['name']}", entry)
                    for entry in entries.values()
                    if within_window(entry, since)
                )

    finally:
        pass

    if not rows:
        print("No pull requests matched.", file=sys.stderr)
        return 1

    config = {
        "days": args.days,
        "statuses": statuses,
        "projects_scanned": len(projects),
        "line_stats": True,
        "anonymise_repos": bool(args.anonymise_repos),
        "cache_hits": cache.hits,
        "cache_misses": cache.misses,
        "chars_per_token": CHARS_PER_TOKEN,
        "triage_threshold_tokens": args.triage_threshold_tokens,
        "max_diff_token_budget": args.max_diff_token_budget,
        "chars_per_token": CHARS_PER_TOKEN,
    }
    report = build_report(rows, config)
    if not args.no_fetch and report["pull_requests"]["with_line_counts"] == 0:
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
