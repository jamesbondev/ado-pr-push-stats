#!/usr/bin/env python3
"""
Measures what pull requests in an Azure DevOps organisation actually link to, so decisions
about an AI reviewer's acceptance-criteria check — which work item types to assess, what to
do with a Task, whether the linked-item cap is losing anything — are made against the
organisation's real pull requests rather than an intuition about them.

Read-only. Needs a PAT with `Code (read)` for the pull request listing and
`Work Items (read)` for the work item lookups.

Output carries no work item ids, titles, descriptions or criteria text and no pull request
ids: counts, work item type names, field reference names and repository names. Pass
--anonymise-repos to replace the names before sharing outside the organisation that ran it.

The reviewer's proposed rules are replayed over every pull request:

- only the accepted types (default: Product Backlog Item, Bug, Incident Action) carry
  criteria, read from the acceptance-criteria field configured for that type;
- a Task whose parent is linked and accepted is scope context, not criteria;
- any other Task is assessed on its description as a single criterion;
- everything else is ignored.

Every parameter is a flag, so a different type list or field mapping re-scores from the
cache with no API calls.

Example:

    export AZDO_PAT=...
    python3 pr_work_item_stats.py \\
        --org https://dev.azure.com/contoso \\
        --all-projects \\
        --days 90 \\
        --cache work-items-raw.json \\
        --json pr-work-item-stats.json

Then hand over `pr-work-item-stats.json`. The cache holds work item ids; keep it local.
"""

from __future__ import annotations

import argparse
import base64
import html
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Sequence

API_VERSION = "7.1"
CACHE_FORMAT_VERSION = 1

# The reviewer's proposed policy. Mirrored as defaults so the report scores the rule set
# under discussion; every one is a flag.
ACCEPTED_TYPES = ["Product Backlog Item", "Bug", "Incident Action"]
TASK_TYPES = ["Task"]
DEFAULT_AC_FIELD = "Microsoft.VSTS.Common.AcceptanceCriteria"
AC_FIELD_BY_TYPE = {"Incident Action": "Custom.IncActionDescription"}
DESCRIPTION_FIELD = "System.Description"
PARENT_FIELD = "System.Parent"
TYPE_FIELD = "System.WorkItemType"
PARENT_LINK = "System.LinkTypes.Hierarchy-Reverse"

# Argus's SourceControl:AzureDevOps:MaxLinkedWorkItems, applied to the relations list in
# the order the service returns it, before the work items themselves are fetched.
MAX_LINKED_WORK_ITEMS = 5

# Field reference names worth reporting when populated: anything that could be a criteria
# or description field, plus every custom field so an unexpected criteria home shows up.
INTERESTING_FIELD_RE = re.compile(
    r"acceptance|criteria|description|repro|^Custom\.", re.IGNORECASE)

# Mirrors Argus's AcCriteriaClassifier: tags stripped before entities are decoded, so an
# escaped "&lt;p&gt;" is text and a bare <p></p> is not.
HTML_TAG_RE = re.compile(r"""</?[a-zA-Z!](?:[^>"']|"[^"]*"|'[^']*')*>""")

BATCH_SIZE = 200


class AdoError(RuntimeError):
    pass


class AdoClient:
    """Minimal REST client: GET/POST + JSON, with retry on throttling and 5xx."""

    def __init__(self, org_url: str, pat: str, timeout: int = 60, max_retries: int = 5) -> None:
        self.org_url = org_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        token = base64.b64encode(f":{pat}".encode()).decode()
        self._headers = {
            "Authorization": f"Basic {token}",
            "Accept": "application/json",
            "User-Agent": "argus-pr-work-item-stats/1.0",
        }

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._send(path, params, None)

    def post(self, path: str, body: dict[str, Any],
             params: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._send(path, params, body)

    def _send(self, path: str, params: dict[str, Any] | None,
              body: dict[str, Any] | None) -> dict[str, Any]:
        query = dict(params or {})
        query.setdefault("api-version", API_VERSION)
        url = f"{self.org_url}/{path.lstrip('/')}?{urllib.parse.urlencode(query)}"
        data = json.dumps(body).encode() if body is not None else None
        headers = dict(self._headers)
        if data is not None:
            headers["Content-Type"] = "application/json"

        last_error: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                request = urllib.request.Request(url, data=data, headers=headers,
                                                 method="POST" if data else "GET")
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    if "text/html" in response.headers.get("Content-Type", ""):
                        raise AdoError(
                            "Azure DevOps returned an HTML sign-in page. The PAT in AZDO_PAT is "
                            "missing, expired, or lacks the read scopes on this organisation."
                        )
                    return json.loads(response.read())
            except urllib.error.HTTPError as exc:
                if exc.code in (401, 403):
                    raise AdoError(
                        f"HTTP {exc.code} on {url}. The PAT is rejected or lacks Code (read) "
                        f"or Work Items (read) scope."
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


def has_text(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    return bool(html.unescape(HTML_TAG_RE.sub(" ", value)).strip())


# --------------------------------------------------------------------------- #
# Collection
# --------------------------------------------------------------------------- #


@dataclass
class PrRecord:
    repository: str
    status: str
    created_at: datetime
    work_item_ids: list[int]  # in the order the service listed them


@dataclass
class WorkItem:
    id: int
    type: str
    parent_id: int | None
    populated: list[str] = field(default_factory=list)  # interesting fields carrying text


def list_projects(client: AdoClient) -> list[str]:
    data = client.get("_apis/projects", {"$top": 1000})
    return sorted(p["name"] for p in data.get("value", []))


def list_repositories(client: AdoClient, project: str) -> list[dict[str, Any]]:
    data = client.get(f"{urllib.parse.quote(project)}/_apis/git/repositories")
    return [r for r in data.get("value", []) if not r.get("isDisabled")]


def list_pull_requests(
    client: AdoClient, project: str, repo_id: str, since: datetime,
    statuses: Sequence[str], page_size: int,
) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for status in statuses:
        skip = 0
        while True:
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
    return found


def pr_work_item_ids(client: AdoClient, project: str, repo_id: str, pr_id: int) -> list[int]:
    """Linked work item ids in service order: the same list, in the same order, Argus caps."""
    data = client.get(
        f"{urllib.parse.quote(project)}/_apis/git/repositories/{repo_id}/"
        f"pullrequests/{pr_id}/workitems"
    )
    ids: list[int] = []
    for ref in data.get("value", []):
        try:
            ids.append(int(ref["id"]))
        except (KeyError, TypeError, ValueError):
            continue
    return ids


def parent_from_relations(item: dict[str, Any]) -> int | None:
    for relation in item.get("relations") or []:
        if relation.get("rel") != PARENT_LINK:
            continue
        tail = str(relation.get("url", "")).rstrip("/").rsplit("/", 1)[-1]
        if tail.isdigit():
            return int(tail)
    return None


def fetch_work_items(client: AdoClient, ids: Iterable[int]) -> dict[int, WorkItem]:
    """
    Every field, in batches of 200, so an unexpected criteria field shows up rather than
    being filtered out by a list of the fields we expected. Deleted items are omitted rather
    than failing the batch. Field values never leave this function: only which interesting
    fields carry text is kept.
    """
    wanted = sorted(set(ids))
    out: dict[int, WorkItem] = {}
    for start in range(0, len(wanted), BATCH_SIZE):
        chunk = wanted[start:start + BATCH_SIZE]
        data = client.post(
            "_apis/wit/workitemsbatch",
            {"ids": chunk, "$expand": "relations", "errorPolicy": "omit"},
        )
        for item in data.get("value", []):
            fields = item.get("fields") or {}
            parent = fields.get(PARENT_FIELD)
            parent_id = int(parent) if isinstance(parent, int) else parent_from_relations(item)
            populated = sorted(
                name for name, value in fields.items()
                if INTERESTING_FIELD_RE.search(name) and has_text(value)
            )
            out[int(item["id"])] = WorkItem(
                id=int(item["id"]),
                type=str(fields.get(TYPE_FIELD) or "(unknown)"),
                parent_id=parent_id,
                populated=populated,
            )
    return out


def collect(
    client: AdoClient, projects: Sequence[str], excluded: set[str], since: datetime,
    statuses: Sequence[str], page_size: int, concurrency: int, quiet: bool,
) -> tuple[list[PrRecord], dict[int, WorkItem]]:
    records: list[PrRecord] = []
    work_items: dict[int, WorkItem] = {}

    for project in projects:
        try:
            repos = list_repositories(client, project)
        except AdoError as exc:
            if not quiet:
                print(f"  {project}: {exc}", file=sys.stderr)
            continue

        for repo in repos:
            if repo["name"].lower() in excluded:
                continue
            try:
                prs = list_pull_requests(client, project, repo["id"], since, statuses, page_size)
            except AdoError as exc:
                if not quiet:
                    print(f"  {project}/{repo['name']}: {exc}", file=sys.stderr)
                continue
            if not prs:
                continue
            if not quiet:
                print(f"  {project}/{repo['name']}: {len(prs)} pull requests",
                      file=sys.stderr, flush=True)

            def fetch(pr: dict[str, Any]) -> PrRecord | None:
                created = parse_time(pr.get("creationDate"))
                if created is None:
                    return None
                try:
                    ids = pr_work_item_ids(client, project, repo["id"], int(pr["pullRequestId"]))
                except AdoError as exc:
                    if not quiet:
                        print(f"    pull request skipped: {exc}", file=sys.stderr)
                    return None
                return PrRecord(
                    repository=f"{project}/{repo['name']}",
                    status=str(pr.get("status", "")),
                    created_at=created,
                    work_item_ids=ids,
                )

            with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
                fetched = [r for r in pool.map(fetch, prs) if r is not None]
            records.extend(fetched)

    # One lookup per distinct work item across the whole scan: a backlog item linked from
    # ten pull requests is fetched once. Parents come in a second round so a Task's parent
    # type is known even when the parent was never linked to any pull request.
    linked_ids = {wid for record in records for wid in record.work_item_ids}
    if not quiet:
        print(f"  fetching {len(linked_ids)} distinct work items", file=sys.stderr, flush=True)
    work_items.update(fetch_work_items(client, linked_ids))

    parent_ids = {
        item.parent_id for item in work_items.values()
        if item.parent_id is not None and item.parent_id not in work_items
    }
    if parent_ids:
        if not quiet:
            print(f"  fetching {len(parent_ids)} parents", file=sys.stderr, flush=True)
        work_items.update(fetch_work_items(client, parent_ids))

    return records, work_items


# --------------------------------------------------------------------------- #
# Cache
# --------------------------------------------------------------------------- #


def save_cache(path: str, records: Sequence[PrRecord], work_items: dict[int, WorkItem],
               meta: dict[str, Any]) -> None:
    payload = {
        "format_version": CACHE_FORMAT_VERSION,
        "meta": meta,
        "pull_requests": [
            {
                "repository": r.repository,
                "status": r.status,
                "created_at": r.created_at.isoformat(),
                "work_item_ids": r.work_item_ids,
            }
            for r in records
        ],
        "work_items": {
            str(w.id): {"type": w.type, "parent": w.parent_id, "populated": w.populated}
            for w in work_items.values()
        },
    }
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, separators=(",", ":"))
    os.replace(tmp, path)


def load_cache(path: str) -> tuple[list[PrRecord], dict[int, WorkItem], dict[str, Any]]:
    with open(path, encoding="utf-8") as handle:
        payload = json.load(handle)
    version = payload.get("format_version")
    if version != CACHE_FORMAT_VERSION:
        raise AdoError(
            f"{path} is cache format v{version}, this build reads v{CACHE_FORMAT_VERSION}. "
            f"Re-run with --refresh to rebuild it."
        )
    records = [
        PrRecord(
            repository=item["repository"],
            status=item["status"],
            created_at=parse_time(item["created_at"]) or datetime.now(timezone.utc),
            work_item_ids=[int(i) for i in item["work_item_ids"]],
        )
        for item in payload["pull_requests"]
    ]
    work_items = {
        int(key): WorkItem(id=int(key), type=value["type"], parent_id=value.get("parent"),
                           populated=list(value.get("populated", [])))
        for key, value in payload.get("work_items", {}).items()
    }
    return records, work_items, payload.get("meta", {})


# --------------------------------------------------------------------------- #
# Scoring
# --------------------------------------------------------------------------- #


@dataclass
class Policy:
    accepted: set[str]
    tasks: set[str]
    ac_field_by_type: dict[str, str]
    default_ac_field: str
    max_linked: int

    def ac_field(self, work_item_type: str) -> str:
        return self.ac_field_by_type.get(work_item_type, self.default_ac_field)


def classify_item(item: WorkItem | None, linked: set[int],
                  work_items: dict[int, WorkItem], policy: Policy) -> str:
    """
    One linked work item's fate under the proposed rules:

      criteria           accepted type, criteria field populated: assessed as criteria
      no_criteria        accepted type, criteria field empty: reported as unassessed
      task_scope         Task whose parent is linked and accepted: scope context only
      task_single        any other Task with a description: one criterion from the description
      task_empty         Task with neither a linked accepted parent nor a description
      ignored            every other type
      missing            the service no longer returns it (deleted or inaccessible)
    """
    if item is None:
        return "missing"
    if item.type in policy.accepted:
        return "criteria" if policy.ac_field(item.type) in item.populated else "no_criteria"
    if item.type in policy.tasks:
        parent = work_items.get(item.parent_id) if item.parent_id is not None else None
        if parent is not None and parent.id in linked and parent.type in policy.accepted:
            return "task_scope"
        return "task_single" if DESCRIPTION_FIELD in item.populated else "task_empty"
    return "ignored"


def task_parent_bucket(item: WorkItem, linked: set[int],
                       work_items: dict[int, WorkItem], policy: Policy) -> str:
    if item.parent_id is None:
        return "no parent"
    parent = work_items.get(item.parent_id)
    if parent is None:
        return "parent not readable"
    accepted = parent.type in policy.accepted
    is_linked = parent.id in linked
    if accepted and is_linked:
        return "accepted parent, linked"
    if accepted:
        return "accepted parent, not linked"
    if is_linked:
        return f"other parent ({parent.type}), linked"
    return f"other parent ({parent.type}), not linked"


def pr_outcome(fates: Sequence[str]) -> str:
    """What the AC check would have to work with on this pull request."""
    if not fates:
        return "nothing linked"
    live = {f for f in fates if f not in ("ignored", "missing")}
    if not live:
        return "only ignored types"
    has_criteria = "criteria" in live
    has_task = "task_single" in live
    if has_criteria and has_task:
        return "criteria + task"
    if has_criteria:
        return "criteria"
    if has_task:
        return "task only"
    return "accepted but empty"


def build_report(records: Sequence[PrRecord], work_items: dict[int, WorkItem],
                 policy: Policy, config: dict[str, Any]) -> dict[str, Any]:
    outcomes: Counter[str] = Counter()
    fates: Counter[str] = Counter()
    types_by_item: Counter[str] = Counter()
    types_by_pr: Counter[str] = Counter()
    link_counts: Counter[str] = Counter()
    task_parents: Counter[str] = Counter()
    fields_by_type: dict[str, Counter[str]] = defaultdict(Counter)
    items_by_type: Counter[str] = Counter()
    cap_prs_over = 0
    cap_prs_losing_assessable = 0
    cap_assessable_lost = 0
    per_repo: dict[str, Counter[str]] = defaultdict(Counter)
    per_repo_types: dict[str, Counter[str]] = defaultdict(Counter)

    seen_items: set[int] = set()

    for record in records:
        linked = set(record.work_item_ids)
        items = [work_items.get(wid) for wid in record.work_item_ids]
        item_fates = [classify_item(item, linked, work_items, policy) for item in items]

        outcome = pr_outcome(item_fates)
        outcomes[outcome] += 1
        per_repo[record.repository]["pull_requests"] += 1
        per_repo[record.repository][outcome] += 1
        fates.update(item_fates)
        link_counts[band(len(record.work_item_ids))] += 1

        for present_type in {i.type for i in items if i is not None}:
            types_by_pr[present_type] += 1
            per_repo_types[record.repository][present_type] += 1

        for item, fate in zip(items, item_fates):
            if item is None:
                continue
            types_by_item[item.type] += 1
            if item.id not in seen_items:
                seen_items.add(item.id)
                items_by_type[item.type] += 1
                for name in item.populated:
                    fields_by_type[item.type][name] += 1
            if item.type in policy.tasks:
                task_parents[task_parent_bucket(item, linked, work_items, policy)] += 1

        if len(record.work_item_ids) > policy.max_linked:
            cap_prs_over += 1
            lost = sum(1 for f in item_fates[policy.max_linked:] if f in ("criteria", "task_single"))
            if lost:
                cap_prs_losing_assessable += 1
                cap_assessable_lost += lost

    total = len(records)
    return {
        "config": config,
        "pull_requests": total,
        "distinct_work_items": len(seen_items),
        "links_per_pull_request": ordered(link_counts, ["0", "1", "2", "3-5", "6-10", "11+"]),
        "outcome_per_pull_request": with_share(outcomes, total),
        "linked_item_fates": with_share(fates, sum(fates.values())),
        "work_item_types_by_link": with_share(types_by_item, sum(types_by_item.values())),
        "work_item_types_by_pull_request": with_share(types_by_pr, total),
        "task_parents": with_share(task_parents, sum(task_parents.values())),
        "populated_fields_by_type": {
            t: {name: {"count": n, "percent": pct(n, items_by_type[t])}
                for name, n in sorted(counter.items(), key=lambda kv: -kv[1])}
            for t, counter in sorted(fields_by_type.items())
        },
        "distinct_items_by_type": dict(items_by_type),
        "cap": {
            "max_linked": policy.max_linked,
            "pull_requests_over_cap": cap_prs_over,
            "pull_requests_losing_assessable": cap_prs_losing_assessable,
            "assessable_items_lost": cap_assessable_lost,
        },
        "per_repository": {
            name: {
                "outcomes": dict(counter),
                "types": dict(per_repo_types[name]),  # PRs linking at least one of the type
            }
            for name, counter in sorted(per_repo.items())
        },
    }


def band(count: int) -> str:
    if count <= 2:
        return str(count)
    if count <= 5:
        return "3-5"
    if count <= 10:
        return "6-10"
    return "11+"


def ordered(counter: Counter[str], keys: Sequence[str]) -> dict[str, int]:
    return {key: counter.get(key, 0) for key in keys}


def pct(part: int, whole: int) -> float:
    return round(100.0 * part / whole, 1) if whole else 0.0


def with_share(counter: Counter[str], whole: int) -> dict[str, dict[str, Any]]:
    return {
        key: {"count": n, "percent": pct(n, whole)}
        for key, n in sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))
    }


# --------------------------------------------------------------------------- #
# Rendering
# --------------------------------------------------------------------------- #


def render(report: dict[str, Any]) -> str:
    lines: list[str] = []
    cfg = report["config"]
    total = report["pull_requests"]

    lines.append("PULL REQUEST WORK ITEM LINKS")
    lines.append(f"  window: last {cfg['days']} days, statuses {', '.join(cfg['statuses'])}")
    lines.append(f"  pull requests: {total:,}   distinct work items: {report['distinct_work_items']:,}")
    lines.append(f"  accepted types: {', '.join(cfg['accepted_types'])}")
    lines.append(f"  task types: {', '.join(cfg['task_types'])}")
    for t, f in cfg["ac_field_by_type"].items():
        lines.append(f"  criteria field for {t}: {f}")
    lines.append(f"  criteria field otherwise: {cfg['default_ac_field']}")
    lines.append("")

    lines.append("LINKS PER PULL REQUEST")
    for key, n in report["links_per_pull_request"].items():
        lines.append(f"  {key:>5}  {n:6,}  {pct(n, total):5.1f}%")
    lines.append("")

    lines.append("WHAT THE AC CHECK WOULD HAVE TO WORK WITH (per pull request)")
    lines.extend(table(report["outcome_per_pull_request"]))
    lines.append("  'task only' is the case that decides whether single-criterion mode is worth building.")
    lines.append("")

    lines.append("WORK ITEM TYPES (share of pull requests linking at least one)")
    lines.extend(table(report["work_item_types_by_pull_request"]))
    lines.append("")

    lines.append("FATE OF EACH LINK UNDER THE PROPOSED RULES")
    lines.extend(table(report["linked_item_fates"]))
    lines.append("")

    if report["task_parents"]:
        lines.append("TASK PARENTS")
        lines.extend(table(report["task_parents"]))
        lines.append("")

    lines.append("POPULATED FIELDS BY TYPE (share of distinct items carrying text)")
    for t, fields in report["populated_fields_by_type"].items():
        lines.append(f"  {t} ({report['distinct_items_by_type'].get(t, 0):,} items)")
        for name, entry in fields.items():
            lines.append(f"    {entry['percent']:5.1f}%  {name}")
    lines.append("")

    cap = report["cap"]
    lines.append(f"TODAY'S CAP OF {cap['max_linked']} (applied in service order, before types are known)")
    lines.append(f"  pull requests over the cap:            {cap['pull_requests_over_cap']:,}")
    lines.append(f"  ...that lose an assessable item to it: {cap['pull_requests_losing_assessable']:,}")
    lines.append(f"  assessable items lost in total:        {cap['assessable_items_lost']:,}")
    lines.append("")

    lines.append("BY REPOSITORY (pull requests with at least one link; % of the repository's pull requests)")
    lines.append("  Which repositories link Tasks or ignored types, so a habit confined to a few teams")
    lines.append("  is not mistaken for an estate-wide one.")
    lines.extend(repository_rows(report["per_repository"]))
    lines.append("")

    return "\n".join(lines)


def repository_rows(per_repo: dict[str, dict[str, Any]]) -> list[str]:
    rows = []
    for name, entry in per_repo.items():
        outcomes = entry["outcomes"]
        total = outcomes.get("pull_requests", 0)
        linked = total - outcomes.get("nothing linked", 0)
        if not linked:
            continue
        rows.append((linked, name, total, outcomes, entry["types"]))
    if not rows:
        return ["  (no repository links a work item)"]

    rows.sort(key=lambda r: (-r[0], r[1]))
    width = max(len(r[1]) for r in rows)
    out = [f"  {'repository':<{width}}  {'linked':>6}  {'task only':>9}  {'ignored only':>12}  types linked"]
    for linked, name, total, outcomes, types in rows:
        task_only = outcomes.get("task only", 0)
        ignored = outcomes.get("only ignored types", 0)
        type_list = ", ".join(
            f"{t} {n}" for t, n in sorted(types.items(), key=lambda kv: (-kv[1], kv[0])))
        out.append(
            f"  {name:<{width}}  {linked:6,}  {pct(task_only, total):8.1f}%  "
            f"{pct(ignored, total):11.1f}%  {type_list}")
    return out


def table(entries: dict[str, dict[str, Any]]) -> list[str]:
    if not entries:
        return ["  (none)"]
    width = max(len(k) for k in entries)
    return [
        f"  {key:<{width}}  {entry['count']:6,}  {entry['percent']:5.1f}%"
        for key, entry in entries.items()
    ]


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure what pull requests link to in an Azure DevOps organisation.")
    parser.add_argument("--org", help="https://dev.azure.com/<org>")
    parser.add_argument("--project", action="append", default=[], help="Repeatable.")
    parser.add_argument("--all-projects", action="store_true")
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--status", action="append", default=[],
                        help="Repeatable. Default: completed.")
    parser.add_argument("--exclude-repo", action="append", default=[])
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=8,
                        help="Parallel per-PR fetches. Lower it if you get throttled.")
    parser.add_argument("--cache", metavar="PATH",
                        help="Read raw records from PATH if it exists, write them there after a fetch.")
    parser.add_argument("--refresh", action="store_true",
                        help="Ignore an existing cache and re-fetch.")
    parser.add_argument("--accepted-type", action="append", default=[],
                        help=f"Repeatable. Default: {', '.join(ACCEPTED_TYPES)}.")
    parser.add_argument("--task-type", action="append", default=[],
                        help=f"Repeatable. Default: {', '.join(TASK_TYPES)}.")
    parser.add_argument("--ac-field", action="append", default=[], metavar="TYPE=FIELD",
                        help="Criteria field for a type. Repeatable. Default: "
                             + ", ".join(f"{t}={f}" for t, f in AC_FIELD_BY_TYPE.items()))
    parser.add_argument("--default-ac-field", default=DEFAULT_AC_FIELD)
    parser.add_argument("--max-linked", type=int, default=MAX_LINKED_WORK_ITEMS,
                        help="The reviewer's linked-item cap to score. Default 5.")
    parser.add_argument("--anonymise-repos", action="store_true",
                        help="Replace repository names with repo-1...repo-N in the output.")
    parser.add_argument("--json", metavar="PATH", help="Write the full report as JSON.")
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args(argv)


def parse_field_map(values: Sequence[str]) -> dict[str, str]:
    out = dict(AC_FIELD_BY_TYPE)
    for value in values:
        if "=" not in value:
            raise AdoError(f"--ac-field expects TYPE=FIELD, got {value!r}")
        work_item_type, _, field_name = value.partition("=")
        out[work_item_type.strip()] = field_name.strip()
    return out


def anonymise(report: dict[str, Any]) -> None:
    names = sorted(report["per_repository"])
    mapping = {name: f"repo-{i + 1}" for i, name in enumerate(names)}
    report["per_repository"] = {mapping[k]: v for k, v in report["per_repository"].items()}


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    statuses = args.status or ["completed"]

    try:
        policy = Policy(
            accepted=set(args.accepted_type or ACCEPTED_TYPES),
            tasks=set(args.task_type or TASK_TYPES),
            ac_field_by_type=parse_field_map(args.ac_field),
            default_ac_field=args.default_ac_field,
            max_linked=args.max_linked,
        )
    except AdoError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    records: list[PrRecord]
    work_items: dict[int, WorkItem]
    meta: dict[str, Any]

    if args.cache and os.path.exists(args.cache) and not args.refresh:
        try:
            records, work_items, meta = load_cache(args.cache)
        except AdoError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        if not args.quiet:
            print(f"  read {len(records)} pull requests from {args.cache}", file=sys.stderr)
    else:
        pat = os.environ.get("AZDO_PAT")
        if not pat:
            print("AZDO_PAT is not set. Use a PAT with Code (read) and Work Items (read).",
                  file=sys.stderr)
            return 2
        if not args.org:
            print("Give --org when fetching.", file=sys.stderr)
            return 2
        client = AdoClient(args.org, pat)
        try:
            projects = list_projects(client) if args.all_projects else args.project
        except AdoError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        if not projects:
            print("Give --project NAME or --all-projects.", file=sys.stderr)
            return 2

        excluded = {name.lower() for name in args.exclude_repo}
        try:
            records, work_items = collect(
                client, projects, excluded, since, statuses,
                args.page_size, args.concurrency, args.quiet)
        except AdoError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        meta = {"days": args.days, "statuses": statuses, "projects": projects,
                "collected_at": datetime.now(timezone.utc).isoformat()}
        if args.cache:
            save_cache(args.cache, records, work_items, meta)

    config = {
        "days": meta.get("days", args.days),
        "statuses": meta.get("statuses", statuses),
        "accepted_types": sorted(policy.accepted),
        "task_types": sorted(policy.tasks),
        "ac_field_by_type": policy.ac_field_by_type,
        "default_ac_field": policy.default_ac_field,
        "max_linked": policy.max_linked,
    }
    report = build_report(records, work_items, policy, config)
    if args.anonymise_repos:
        anonymise(report)

    print(render(report))
    if args.json:
        with open(args.json, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)
        if not args.quiet:
            print(f"  wrote {args.json}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
