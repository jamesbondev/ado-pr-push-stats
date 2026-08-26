#!/usr/bin/env python3
"""Aggregate Azure DevOps pull request push behaviour over a recent window.

Answers, across every repository in one or more projects:

  * how many pull requests start in draft versus published, and when they publish
  * how many *pushes* each one receives (source-ref updates, not commits)
  * how those pushes split either side of publication
  * how far apart consecutive pushes are, bucketed around the review debounce
  * how many reviews the debounce would actually let through, and what a hard
    per-pull-request cap would save on top of it

Only aggregates are printed. No per-pull-request rows, titles, branch names or
author identities are emitted in either the text report or the JSON dump, so the
output is safe to paste into a chat or a planning document.

Authentication is a personal access token in AZDO_PAT (scope: Code (read)).

Usage:
    export AZDO_PAT=...
    ./pr_push_stats.py --org https://dev.azure.com/contoso --project MyProject
    ./pr_push_stats.py --org contoso --all-projects --days 30 --json stats.json

See README.md for the metric definitions and the caveats worth knowing.
"""

from __future__ import annotations

import argparse
import base64
import json
import math
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Sequence

API_VERSION = "7.1"
PR_PAGE_SIZE = 100

# Argus' shipped debounce defaults (its Processor appsettings.json → ReviewDebounce).
# Overridable so a "what if we changed it to N" pass is one flag away.
DEFAULT_QUIET_SECONDS = 150
DEFAULT_JITTER_SECONDS = 15
DEFAULT_MAX_STALENESS_MINUTES = 20

# Caps modelled in the what-if table. Purely a reporting choice.
CAP_CANDIDATES = (3, 5, 8, 10, 15, 20)

FRACTIONAL_SECONDS = re.compile(r"\.(\d{1,9})")


# --------------------------------------------------------------------------- #
# Azure DevOps client
# --------------------------------------------------------------------------- #


class AdoError(RuntimeError):
    """A request failed in a way retrying will not fix."""


class AdoClient:
    """Minimal REST client: GET + JSON, with retry on throttling and 5xx."""

    def __init__(self, org_url: str, pat: str, timeout: int = 60, max_retries: int = 5) -> None:
        self.org_url = org_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        token = base64.b64encode(f":{pat}".encode()).decode()
        self._headers = {
            "Authorization": f"Basic {token}",
            "Accept": "application/json",
            "User-Agent": "argus-pr-push-stats/1.0",
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
                    content_type = response.headers.get("Content-Type", "")
                    body = response.read()
                    # A bad or expired PAT does not 401 here — Azure DevOps answers 203
                    # with an HTML sign-in page, which json.loads reports as a syntax
                    # error several hundred characters in. Name the real cause instead.
                    if "text/html" in content_type:
                        raise AdoError(
                            "Azure DevOps returned an HTML sign-in page. The PAT in AZDO_PAT is "
                            "missing, expired, or lacks Code (read) scope on this organisation."
                        )
                    return json.loads(body)
            except urllib.error.HTTPError as exc:
                if exc.code in (401, 403):
                    raise AdoError(
                        f"HTTP {exc.code} on {url}. The PAT is rejected or lacks Code (read) "
                        f"scope on this organisation/project."
                    ) from exc
                if exc.code == 404:
                    raise AdoError(f"HTTP 404 on {url}. Check the org/project name.") from exc
                if exc.code == 429 or exc.code >= 500:
                    last_error = exc
                    delay = self._retry_delay(exc, attempt)
                    time.sleep(delay)
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


# --------------------------------------------------------------------------- #
# Parsing helpers
# --------------------------------------------------------------------------- #


def parse_time(value: str | None) -> datetime | None:
    """Parse an Azure DevOps timestamp, tolerating 7-digit fractional seconds."""
    if not value:
        return None
    text = value.replace("Z", "+00:00")
    # datetime.fromisoformat accepts at most 6 fractional digits; ADO emits 7.
    text = FRACTIONAL_SECONDS.sub(lambda m: "." + m.group(1)[:6], text)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def property_value(raw: Any) -> str:
    """Unwrap Azure DevOps' {"$type": ..., "$value": ...} property envelope."""
    if isinstance(raw, dict):
        return str(raw.get("$value", ""))
    return str(raw)


# --------------------------------------------------------------------------- #
# Per-pull-request extraction
# --------------------------------------------------------------------------- #


@dataclass
class DraftHistory:
    """What the thread timeline says about a pull request's draft lifecycle."""

    started_draft: bool | None = None
    published_at: datetime | None = None
    toggles: int = 0
    property_keys: list[str] = field(default_factory=list)
    undetermined_events: int = 0


@dataclass
class PrRecord:
    """Everything the aggregates need from one pull request. Never emitted as-is."""

    repository: str
    status: str
    created_at: datetime
    closed_at: datetime | None
    push_times: list[datetime]
    reasons: list[str]
    draft: DraftHistory


def read_draft_history(threads: Sequence[dict[str, Any]], is_draft_now: bool) -> DraftHistory:
    """Reconstruct the draft/published timeline from a pull request's threads.

    Azure DevOps has no "publishedAt" field. It records draft transitions as system
    threads, so the timeline has to be read back out of them. The thread shape is not
    contractually stable, so this looks for any draft-ish property key *and* falls back
    to the system comment text, and reports what it saw under Data quality so a schema
    change shows up as a coverage number rather than as silently wrong percentages.
    """
    history = DraftHistory()
    events: list[tuple[datetime, str | None]] = []
    seen_keys: set[str] = set()

    for thread in threads:
        properties = thread.get("properties") or {}
        draft_keys = [key for key in properties if "draft" in key.lower()]
        comments = thread.get("comments") or []
        text = " ".join(
            str(comment.get("content") or "")
            for comment in comments
            if comment.get("commentType") == "system"
        ).lower()

        if not draft_keys and "draft" not in text and "publish" not in text:
            continue

        seen_keys.update(draft_keys)
        timestamp = parse_time(thread.get("publishedDate")) or parse_time(
            thread.get("lastUpdatedDate")
        )
        if timestamp is None:
            continue

        direction: str | None = None
        if "publish" in text or "ready for review" in text:
            direction = "published"
        elif "draft" in text:
            direction = "draft"
        else:
            for key in draft_keys:
                value = property_value(properties[key]).strip().lower()
                if value in ("0", "false"):
                    direction = "published"
                    break
                if value in ("1", "true"):
                    direction = "draft"
                    break

        events.append((timestamp, direction))

    history.property_keys = sorted(seen_keys)
    history.toggles = len(events)
    history.undetermined_events = sum(1 for _, direction in events if direction is None)

    if not events:
        # No transition ever recorded, so the pull request has been in its current
        # state throughout. For a completed or abandoned PR that is almost always
        # "published", but an abandoned draft is possible, hence reading it back.
        history.started_draft = is_draft_now
        history.published_at = None
        return history

    events.sort(key=lambda item: item[0])
    first_time, first_direction = events[0]
    if first_direction == "published":
        history.started_draft = True
        history.published_at = first_time
    elif first_direction == "draft":
        history.started_draft = False
        history.published_at = None
    else:
        history.started_draft = None
        history.published_at = None

    return history


def fetch_pr_record(
    client: AdoClient,
    project: str,
    repository_id: str,
    repository_name: str,
    pull_request: dict[str, Any],
    read_drafts: bool,
) -> PrRecord | None:
    pr_id = pull_request["pullRequestId"]
    base = f"{urllib.parse.quote(project)}/_apis/git/repositories/{repository_id}/pullRequests/{pr_id}"

    iterations = client.get(f"{base}/iterations").get("value", [])
    push_times: list[datetime] = []
    reasons: list[str] = []
    for iteration in iterations:
        created = parse_time(iteration.get("createdDate"))
        if created is None:
            continue
        push_times.append(created)
        reasons.append(str(iteration.get("reason") or "unknown"))

    if not push_times:
        return None

    order = sorted(range(len(push_times)), key=lambda index: push_times[index])
    push_times = [push_times[index] for index in order]
    reasons = [reasons[index] for index in order]

    if read_drafts:
        threads = client.get(f"{base}/threads").get("value", [])
        draft = read_draft_history(threads, bool(pull_request.get("isDraft")))
    else:
        draft = DraftHistory(started_draft=bool(pull_request.get("isDraft")))

    return PrRecord(
        repository=repository_name,
        status=str(pull_request.get("status") or "unknown"),
        created_at=parse_time(pull_request.get("creationDate")) or push_times[0],
        closed_at=parse_time(pull_request.get("closedDate")),
        push_times=push_times,
        reasons=reasons,
        draft=draft,
    )


# --------------------------------------------------------------------------- #
# Collection
# --------------------------------------------------------------------------- #


def list_projects(client: AdoClient) -> list[str]:
    projects: list[str] = []
    token: str | None = None
    while True:
        params: dict[str, Any] = {"$top": 200}
        if token:
            params["continuationToken"] = token
        payload = client.get("_apis/projects", params)
        projects.extend(item["name"] for item in payload.get("value", []))
        token = payload.get("continuationToken")
        if not token:
            break
    return projects


def list_repositories(client: AdoClient, project: str) -> list[dict[str, Any]]:
    payload = client.get(f"{urllib.parse.quote(project)}/_apis/git/repositories")
    return [
        repo
        for repo in payload.get("value", [])
        if not repo.get("isDisabled") and not repo.get("isInMaintenance")
    ]


def list_pull_requests(
    client: AdoClient,
    project: str,
    repository_id: str,
    since: datetime,
    statuses: set[str],
) -> list[dict[str, Any]]:
    """Every pull request in the repo created since `since`, filtered to `statuses`.

    Queried with status=all rather than once per status: the time-range filter and the
    status filter are separate query parameters and asking for both narrows server-side
    inconsistently across API versions, so the status cut is done here where it is
    verifiable.
    """
    results: list[dict[str, Any]] = []
    skip = 0
    while True:
        payload = client.get(
            f"{urllib.parse.quote(project)}/_apis/git/repositories/{repository_id}/pullrequests",
            {
                "searchCriteria.status": "all",
                "searchCriteria.queryTimeRangeType": "created",
                "searchCriteria.minTime": since.isoformat(),
                "$top": PR_PAGE_SIZE,
                "$skip": skip,
            },
        )
        page = payload.get("value", [])
        for pull_request in page:
            created = parse_time(pull_request.get("creationDate"))
            if created is None or created < since:
                continue
            if str(pull_request.get("status") or "").lower() in statuses:
                results.append(pull_request)
        if len(page) < PR_PAGE_SIZE:
            break
        skip += PR_PAGE_SIZE
    return results


# --------------------------------------------------------------------------- #
# Debounce simulation
# --------------------------------------------------------------------------- #


def simulate_debounce(
    triggers: Sequence[datetime], quiet: timedelta, max_staleness: timedelta
) -> int:
    """Count how many reviews Argus' push debounce would actually execute.

    Mirrors ReviewDebounceScheduler + ReviewDebounceCoordinator: every push registers
    itself as the pull request's latest and schedules its own message for `quiet` later;
    on firing, a message that is no longer the latest coalesces away, unless the burst
    has been running longer than `max_staleness`, in which case it is forced through
    against head. The burst clock restarts whenever a review executes with nothing newer
    already registered behind it.
    """
    if not triggers:
        return 0

    events: list[tuple[datetime, int, int]] = []
    for index, moment in enumerate(triggers):
        events.append((moment, 0, index))  # registration
        events.append((moment + quiet, 1, index))  # message fires
    events.sort(key=lambda event: (event[0], event[1]))

    latest_registered_at: datetime | None = None
    latest_index: int | None = None
    burst_started_at: datetime | None = None
    last_executed_at: datetime | None = None
    last_executed_message_created: datetime | None = None
    reviews = 0

    for moment, kind, index in events:
        if kind == 0:
            if latest_registered_at is None or (
                last_executed_at is not None and last_executed_at >= latest_registered_at
            ):
                burst_started_at = moment
            latest_registered_at = moment
            latest_index = index
            continue

        created = triggers[index]
        if latest_index != index:
            if burst_started_at is not None and (moment - burst_started_at) > max_staleness:
                reviews += 1
                last_executed_at = moment
                last_executed_message_created = created
            continue

        if last_executed_message_created is not None and last_executed_message_created > created:
            continue  # a later message already ran and covered this push

        reviews += 1
        last_executed_at = moment
        last_executed_message_created = created

    return reviews


# --------------------------------------------------------------------------- #
# Aggregation
# --------------------------------------------------------------------------- #


def percentile(values: Sequence[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(ordered[int(position)])
    return float(ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower))


def summarise(values: Sequence[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0}
    return {
        "count": len(values),
        "mean": round(sum(values) / len(values), 2),
        "p50": round(percentile(values, 0.50) or 0, 2),
        "p75": round(percentile(values, 0.75) or 0, 2),
        "p90": round(percentile(values, 0.90) or 0, 2),
        "p95": round(percentile(values, 0.95) or 0, 2),
        "p99": round(percentile(values, 0.99) or 0, 2),
        "max": round(max(values), 2),
    }


def histogram(values: Iterable[int]) -> dict[str, int]:
    buckets = {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0, "6-10": 0, "11-20": 0, "21-50": 0, "51+": 0}
    for value in values:
        if value <= 5:
            buckets[str(max(value, 1))] += 1
        elif value <= 10:
            buckets["6-10"] += 1
        elif value <= 20:
            buckets["11-20"] += 1
        elif value <= 50:
            buckets["21-50"] += 1
        else:
            buckets["51+"] += 1
    return buckets


def gap_buckets(gaps_seconds: Iterable[float], quiet_seconds: int) -> dict[str, int]:
    """Bucket inter-push gaps around the debounce, since that is the decision boundary."""
    edges = [
        (f"<= {quiet_seconds}s (debounce absorbs)", quiet_seconds),
        (f"{quiet_seconds}s - 5m", 300),
        ("5m - 20m", 1200),
        ("20m - 1h", 3600),
        ("1h - 4h", 14400),
        ("4h - 24h", 86400),
        ("> 24h", float("inf")),
    ]
    buckets = {label: 0 for label, _ in edges}
    for gap in gaps_seconds:
        for label, ceiling in edges:
            if gap <= ceiling:
                buckets[label] += 1
                break
    return buckets


def build_report(records: Sequence[PrRecord], config: dict[str, Any]) -> dict[str, Any]:
    quiet_seconds = config["quiet_seconds"] + config["jitter_seconds"] / 2
    quiet = timedelta(seconds=quiet_seconds)
    max_staleness = timedelta(minutes=config["max_staleness_minutes"])

    push_counts: list[int] = []
    reason_totals: Counter[str] = Counter()
    status_totals: Counter[str] = Counter()
    gaps: list[float] = []
    lifetimes_hours: list[float] = []

    started_draft = 0
    started_published = 0
    draft_undetermined = 0
    multi_toggle = 0
    time_to_publish_hours: list[float] = []
    pushes_while_draft: list[int] = []
    pushes_after_publish: list[int] = []

    eligible_triggers: list[int] = []
    simulated_reviews: list[int] = []
    simulated_reviews_if_drafts_reviewed: list[int] = []
    repo_rollup: dict[str, dict[str, int]] = {}
    draft_property_keys: Counter[str] = Counter()
    prs_with_draft_events = 0

    for record in records:
        status_totals[record.status] += 1
        push_counts.append(len(record.push_times))
        reason_totals.update(record.reasons)
        draft_property_keys.update(record.draft.property_keys)

        for earlier, later in zip(record.push_times, record.push_times[1:]):
            gaps.append((later - earlier).total_seconds())

        if record.closed_at:
            lifetimes_hours.append((record.closed_at - record.created_at).total_seconds() / 3600)

        history = record.draft
        if history.toggles:
            prs_with_draft_events += 1
        if history.toggles > 1:
            multi_toggle += 1

        if history.started_draft is None:
            draft_undetermined += 1
            triggers = list(record.push_times)
        elif history.started_draft:
            started_draft += 1
            publish_at = history.published_at
            if publish_at is None:
                # Marked draft at some point but never observed publishing; treat every
                # push as review-eligible rather than dropping the pull request.
                triggers = list(record.push_times)
            else:
                time_to_publish_hours.append(
                    (publish_at - record.created_at).total_seconds() / 3600
                )
                before = [t for t in record.push_times if t < publish_at]
                after = [t for t in record.push_times if t >= publish_at]
                pushes_while_draft.append(len(before))
                pushes_after_publish.append(len(after))
                # Publication itself triggers a review (ADO re-notifies with isDraft
                # cleared), then every subsequent push does.
                triggers = [publish_at] + [t for t in after if t > publish_at]
        else:
            started_published += 1
            pushes_after_publish.append(len(record.push_times))
            triggers = list(record.push_times)

        eligible_triggers.append(len(triggers))
        reviews = simulate_debounce(triggers, quiet, max_staleness)
        simulated_reviews.append(reviews)
        simulated_reviews_if_drafts_reviewed.append(
            simulate_debounce(record.push_times, quiet, max_staleness)
        )

        rollup = repo_rollup.setdefault(record.repository, {"prs": 0, "pushes": 0, "reviews": 0})
        rollup["prs"] += 1
        rollup["pushes"] += len(record.push_times)
        rollup["reviews"] += reviews

    total_triggers = sum(eligible_triggers)
    total_reviews = sum(simulated_reviews)

    caps = {}
    for cap in CAP_CANDIDATES:
        over = [count for count in simulated_reviews if count > cap]
        caps[str(cap)] = {
            "prs_over_cap": len(over),
            "pct_prs_over_cap": round(100 * len(over) / len(records), 1) if records else 0.0,
            "reviews_saved": sum(count - cap for count in over),
            "pct_reviews_saved": (
                round(100 * sum(count - cap for count in over) / total_reviews, 1)
                if total_reviews
                else 0.0
            ),
        }

    top_repos = sorted(
        (
            {"repository": name, **values}
            for name, values in repo_rollup.items()
        ),
        key=lambda item: item["reviews"],
        reverse=True,
    )[: config["top_repos"]]
    if config["anonymise_repos"]:
        for index, entry in enumerate(top_repos, start=1):
            entry["repository"] = f"repo-{index}"

    return {
        "config": config,
        "scope": {
            "pull_requests": len(records),
            "repositories_with_prs": len(repo_rollup),
            "status_breakdown": dict(status_totals),
        },
        "draft_lifecycle": {
            "started_published": started_published,
            "started_draft": started_draft,
            "undetermined": draft_undetermined,
            "pct_started_draft": (
                round(100 * started_draft / len(records), 1) if records else 0.0
            ),
            "toggled_draft_more_than_once": multi_toggle,
            "hours_create_to_publish": summarise(time_to_publish_hours),
            "pushes_while_draft": summarise([float(v) for v in pushes_while_draft]),
            "pushes_while_draft_histogram": histogram(pushes_while_draft),
        },
        "pushes": {
            "total": sum(push_counts),
            "per_pr": summarise([float(v) for v in push_counts]),
            "per_pr_histogram": histogram(push_counts),
            "by_reason": dict(reason_totals.most_common()),
            "after_publish_per_pr": summarise([float(v) for v in pushes_after_publish]),
            "after_publish_histogram": histogram(pushes_after_publish),
        },
        "gaps_between_pushes_seconds": {
            "summary": summarise(gaps),
            "buckets": gap_buckets(gaps, config["quiet_seconds"]),
            "total_gaps": len(gaps),
        },
        "debounce_simulation": {
            "review_eligible_triggers": total_triggers,
            "reviews_after_debounce": total_reviews,
            "pct_suppressed_by_debounce": (
                round(100 * (total_triggers - total_reviews) / total_triggers, 1)
                if total_triggers
                else 0.0
            ),
            "reviews_per_pr": summarise([float(v) for v in simulated_reviews]),
            "reviews_per_pr_histogram": histogram(simulated_reviews),
            "reviews_if_drafts_were_reviewed": sum(simulated_reviews_if_drafts_reviewed),
        },
        "hard_cap_what_if": caps,
        "pr_lifetime_hours": summarise(lifetimes_hours),
        "top_repositories_by_reviews": top_repos,
        "data_quality": {
            "draft_property_keys_seen": dict(draft_property_keys.most_common()),
            "prs_with_undetermined_draft_state": draft_undetermined,
            "prs_with_any_draft_transition_event": prs_with_draft_events,
            # Zero events across a whole scan means the thread shape changed and the
            # draft split below is really "everything looked published", not a finding.
            "draft_detection_produced_no_signal": (
                config["draft_detection"] and prs_with_draft_events == 0 and bool(records)
            ),
        },
    }


# --------------------------------------------------------------------------- #
# Rendering
# --------------------------------------------------------------------------- #


def render_summary(lines: list[str], values: dict[str, Any]) -> None:
    if not values.get("count"):
        lines.append("    (no data)")
        return
    lines.append(
        f"    n={values['count']}  mean={values['mean']}  p50={values['p50']}  "
        f"p75={values['p75']}  p90={values['p90']}  p95={values['p95']}  "
        f"p99={values['p99']}  max={values['max']}"
    )


def render(stats: dict[str, Any]) -> str:
    config = stats["config"]
    lines: list[str] = []
    add = lines.append

    add("=" * 78)
    add("AZURE DEVOPS PULL REQUEST PUSH BEHAVIOUR")
    add("=" * 78)
    add(f"Window            : last {config['days']} days (PRs created since {config['since']})")
    add(f"Projects          : {', '.join(config['projects'])}")
    add(f"Statuses included : {', '.join(sorted(config['statuses']))}")
    add(
        f"Debounce modelled : quiet {config['quiet_seconds']}s "
        f"(+{config['jitter_seconds']}s jitter), max staleness "
        f"{config['max_staleness_minutes']}m"
    )
    add("")

    scope = stats["scope"]
    add(f"Pull requests     : {scope['pull_requests']} across {scope['repositories_with_prs']} repositories")
    add(f"  by status       : {scope['status_breakdown']}")
    add("")

    draft = stats["draft_lifecycle"]
    add("-" * 78)
    add("DRAFT LIFECYCLE")
    add("-" * 78)
    add(f"  started published        : {draft['started_published']}")
    add(f"  started as draft         : {draft['started_draft']} ({draft['pct_started_draft']}%)")
    add(f"  undetermined             : {draft['undetermined']}")
    add(f"  toggled draft >1 time    : {draft['toggled_draft_more_than_once']}")
    add("  hours from create to publish (draft-started PRs):")
    render_summary(lines, draft["hours_create_to_publish"])
    add("  pushes made while still in draft:")
    render_summary(lines, draft["pushes_while_draft"])
    add(f"    histogram: {draft['pushes_while_draft_histogram']}")
    add("")

    pushes = stats["pushes"]
    add("-" * 78)
    add("PUSHES (source-ref updates; iteration 1 == PR creation)")
    add("-" * 78)
    add(f"  total pushes             : {pushes['total']}")
    add("  pushes per PR:")
    render_summary(lines, pushes["per_pr"])
    add(f"    histogram: {pushes['per_pr_histogram']}")
    add(f"  by reason                : {pushes['by_reason']}")
    add("  pushes per PR while published (the review-eligible ones):")
    render_summary(lines, pushes["after_publish_per_pr"])
    add(f"    histogram: {pushes['after_publish_histogram']}")
    add("")

    gaps = stats["gaps_between_pushes_seconds"]
    add("-" * 78)
    add("GAP BETWEEN CONSECUTIVE PUSHES (seconds)")
    add("-" * 78)
    render_summary(lines, gaps["summary"])
    add(f"  total gaps observed      : {gaps['total_gaps']}")
    for label, count in gaps["buckets"].items():
        share = 100 * count / gaps["total_gaps"] if gaps["total_gaps"] else 0
        add(f"    {label:<32} {count:>6}  ({share:5.1f}%)")
    add("")

    sim = stats["debounce_simulation"]
    add("-" * 78)
    add("DEBOUNCE SIMULATION (what the current debounce would let through)")
    add("-" * 78)
    add(f"  review-eligible triggers : {sim['review_eligible_triggers']}")
    add(f"  reviews after debounce   : {sim['reviews_after_debounce']}")
    add(f"  suppressed by debounce   : {sim['pct_suppressed_by_debounce']}%")
    add("  reviews per PR after debounce:")
    render_summary(lines, sim["reviews_per_pr"])
    add(f"    histogram: {sim['reviews_per_pr_histogram']}")
    add(
        f"  if drafts were reviewed  : {sim['reviews_if_drafts_were_reviewed']} reviews "
        f"(DraftBehaviour=Review across the board)"
    )
    add("")

    add("-" * 78)
    add("HARD CAP WHAT-IF (on top of the debounce)")
    add("-" * 78)
    add(f"  {'cap':>5}  {'PRs over':>9}  {'% PRs':>7}  {'reviews saved':>14}  {'% reviews':>10}")
    for cap, values in stats["hard_cap_what_if"].items():
        add(
            f"  {cap:>5}  {values['prs_over_cap']:>9}  {values['pct_prs_over_cap']:>6}%  "
            f"{values['reviews_saved']:>14}  {values['pct_reviews_saved']:>9}%"
        )
    add("")

    add("-" * 78)
    add("PR LIFETIME (hours, closed PRs)")
    add("-" * 78)
    render_summary(lines, stats["pr_lifetime_hours"])
    add("")

    add("-" * 78)
    add("TOP REPOSITORIES BY SIMULATED REVIEWS")
    add("-" * 78)
    add(f"  {'repository':<40} {'PRs':>5} {'pushes':>8} {'reviews':>9}")
    for entry in stats["top_repositories_by_reviews"]:
        add(
            f"  {entry['repository'][:40]:<40} {entry['prs']:>5} "
            f"{entry['pushes']:>8} {entry['reviews']:>9}"
        )
    add("")

    quality = stats["data_quality"]
    add("-" * 78)
    add("DATA QUALITY")
    add("-" * 78)
    add(f"  draft property keys seen : {quality['draft_property_keys_seen'] or '(none)'}")
    add(f"  PRs with a draft event   : {quality['prs_with_any_draft_transition_event']}")
    add(f"  PRs with unknown draft   : {quality['prs_with_undetermined_draft_state']}")
    add(f"  PRs skipped (no iterations): {config['prs_skipped_no_iterations']}")
    if quality["draft_detection_produced_no_signal"]:
        add("")
        add("  !! No draft transition was detected on ANY pull request. Either nothing in")
        add("     the window was ever a draft, or Azure DevOps changed the thread shape this")
        add("     reads. Treat the DRAFT LIFECYCLE section as unverified until one PR you")
        add("     know started as a draft shows up in the started-as-draft count.")
    add("")

    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--org",
        required=True,
        help="Organisation URL (https://dev.azure.com/contoso) or bare organisation name.",
    )
    parser.add_argument("--project", action="append", default=[], help="Project name. Repeatable.")
    parser.add_argument("--all-projects", action="store_true", help="Scan every project in the org.")
    parser.add_argument("--days", type=int, default=30, help="Window size in days (default 30).")
    parser.add_argument(
        "--status",
        action="append",
        default=[],
        choices=["active", "completed", "abandoned"],
        help="PR statuses to include. Repeatable. Default: completed and abandoned.",
    )
    parser.add_argument("--exclude-repo", action="append", default=[], help="Repository name to skip. Repeatable.")
    parser.add_argument("--concurrency", type=int, default=8, help="Parallel PR fetches (default 8).")
    parser.add_argument("--quiet-seconds", type=int, default=DEFAULT_QUIET_SECONDS)
    parser.add_argument("--jitter-seconds", type=int, default=DEFAULT_JITTER_SECONDS)
    parser.add_argument("--max-staleness-minutes", type=int, default=DEFAULT_MAX_STALENESS_MINUTES)
    parser.add_argument("--top-repos", type=int, default=10)
    parser.add_argument("--anonymise-repos", action="store_true", help="Replace repo names with repo-N.")
    parser.add_argument(
        "--no-draft-detection",
        action="store_true",
        help="Skip the threads call. Halves the request count, loses the draft timeline.",
    )
    parser.add_argument("--json", dest="json_path", help="Also write the aggregates to this JSON file.")
    return parser.parse_args(argv)


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)

    pat = os.environ.get("AZDO_PAT", "").strip()
    if not pat:
        print("AZDO_PAT is not set. Create a PAT with Code (read) scope and export it.", file=sys.stderr)
        return 2

    if not args.project and not args.all_projects:
        print("Pass --project NAME (repeatable) or --all-projects.", file=sys.stderr)
        return 2

    org_url = args.org if args.org.startswith("http") else f"https://dev.azure.com/{args.org}"
    client = AdoClient(org_url, pat)

    statuses = set(args.status) or {"completed", "abandoned"}
    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    excluded = {name.lower() for name in args.exclude_repo}

    try:
        projects = list_projects(client) if args.all_projects else list(args.project)
    except AdoError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(f"Scanning {len(projects)} project(s) since {since.date()}...", file=sys.stderr)

    jobs: list[tuple[str, str, str, dict[str, Any]]] = []
    for project in projects:
        try:
            repositories = list_repositories(client, project)
        except AdoError as exc:
            print(f"  ! {project}: {exc}", file=sys.stderr)
            continue
        for repository in repositories:
            if repository["name"].lower() in excluded:
                continue
            try:
                pull_requests = list_pull_requests(
                    client, project, repository["id"], since, statuses
                )
            except AdoError as exc:
                print(f"  ! {project}/{repository['name']}: {exc}", file=sys.stderr)
                continue
            for pull_request in pull_requests:
                jobs.append((project, repository["id"], repository["name"], pull_request))
        print(f"  {project}: {len(jobs)} pull requests queued (cumulative)", file=sys.stderr)

    if not jobs:
        print("No pull requests matched the window and status filter.", file=sys.stderr)
        return 1

    print(f"Fetching iterations{'' if args.no_draft_detection else ' and threads'} for {len(jobs)} PRs...", file=sys.stderr)

    records: list[PrRecord] = []
    failures = 0

    def worker(job: tuple[str, str, str, dict[str, Any]]) -> PrRecord | None:
        project, repository_id, repository_name, pull_request = job
        return fetch_pr_record(
            client,
            project,
            repository_id,
            repository_name,
            pull_request,
            read_drafts=not args.no_draft_detection,
        )

    with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as pool:
        for index, result in enumerate(pool.map(worker, jobs), start=1):
            if result is None:
                failures += 1
            else:
                records.append(result)
            if index % 50 == 0:
                print(f"  {index}/{len(jobs)}", file=sys.stderr)

    config = {
        "days": args.days,
        "since": since.isoformat(),
        "projects": projects,
        "statuses": sorted(statuses),
        "quiet_seconds": args.quiet_seconds,
        "jitter_seconds": args.jitter_seconds,
        "max_staleness_minutes": args.max_staleness_minutes,
        "top_repos": args.top_repos,
        "anonymise_repos": args.anonymise_repos,
        "draft_detection": not args.no_draft_detection,
        "prs_skipped_no_iterations": failures,
    }

    stats = build_report(records, config)
    print(render(stats))

    if args.json_path:
        with open(args.json_path, "w", encoding="utf-8") as handle:
            json.dump(stats, handle, indent=2, sort_keys=True)
        print(f"Aggregates written to {args.json_path}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
