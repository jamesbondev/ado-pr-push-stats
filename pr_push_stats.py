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
import heapq
import zlib
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

# Review allowances and cooldowns modelled in the policy tables. Reporting choices only.
CAP_CANDIDATES = (3, 5, 8, 10, 15, 20)
POLICY_LIMITS = (2, 3, 5)
COOLDOWN_HOURS = (1, 2, 4, 8)
RESUME_RATES = (0.0, 0.25, 0.5, 0.75, 1.0)

# A repository needs at least this many pull requests before it appears in the push
# profile. Without a floor the table is topped by repos with one very active PR.
MIN_PRS_FOR_PROFILE = 5

CACHE_FORMAT_VERSION = 2  # v2 adds per-push author attribution

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
    push_authors: list[str]
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
    authors: list[str] = []
    for iteration in iterations:
        created = parse_time(iteration.get("createdDate"))
        if created is None:
            continue
        push_times.append(created)
        reasons.append(str(iteration.get("reason") or "unknown"))
        # Who pushed, not who opened the pull request — a colleague pushing a fix to
        # someone else's branch is a real and fairly common case.
        identity = iteration.get("author") or {}
        authors.append(
            str(identity.get("displayName") or identity.get("uniqueName") or "unknown")
        )

    if not push_times:
        return None

    order = sorted(range(len(push_times)), key=lambda index: push_times[index])
    push_times = [push_times[index] for index in order]
    reasons = [reasons[index] for index in order]
    authors = [authors[index] for index in order]

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
        push_authors=authors,
        draft=draft,
    )


# --------------------------------------------------------------------------- #
# Raw cache
#
# The fetch is thousands of requests and minutes of wall clock; every policy question
# afterwards is arithmetic over the same timelines. So the timelines are written once and
# re-read for every subsequent scoring run.
#
# This file is NOT the shareable artefact. It holds per-pull-request push timestamps,
# which the aggregate report deliberately does not. Keep it local; share the --json.
# --------------------------------------------------------------------------- #


def save_cache(path: str, records: Sequence[PrRecord], meta: dict[str, Any]) -> None:
    payload = {
        "format_version": CACHE_FORMAT_VERSION,
        "meta": meta,
        "pull_requests": [
            {
                "repository": record.repository,
                "status": record.status,
                "created_at": record.created_at.isoformat(),
                "closed_at": record.closed_at.isoformat() if record.closed_at else None,
                "push_times": [moment.isoformat() for moment in record.push_times],
                "reasons": record.reasons,
                "push_authors": record.push_authors,
                "draft": {
                    "started_draft": record.draft.started_draft,
                    "published_at": (
                        record.draft.published_at.isoformat()
                        if record.draft.published_at
                        else None
                    ),
                    "toggles": record.draft.toggles,
                    "property_keys": record.draft.property_keys,
                },
            }
            for record in records
        ],
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, separators=(",", ":"))


def load_cache(path: str) -> tuple[list[PrRecord], dict[str, Any]]:
    with open(path, encoding="utf-8") as handle:
        payload = json.load(handle)

    version = payload.get("format_version")
    if version != CACHE_FORMAT_VERSION:
        raise AdoError(
            f"{path} is cache format v{version}, this build reads v{CACHE_FORMAT_VERSION}. "
            f"Re-run with --refresh to rebuild it."
        )

    records: list[PrRecord] = []
    for item in payload["pull_requests"]:
        draft = item["draft"]
        push_times = [parse_time(value) for value in item["push_times"]]
        push_times = [moment for moment in push_times if moment is not None]
        if not push_times:
            continue
        records.append(
            PrRecord(
                repository=item["repository"],
                status=item["status"],
                created_at=parse_time(item["created_at"]) or push_times[0],
                closed_at=parse_time(item["closed_at"]),
                push_times=push_times,
                reasons=item["reasons"],
                push_authors=item.get("push_authors", []),
                draft=DraftHistory(
                    started_draft=draft["started_draft"],
                    published_at=parse_time(draft["published_at"]),
                    toggles=draft["toggles"],
                    property_keys=draft["property_keys"],
                ),
            )
        )
    return records, payload.get("meta", {})


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


def debounce_review_times(
    triggers: Sequence[datetime],
    quiet: timedelta,
    max_staleness: timedelta,
    rereview_quiet: timedelta | None = None,
    closed_at: datetime | None = None,
) -> list[datetime]:
    """Return the times at which Argus' push debounce would actually execute a review.

    Mirrors ReviewDebounceScheduler + ReviewDebounceCoordinator: every push registers
    itself as the pull request's latest and schedules its own message for `quiet` later;
    on firing, a message that is no longer the latest coalesces away, unless the burst
    has been running longer than `max_staleness`, in which case it is forced through
    against head. The burst clock restarts whenever a review executes with nothing newer
    already registered behind it.

    Returns execution times rather than a bare count so the review-limit policies below
    can gate the same stream without re-deriving it.

    `rereview_quiet` makes the wait asymmetric: the first review of a pull request waits
    `quiet`, every one after it waits `rereview_quiet`. The two questions are not the same
    — a first review competes only on latency, while a re-review competes with the change
    the author is still making — and a single window has to be wrong for one of them. The
    ledger already knows whether a review has run, so this is implementable as written.

    `closed_at` drops reviews that would fire after the pull request closed. Without it the
    simulation reports a review as delivered whenever it was scheduled, which silently
    assumes every pull request stays open long enough to receive it — and the longer the
    quiet period, the less true that becomes. Those are not savings; they are the final
    state going unreviewed.
    """
    if not triggers:
        return []

    executed: list[datetime] = []
    # Fire times can no longer be precomputed: how long a push waits depends on whether a
    # review has executed by the time it registers. A heap keeps the merged ordering.
    events: list[tuple[datetime, int, int]] = [
        (moment, 0, index) for index, moment in enumerate(triggers)
    ]
    heapq.heapify(events)

    latest_registered_at: datetime | None = None
    latest_index: int | None = None
    burst_started_at: datetime | None = None
    last_executed_at: datetime | None = None
    last_executed_message_created: datetime | None = None

    while events:
        moment, kind, index = heapq.heappop(events)
        if kind == 0:
            # The ledger restarts the burst clock on
            #   LastExecutedMessageCreatedAt >= LatestMessageCreatedAt
            # and BOTH are message-creation timestamps, not execution times. Comparing the
            # execution time instead resets the clock far too eagerly, because a review
            # always executes later than the push that scheduled it — which under-reports
            # the forced executions the staleness valve produces during a sustained burst.
            if latest_registered_at is None or (
                last_executed_message_created is not None
                and last_executed_message_created >= latest_registered_at
            ):
                burst_started_at = moment
            latest_registered_at = moment
            latest_index = index
            wait = quiet if (rereview_quiet is None or not executed) else rereview_quiet
            heapq.heappush(events, (moment + wait, 1, index))
            continue

        created = triggers[index]
        if latest_index != index:
            if burst_started_at is not None and (moment - burst_started_at) > max_staleness:
                if closed_at is None or moment <= closed_at:
                    executed.append(moment)
                last_executed_at = moment
                last_executed_message_created = created
            continue

        if last_executed_message_created is not None and last_executed_message_created > created:
            continue  # a later message already ran and covered this push

        if closed_at is None or moment <= closed_at:
            executed.append(moment)
        last_executed_at = moment
        last_executed_message_created = created

    return executed


def simulate_debounce(
    triggers: Sequence[datetime], quiet: timedelta, max_staleness: timedelta
) -> int:
    return len(debounce_review_times(triggers, quiet, max_staleness))


# --------------------------------------------------------------------------- #
# Review-limit policies
#
# Three ways to stop a pull request consuming unbounded reviews, modelled as a gate on
# the stream of reviews the debounce already decided to run. The gate never re-opens the
# debounce's own decisions, so a policy can only ever remove reviews, never add them.
#
# The approximation: a push arriving during a cooldown is dropped here, whereas in
# production it would never have entered the ledger at all. The two differ only for a
# push that straddles a cooldown boundary and would have been coalesced across it —
# rare, and it costs at most one review per boundary.
# --------------------------------------------------------------------------- #


@dataclass
class PolicyOutcome:
    """What one policy did to one pull request's review stream."""

    reviews: list[datetime] = field(default_factory=list)
    gate_trips: int = 0  # each trip is one "not ready for review" comment
    resume_requests: int = 0  # option 1 only: times the author had to ask
    suppressed: int = 0  # reviews the policy removed on this pull request

    def final_state_reviewed(self, last_trigger: datetime | None) -> bool:
        """Did any surviving review cover the pull request's final push?

        The safety question. A policy that suppresses the last review leaves the code
        that actually merges unreviewed, which is a different kind of cost from spend.
        """
        if last_trigger is None:
            return True
        return any(moment >= last_trigger for moment in self.reviews)


def apply_hard_cap(reviews: Sequence[datetime], limit: int) -> PolicyOutcome:
    """Baseline for comparison: after `limit` reviews the pull request is never reviewed again.

    A trip is counted when the limit is *reached*, matching the other two policies — that
    is the moment a real implementation posts its comment, whether or not any further push
    ever arrives to be suppressed.
    """
    outcome = PolicyOutcome(reviews=list(reviews[:limit]))
    if len(reviews) >= limit:
        outcome.gate_trips = 1
    outcome.suppressed = len(reviews) - len(outcome.reviews)
    return outcome


def apply_cooldown(
    reviews: Sequence[datetime], limit: int, cooldown: timedelta
) -> PolicyOutcome:
    """Option 2: after `limit` reviews, comment and ignore pushes for `cooldown`, then reset.

    The allowance is a sliding window anchored on the review that tripped it, not a fixed
    calendar bucket: three reviews at 12:03, 12:50 and 13:01 with a two-hour cooldown go
    quiet until 15:01, and a push at 15:05 starts a fresh allowance of three.
    """
    outcome = PolicyOutcome()
    count = 0
    cooldown_until: datetime | None = None

    for moment in reviews:
        if cooldown_until is not None:
            if moment < cooldown_until:
                continue  # inside the quiet window: push ignored, no review
            cooldown_until = None
            count = 0  # window expired, allowance resets

        outcome.reviews.append(moment)
        count += 1
        if count >= limit:
            outcome.gate_trips += 1
            cooldown_until = moment + cooldown
            count = 0

    outcome.suppressed = len(reviews) - len(outcome.reviews)
    return outcome


def apply_consent_gate(
    reviews: Sequence[datetime], limit: int, resume_rate: float, key: str
) -> PolicyOutcome:
    """Option 1: after `limit` reviews, stop until the author explicitly asks to continue.

    Whether an author asks cannot be read off historical data — nobody was ever asked. So
    it is a parameter, and the two ends of it bracket the policy: at `resume_rate` 0 this
    is exactly a hard cap, and at 1.0 it saves nothing but the round trip. The draw is a
    stable hash of the pull request and the trip number, so a given rate always produces
    the same answer for the same input.
    """
    outcome = PolicyOutcome()
    count = 0
    blocked = False
    will_resume = False

    for moment in reviews:
        if blocked:
            if not will_resume:
                continue
            blocked = False
            will_resume = False
            count = 0

        outcome.reviews.append(moment)
        count += 1
        if count >= limit:
            outcome.gate_trips += 1
            blocked = True
            draw = zlib.crc32(f"{key}:{outcome.gate_trips}".encode()) % 10_000 / 10_000
            will_resume = draw < resume_rate
            if will_resume:
                outcome.resume_requests += 1

    outcome.suppressed = len(reviews) - len(outcome.reviews)
    return outcome


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


def format_duration(seconds: float) -> str:
    """Render a bucket edge without rounding it into a different number.

    150 seconds formatted as "2m" is not a rounding nicety: a reader takes the bucket to
    be two minutes, concludes the 150-second threshold is a subset of it, and reasons
    about the wrong population. Sub-minute precision is kept wherever it changes the value.
    """
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.0f}m" if abs(minutes - round(minutes)) < 0.05 else f"{minutes:.1f}m"
    if seconds < 86400:
        return f"{seconds / 3600:.0f}h"
    return f"{seconds / 86400:.0f}d"


def gap_buckets(gaps_seconds: Iterable[float], quiet_seconds: int) -> dict[str, int]:
    """Bucket inter-push gaps around the debounce, since that is the decision boundary.

    The upper edges are fixed, but the first is the quiet period — so a quiet period above
    an edge swallows that bucket whole. Those are dropped rather than printed as an empty
    row with a label that misdescribes what it holds (a quiet period of 600s left a
    "600s - 5m" row reading zero, and a "5m - 20m" row actually holding 10m-20m).
    """
    ceilings = [300.0, 1200.0, 3600.0, 14400.0, 86400.0]
    edges: list[tuple[str, float]] = [
        (f"<= {format_duration(quiet_seconds)} (debounce absorbs)", float(quiet_seconds))
    ]
    lower = float(quiet_seconds)
    for ceiling in ceilings:
        if ceiling <= quiet_seconds:
            continue  # already inside the absorbed band
        edges.append((f"{format_duration(lower)} - {format_duration(ceiling)}", ceiling))
        lower = ceiling
    edges.append((f"> {format_duration(lower)}", float("inf")))
    buckets = {label: 0 for label, _ in edges}
    for gap in gaps_seconds:
        for label, ceiling in edges:
            if gap <= ceiling:
                buckets[label] += 1
                break
    return buckets


def measured_only(stats: dict[str, Any]) -> dict[str, Any]:
    """Strip every modelled figure, leaving only what was measured from the API.

    The report mixes two kinds of number. Push counts, gaps, lifetimes and draft
    transitions are observations. Review counts and everything derived from them are the
    output of a debounce simulator, which encodes one reading of one implementation — and
    on a repository that may not even be onboarded, they describe a hypothetical.

    Handing the whole report to an outside reviewer imports those assumptions without
    flagging them, and the policy tables additionally pre-commit the reader to the three
    options we happened to think of. This returns the half that is safe to hand over.
    """
    repositories = [
        {
            "repository": entry["repository"],
            "prs": entry["prs"],
            "pushes": entry["pushes"],
            "pushes_per_pr": entry["pushes_per_pr"],
            "excess_pushes": entry["excess_pushes"],
            "median_gap_hours": entry["median_gap_hours"],
            "median_lifetime_hours": entry["median_lifetime_hours"],
            "distinct_authors": entry["distinct_authors"],
            "top_author_share": entry["top_author_share"],
            "busiest_over_median": entry["busiest_over_median"],
        }
        for entry in stats["top_repositories_by_pushes_per_pr"]
    ]
    return {
        "provenance": {
            key: stats["config"][key]
            for key in ("days", "since", "projects", "statuses", "draft_detection")
            if key in stats["config"]
        },
        "scope": stats["scope"],
        "pushes": {
            key: value
            for key, value in stats["pushes"].items()
            if key not in ("after_publish_per_pr", "after_publish_histogram")
        },
        "gaps_between_pushes_seconds": stats["gaps_between_pushes_seconds"],
        "draft_lifecycle": {
            key: value
            for key, value in stats["draft_lifecycle"].items()
            if key != "reviews_if_drafts_were_reviewed"
        },
        "pr_lifetime_hours": stats["pr_lifetime_hours"],
        "monthly_volume": {
            month: {k: v for k, v in values.items() if k not in ("reviews", "reviews_per_pr")}
            for month, values in stats["monthly_volume"].items()
        },
        "repositories": repositories,
        "data_quality": stats["data_quality"],
    }


def render_flat(stats: dict[str, Any]) -> str:
    """The measured aggregates as dense prefixed lines, for transcription.

    JSON is the wrong shape when the only way off the machine is a photograph of a
    terminal: braces and indentation spend most of the pixels on punctuation. This is the
    same content as measured_only(), one record per line, every line under 78 characters,
    with a legend so it can be read back without guessing at column meanings.
    """
    def pack(prefix: str, pairs: dict[str, Any]) -> list[str]:
        """key=value pairs wrapped at 78 chars, repeating the prefix on continuations."""
        out: list[str] = []
        current = prefix
        for key, value in pairs.items():
            token = f" {key}={value}"
            if len(current) + len(token) > 78:
                out.append(current)
                current = prefix
            current += token
        out.append(current)
        return out

    def summary(prefix: str, values: dict[str, Any], nd: int = 1) -> list[str]:
        if not values.get("count"):
            return [f"{prefix} n=0"]
        rounded = {
            ("n" if k == "count" else k): (
                (int(round(v)) if nd == 0 else round(v, nd)) if isinstance(v, float) else v
            )
            for k, v in values.items()
        }
        return pack(prefix, rounded)

    data = measured_only(stats)
    lines: list[str] = []
    add = lines.append

    add("### MEASURED-ONLY EXPORT (flat) ###")
    add("# legend: WIN=window SCOPE=totals PUSH=pushes/PR PHIST=push histogram")
    add("# REASON=iteration reasons GAP=gaps(sec) GAPB=gap buckets DRAFT=draft split")
    add("# DPUB=hours to publish DPUSH=pushes while draft LIFE=PR lifetime(h)")
    add("# MON=month prs pushes pushes/PR   DQ=data quality")
    add("# REPO=name prs push medpush p90push exc gap_h life_h auth top1% peak/med")

    prov, scope = data["provenance"], data["scope"]
    lines.extend(pack("WIN", {"days": prov.get("days"),
                              "since": str(prov.get("since"))[:10],
                              "proj": ",".join(prov.get("projects", []))[:24]}))
    lines.extend(pack("SCOPE", {"prs": scope["pull_requests"],
                                "repos": scope["repositories_with_prs"],
                                **scope["status_breakdown"]}))

    pushes = data["pushes"]
    lines.extend(summary("PUSH", pushes["per_pr"]))
    lines.extend(pack("PHIST", pushes["per_pr_histogram"]))
    lines.extend(pack("REASON", {**pushes["by_reason"], "total": pushes["total"]}))

    gaps = data["gaps_between_pushes_seconds"]
    # Seconds to whole numbers: two decimal places on a 4,647,923-second maximum is
    # precision nobody transcribing this by eye can use, and it costs line width.
    lines.extend(summary("GAP", {**gaps["summary"], "count": gaps["total_gaps"]}, nd=0))
    lines.extend(pack("GAPB", {
        label.split("(")[0].strip().replace(" ", ""): count
        for label, count in gaps["buckets"].items()
    }))

    draft = data["draft_lifecycle"]
    lines.extend(pack("DRAFT", {"pub": draft["started_published"],
                                "draft": draft["started_draft"],
                                "undet": draft["undetermined"],
                                "pct": draft["pct_started_draft"],
                                "mtoggle": draft["toggled_draft_more_than_once"]}))
    lines.extend(summary("DPUB", draft["hours_create_to_publish"]))
    lines.extend(summary("DPUSH", draft["pushes_while_draft"]))
    lines.extend(summary("LIFE", data["pr_lifetime_hours"]))

    for month, values in data["monthly_volume"].items():
        add(f"MON {month} {values['prs']} {values['pushes']} {values['pushes_per_pr']}")

    for entry in data["repositories"]:
        profile = entry["pushes_per_pr"]
        add(
            f"REPO {entry['repository'][:24]} {entry['prs']} {entry['pushes']} "
            f"{profile['median']} {profile['p90']} {entry['excess_pushes']} "
            f"{entry['median_gap_hours']} {entry['median_lifetime_hours']} "
            f"{entry['distinct_authors']} {entry['top_author_share']} "
            f"{entry['busiest_over_median']}"
        )

    quality = data["data_quality"]
    lines.extend(pack("DQ", {"draftevents": quality["prs_with_any_draft_transition_event"],
                             "unknown": quality["prs_with_undetermined_draft_state"]}))
    add("### END ###")
    return "\n".join(lines)


def build_report(records: Sequence[PrRecord], config: dict[str, Any]) -> dict[str, Any]:
    quiet_seconds = config["quiet_seconds"] + config["jitter_seconds"] / 2
    quiet = timedelta(seconds=quiet_seconds)
    max_staleness = timedelta(minutes=config["max_staleness_minutes"])
    rereview_quiet = (
        timedelta(seconds=config["rereview_quiet_seconds"] + config["jitter_seconds"] / 2)
        if config.get("rereview_quiet_seconds")
        else None
    )

    push_counts: list[int] = []
    reason_totals: Counter[str] = Counter()
    status_totals: Counter[str] = Counter()
    gaps: list[float] = []
    lifetimes_hours: list[float] = []
    # Hours from the final push to the pull request closing. A quiet window longer than
    # this leaves the merged state unreviewed, so it is the hard ceiling on any
    # coalescing scheme — and no amount of gap data substitutes for it.
    last_push_to_close_hours: list[float] = []
    # Split by push count: for a single-push pull request the last push is also the first,
    # so a window that outlives it costs the ONLY review, not merely the latest one.
    settle_single: list[float] = []
    settle_multi: list[float] = []

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
    repo_rollup: dict[str, dict[str, Any]] = {}
    draft_property_keys: Counter[str] = Counter()
    prs_with_draft_events = 0
    reviews_lost_to_close = 0
    prs_final_state_unreviewed = 0
    # (review times, last trigger, repo) per PR — the input every policy is scored on.
    streams: list[tuple[list[datetime], datetime | None, str]] = []
    # Volume by calendar month of PR creation. Only says anything over a window long
    # enough to hold more than one, but that is exactly when seasonality is the question:
    # a single month cannot distinguish a habit from the month it was measured in.
    monthly: dict[str, dict[str, int]] = {}

    for index, record in enumerate(records):
        status_totals[record.status] += 1
        push_counts.append(len(record.push_times))
        reason_totals.update(record.reasons)
        draft_property_keys.update(record.draft.property_keys)

        for earlier, later in zip(record.push_times, record.push_times[1:]):
            gaps.append((later - earlier).total_seconds())

        if record.closed_at:
            lifetimes_hours.append((record.closed_at - record.created_at).total_seconds() / 3600)
            settle = (record.closed_at - record.push_times[-1]).total_seconds() / 3600
            if settle >= 0:
                last_push_to_close_hours.append(settle)
                target = settle_single if len(record.push_times) == 1 else settle_multi
                target.append(settle)

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
        review_times = debounce_review_times(
            triggers, quiet, max_staleness, rereview_quiet, record.closed_at
        )
        # What the window cost in coverage, as distinct from what it saved. A review that
        # was scheduled but fired after the pull request closed is not a saving.
        scheduled = len(
            debounce_review_times(triggers, quiet, max_staleness, rereview_quiet)
        )
        reviews_lost_to_close += scheduled - len(review_times)
        final_unreviewed = bool(triggers) and not any(t >= triggers[-1] for t in review_times)
        if final_unreviewed:
            prs_final_state_unreviewed += 1
        reviews = len(review_times)
        simulated_reviews.append(reviews)
        simulated_reviews_if_drafts_reviewed.append(
            simulate_debounce(record.push_times, quiet, max_staleness)
        )
        streams.append((review_times, triggers[-1] if triggers else None, record.repository))

        month = record.created_at.strftime("%Y-%m")
        bucket = monthly.setdefault(month, {"prs": 0, "pushes": 0, "reviews": 0})
        bucket["prs"] += 1
        bucket["pushes"] += len(record.push_times)
        bucket["reviews"] += reviews

        rollup = repo_rollup.setdefault(
            record.repository,
            {
                "prs": 0,
                "pushes": 0,
                "reviews": 0,
                "push_counts": [],
                "gaps": [],
                "lifetimes": [],
                "author_pushes": Counter(),
                "author_prs": {},
                # An org-wide coverage figure is an average over repositories whose pull
                # requests close at wildly different speeds. A repo closing in 3 hours
                # loses far more final reviews than one closing in 5 days, and the team on
                # the wrong end of that experiences a materially worse service than the
                # headline number describes.
                "final_unreviewed": 0,
                "settles": [],
            },
        )
        rollup["prs"] += 1
        rollup["pushes"] += len(record.push_times)
        rollup["reviews"] += reviews
        rollup["push_counts"].append(len(record.push_times))
        rollup["final_unreviewed"] += 1 if final_unreviewed else 0
        if record.closed_at:
            rollup["settles"].append(
                (record.closed_at - record.push_times[-1]).total_seconds() / 3600
            )
        for author in record.push_authors:
            rollup["author_pushes"][author] += 1
            rollup["author_prs"].setdefault(author, set()).add(index)
        # Cadence and longevity, per repo. Pushes per PR on its own cannot tell a team
        # iterating quickly from a team drip-feeding a pull request over days, and those
        # have different owners: the first is how they work, the second is usually
        # something upstream of them (review turnaround, flaky CI) making them wait.
        rollup["gaps"].extend(
            (later - earlier).total_seconds()
            for earlier, later in zip(record.push_times, record.push_times[1:])
        )
        if record.closed_at:
            rollup["lifetimes"].append(
                (record.closed_at - record.created_at).total_seconds() / 3600
            )

    total_triggers = sum(eligible_triggers)
    total_reviews = sum(simulated_reviews)

    def score(outcomes: Sequence[PolicyOutcome]) -> dict[str, Any]:
        """Reduce one policy's per-PR outcomes to the numbers worth comparing."""
        kept = sum(len(outcome.reviews) for outcome in outcomes)
        trips = sum(outcome.gate_trips for outcome in outcomes)
        gated = sum(1 for outcome in outcomes if outcome.gate_trips)
        repeat_gated = sum(1 for outcome in outcomes if outcome.gate_trips > 1)
        unreviewed_final = sum(
            1
            for outcome, (_, last_trigger, _) in zip(outcomes, streams)
            if not outcome.final_state_reviewed(last_trigger)
        )
        return {
            "reviews": kept,
            "reviews_saved": total_reviews - kept,
            "pct_reviews_saved": (
                round(100 * (total_reviews - kept) / total_reviews, 1) if total_reviews else 0.0
            ),
            # Each trip posts a comment. This is the policy's noise cost, and it is the
            # number to watch: a policy that saves reviews by interrupting everyone
            # constantly has moved the cost rather than removed it.
            "gate_trips": trips,
            "prs_gated": gated,
            "pct_prs_gated": round(100 * gated / len(records), 1) if records else 0.0,
            "prs_gated_more_than_once": repeat_gated,
            # Gates that fired on a pull request nothing was ever suppressed on: the
            # comment gets posted, the author is told to tidy up, and not one review is
            # avoided. Pure noise, and the fix is to comment on the first suppressed
            # push rather than on reaching the limit.
            "gates_with_no_effect": sum(
                1 for outcome in outcomes if outcome.gate_trips and not outcome.suppressed
            ),
            # The safety cost: the pull request's final state never got reviewed.
            "prs_final_push_unreviewed": unreviewed_final,
            "pct_final_push_unreviewed": (
                round(100 * unreviewed_final / len(records), 1) if records else 0.0
            ),
            "resume_requests": sum(outcome.resume_requests for outcome in outcomes),
        }

    hard_caps = {
        str(limit): score([apply_hard_cap(times, limit) for times, _, _ in streams])
        for limit in CAP_CANDIDATES
    }

    cooldowns = {
        f"limit={limit},cooldown={hours}h": score(
            [
                apply_cooldown(times, limit, timedelta(hours=hours))
                for times, _, _ in streams
            ]
        )
        for limit in POLICY_LIMITS
        for hours in COOLDOWN_HOURS
    }

    consent_gates = {
        f"limit={limit},resume={rate:.2f}": score(
            [
                apply_consent_gate(times, limit, rate, f"{repo}:{index}")
                for index, (times, _, repo) in enumerate(streams)
            ]
        )
        for limit in POLICY_LIMITS
        for rate in RESUME_RATES
    }

    for values in repo_rollup.values():
        # Concentration, not identity. The question is whether a repo's push volume is
        # one person or the whole team; that is answered by shares and ratios, and needs
        # no names. A push count is a poor measure of an individual — whoever owns the
        # hardest integration work will legitimately push most — so the default output
        # emits none, and --name-authors is opt-in and marked not-for-sharing.
        author_pushes: Counter[str] = values.pop("author_pushes")
        author_prs: dict[str, set[int]] = values.pop("author_prs")
        ranked = author_pushes.most_common()
        total_author_pushes = sum(author_pushes.values())
        values["distinct_authors"] = len(ranked)
        values["top_author_share"] = (
            round(100 * ranked[0][1] / total_author_pushes, 1) if ranked else 0.0
        )
        values["top3_author_share"] = (
            round(100 * sum(count for _, count in ranked[:3]) / total_author_pushes, 1)
            if ranked
            else 0.0
        )
        # Rate, not volume: someone who simply opens more pull requests is not the same
        # as someone who pushes more times to each one. Only the second is a habit.
        rates = sorted(
            author_pushes[name] / len(author_prs[name]) for name in author_pushes
        )
        median_rate = percentile(rates, 0.5) or 0.0
        values["busiest_author_rate"] = round(max(rates), 2) if rates else 0.0
        values["median_author_rate"] = round(median_rate, 2)
        values["busiest_over_median"] = (
            round(max(rates) / median_rate, 2) if rates and median_rate else 0.0
        )
        values["named_top_authors"] = (
            [{"author": name, "pushes": count} for name, count in ranked[:3]]
            if config.get("name_authors")
            else []
        )
        repo_gaps = values.pop("gaps")
        repo_lifetimes = values.pop("lifetimes")
        values["median_gap_hours"] = (
            round((percentile(repo_gaps, 0.5) or 0) / 3600, 1) if repo_gaps else 0.0
        )
        values["median_lifetime_hours"] = (
            round(percentile(repo_lifetimes, 0.5) or 0, 1) if repo_lifetimes else 0.0
        )
        counts = [float(count) for count in values.pop("push_counts")]
        values["pushes_per_pr"] = {
            "min": int(min(counts)),
            "median": round(percentile(counts, 0.5) or 0, 1),
            "mean": round(sum(counts) / len(counts), 2),
            "p90": round(percentile(counts, 0.9) or 0, 1),
            "max": int(max(counts)),
        }
        values["reviews_per_pr"] = round(values["reviews"] / values["prs"], 2)
        values["pct_final_unreviewed"] = round(100 * values["final_unreviewed"] / values["prs"], 1)
        # Median hours from the LAST PUSH to close, not from creation. Pull request
        # lifetime was the obvious explanatory column and it is the wrong one: a repo can
        # sit at a 27-hour median lifetime and still merge five minutes after every final
        # push, which is what actually decides whether a quiet window outlives the PR.
        repo_settles = values.pop("settles")
        values["median_settle_hours"] = (
            round(percentile(repo_settles, 0.5) or 0, 1) if repo_settles else 0.0
        )

    org_push_avg = sum(push_counts) / len(records) if records else 0.0
    eligible = [
        {
            "repository": name,
            **values,
            "excess_pushes": round(values["pushes"] - values["prs"] * org_push_avg),
        }
        for name, values in repo_rollup.items()
        if values["prs"] >= MIN_PRS_FOR_PROFILE
    ]
    push_offenders = sorted(
        eligible,
        key=lambda item: item["pushes"] - item["prs"] * (sum(push_counts) / max(len(records), 1)),
        reverse=True,
    )[: config["top_repos"]]

    # Excess: reviews above what this repo's pull request count would predict at the
    # org-wide average. Ranking by rate alone answers "who has the habit" and is dominated
    # by small repos, where a handful of busy pull requests moves the mean a long way and
    # the absolute load is negligible. Excess answers the different question — "where would
    # a change actually recover something" — and needs no minimum-PR floor, because a small
    # repo can only ever accumulate a small excess. A repo sitting exactly on the org
    # average scores zero; one below it scores negative.
    org_reviews_per_pr = total_reviews / len(records) if records else 0.0
    org_pushes_per_pr = sum(push_counts) / len(records) if records else 0.0

    load_ranked = sorted(
        (
            {
                "repository": name,
                "prs": values["prs"],
                "pushes": values["pushes"],
                "reviews": values["reviews"],
                "reviews_per_pr": values["reviews_per_pr"],
                "excess_reviews": round(values["reviews"] - values["prs"] * org_reviews_per_pr),
                "excess_pushes": round(values["pushes"] - values["prs"] * org_pushes_per_pr),
                "pct_of_all_reviews": (
                    round(100 * values["reviews"] / total_reviews, 1) if total_reviews else 0.0
                ),
            }
            for name, values in repo_rollup.items()
        ),
        key=lambda item: item["excess_reviews"],
        reverse=True,
    )

    total_excess = sum(item["excess_reviews"] for item in load_ranked if item["excess_reviews"] > 0)
    running = 0
    repos_to_half_excess = 0
    for item in load_ranked:
        if item["excess_reviews"] <= 0 or running >= total_excess / 2:
            break
        running += item["excess_reviews"]
        repos_to_half_excess += 1

    load_ranked = load_ranked[: config["top_repos"]]
    cumulative = 0.0
    for item in load_ranked:
        cumulative += item["excess_reviews"] / total_excess * 100 if total_excess else 0
        item["cumulative_pct_of_excess"] = round(cumulative, 1)

    top_repos = sorted(
        (
            {"repository": name, **values}
            for name, values in repo_rollup.items()
        ),
        key=lambda item: item["reviews"],
        reverse=True,
    )[: config["top_repos"]]
    if config["anonymise_repos"]:
        aliases: dict[str, str] = {}
        for entry in [*top_repos, *push_offenders, *load_ranked]:
            alias = aliases.setdefault(entry["repository"], f"repo-{len(aliases) + 1}")
            entry["repository"] = alias

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
            # Coverage cost, not saving: scheduled reviews that fired after the pull
            # request had already closed.
            "reviews_lost_to_close": reviews_lost_to_close,
            "prs_final_state_unreviewed": prs_final_state_unreviewed,
            "pct_final_state_unreviewed": (
                round(100 * prs_final_state_unreviewed / len(records), 1) if records else 0.0
            ),
        },
        "hard_cap_what_if": hard_caps,
        "cooldown_policy": cooldowns,
        "consent_gate_policy": consent_gates,
        "monthly_volume": {
            month: {
                **values,
                "reviews_per_pr": round(values["reviews"] / values["prs"], 2),
                "pushes_per_pr": round(values["pushes"] / values["prs"], 2),
            }
            for month, values in sorted(monthly.items())
        },
        "pr_lifetime_hours": summarise(lifetimes_hours),
        "last_push_to_close_single_push": {
            **summarise(settle_single),
            "pct_closed_within": {
                label: round(100 * sum(1 for v in settle_single if v <= hours) / len(settle_single), 1)
                if settle_single
                else 0.0
                for label, hours in (
                    ("10m", 1 / 6), ("20m", 1 / 3), ("30m", 0.5),
                    ("1h", 1.0), ("2h", 2.0), ("4h", 4.0),
                )
            },
        },
        "last_push_to_close_multi_push": {
            **summarise(settle_multi),
            "pct_closed_within": {
                label: round(100 * sum(1 for v in settle_multi if v <= hours) / len(settle_multi), 1)
                if settle_multi
                else 0.0
                for label, hours in (
                    ("10m", 1 / 6), ("20m", 1 / 3), ("30m", 0.5),
                    ("1h", 1.0), ("2h", 2.0), ("4h", 4.0),
                )
            },
        },
        "last_push_to_close_hours": {
            **summarise(last_push_to_close_hours),
            "pct_closed_within": {
                label: round(
                    100
                    * sum(1 for v in last_push_to_close_hours if v <= hours)
                    / len(last_push_to_close_hours),
                    1,
                )
                if last_push_to_close_hours
                else 0.0
                for label, hours in (
                    ("10m", 1 / 6), ("20m", 1 / 3), ("30m", 0.5),
                    ("1h", 1.0), ("2h", 2.0), ("4h", 4.0),
                )
            },
        },
        "top_repositories_by_reviews": top_repos,
        "top_repositories_by_pushes_per_pr": push_offenders,
        "author_concentration": [
            {
                "repository": entry["repository"],
                "prs": entry["prs"],
                "pushes": entry["pushes"],
                "distinct_authors": entry["distinct_authors"],
                "top_author_share": entry["top_author_share"],
                "top3_author_share": entry["top3_author_share"],
                "busiest_author_rate": entry["busiest_author_rate"],
                "median_author_rate": entry["median_author_rate"],
                "busiest_over_median": entry["busiest_over_median"],
                "named_top_authors": entry["named_top_authors"],
            }
            for entry in push_offenders
        ],
        "coverage_by_repository": sorted(
            (
                {
                    "repository": name,
                    "prs": values["prs"],
                    "median_lifetime_hours": values["median_lifetime_hours"],
                    "median_settle_hours": values["median_settle_hours"],
                    "final_unreviewed": values["final_unreviewed"],
                    "pct_final_unreviewed": values["pct_final_unreviewed"],
                }
                for name, values in repo_rollup.items()
                if values["prs"] >= MIN_PRS_FOR_PROFILE
            ),
            key=lambda item: (-item["pct_final_unreviewed"], -item["prs"]),
        )[: config["top_repos"]],
        "repository_review_load": {
            "org_reviews_per_pr": round(org_reviews_per_pr, 2),
            "org_pushes_per_pr": round(org_pushes_per_pr, 2),
            "total_excess_reviews": total_excess,
            "repos_holding_half_the_excess": repos_to_half_excess,
            "repositories": load_ranked,
        },
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

    def policy_table(title: str, note: str, rows: dict[str, Any], first_column: str) -> None:
        add("-" * 78)
        add(title)
        add("-" * 78)
        add(f"  {note}")
        add("")
        add(
            f"  {first_column:<22} {'reviews':>8} {'saved':>7} {'comments':>9} {'no-op':>6} "
            f"{'PRs hit':>8} {'>1 hit':>7} {'unreviewed final':>17}"
        )
        for label, values in rows.items():
            add(
                f"  {label:<22} {values['reviews']:>8} {values['pct_reviews_saved']:>6}% "
                f"{values['gate_trips']:>9} {values['gates_with_no_effect']:>6} "
                f"{values['pct_prs_gated']:>7}% "
                f"{values['prs_gated_more_than_once']:>7} "
                f"{values['prs_final_push_unreviewed']:>7} ({values['pct_final_push_unreviewed']:>4}%)"
            )
        add("")

    policy_table(
        "POLICY A — HARD CAP (never review again once the limit is hit)",
        "Included as the floor: it is what options 1 and 2 collapse to if nobody ever resumes.",
        stats["hard_cap_what_if"],
        "cap",
    )
    policy_table(
        "POLICY B — COOLDOWN WINDOW (stop, comment, ignore pushes for N hours, reset)",
        "'comments' is how many 'not ready for review' notes would be posted in the window.",
        stats["cooldown_policy"],
        "limit / cooldown",
    )
    policy_table(
        "POLICY C — CONSENT GATE (stop until the author explicitly asks to continue)",
        "resume=0.00 is identical to the hard cap; resume=1.00 costs a round trip and saves nothing.",
        stats["consent_gate_policy"],
        "limit / resume rate",
    )

    months = stats["monthly_volume"]
    if len(months) > 1:
        add("-" * 78)
        add("VOLUME BY MONTH OF PR CREATION")
        add("-" * 78)
        add("  Partial at both ends: the first month is clipped by the window start, and the")
        add("  last by PRs still open at collection. Compare whole months only.")
        add("")
        add(f"  {'month':<10} {'PRs':>7} {'pushes':>8} {'reviews':>9} {'push/PR':>9} {'rev/PR':>8}")
        for month, values in months.items():
            add(
                f"  {month:<10} {values['prs']:>7} {values['pushes']:>8} "
                f"{values['reviews']:>9} {values['pushes_per_pr']:>9} "
                f"{values['reviews_per_pr']:>8}"
            )
        add("")

    add("-" * 78)
    add("PR LIFETIME (hours, closed PRs)")
    add("-" * 78)
    render_summary(lines, stats["pr_lifetime_hours"])
    add("")

    settle = stats["last_push_to_close_hours"]
    add("-" * 78)
    add("LAST PUSH -> CLOSE (hours) — the ceiling on any quiet window")
    add("-" * 78)
    add("  A quiet window longer than this leaves the merged state unreviewed. The")
    add("  percentages are the share of PRs that closed within each window, i.e. the")
    add("  share whose final state a window of that length would MISS.")
    render_summary(lines, settle)
    add("")
    single = stats["last_push_to_close_single_push"]
    multi = stats["last_push_to_close_multi_push"]
    add(f"  {'window':<8}{'all':>8}{'1-push PRs':>13}{'2+-push PRs':>13}")
    for label in settle["pct_closed_within"]:
        add(
            f"  {label:<8}{settle['pct_closed_within'][label]:>7}%"
            f"{single['pct_closed_within'][label]:>12}%"
            f"{multi['pct_closed_within'][label]:>12}%"
        )
    add("")
    add("  The 1-push column is the one that matters: for those PRs the last push is also")
    add("  the first, so a window outliving it costs the ONLY review, not the latest one.")
    add("")

    sim_cov = stats["debounce_simulation"]
    add(f"  at the modelled window: {sim_cov['reviews_lost_to_close']} reviews fired after close; "
        f"{sim_cov['prs_final_state_unreviewed']} PRs "
        f"({sim_cov['pct_final_state_unreviewed']}%) end with an unreviewed final state")
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

    coverage = stats["coverage_by_repository"]
    if coverage and any(entry["final_unreviewed"] for entry in coverage):
        overall = stats["debounce_simulation"]["pct_final_state_unreviewed"]
        add("-" * 78)
        add("COVERAGE BY REPOSITORY (who actually bears the unreviewed-final cost)")
        add("-" * 78)
        add(f"  Org-wide the modelled window leaves {overall}% of pull requests with an")
        add("  unreviewed final state. That is an average over repositories whose pull")
        add("  requests close at very different speeds, and it is not what any one team")
        add("  experiences. Worst-affected first. Read 'settle h' - the median hours from")
        add("  the LAST PUSH to close - not 'life h': a repo can run a 27-hour median")
        add("  lifetime and still merge minutes after every final push.")
        add("")
        add(f"  {'repository':<28} {'PRs':>5} {'life h':>7} {'settle h':>9} {'unrev':>6} {'% PRs':>7}")
        for entry in coverage:
            add(
                f"  {entry['repository'][:28]:<28} {entry['prs']:>5} "
                f"{entry['median_lifetime_hours']:>7} {entry['median_settle_hours']:>9} "
                f"{entry['final_unreviewed']:>6} {entry['pct_final_unreviewed']:>6}%"
            )
        add("")

    concentration = stats["author_concentration"]
    if concentration:
        add("-" * 78)
        add("AUTHOR CONCENTRATION (is it a person, or the team?)")
        add("-" * 78)
        add("  'top1%' is the share of a repo's pushes from its single busiest author.")
        add("  'peak/med' compares that author's pushes-PER-PR against the repo's median")
        add("  author - a RATE, so someone who simply opens more pull requests does not")
        add("  register. Near 1.0 means the pattern is structural and applies to everyone;")
        add("  well above 1.0 means one person works differently from their colleagues.")
        add("")
        add("  A push count is a poor measure of an individual - whoever owns the hardest")
        add("  integration work will legitimately push most. Use this to choose what to ask,")
        add("  never to appraise anyone.")
        add("")
        add(
            f"  {'repository':<26} {'PRs':>4} {'push':>6} {'authors':>8} {'top1%':>6} "
            f"{'top3%':>6} {'peak/med':>9}"
        )
        for entry in concentration:
            add(
                f"  {entry['repository'][:26]:<26} {entry['prs']:>4} {entry['pushes']:>6} "
                f"{entry['distinct_authors']:>8} {entry['top_author_share']:>5}% "
                f"{entry['top3_author_share']:>5}% {entry['busiest_over_median']:>9}"
            )
        if any(entry["named_top_authors"] for entry in concentration):
            add("")
            add("  !! --name-authors: the rows below identify people. NOT for sharing.")
            for entry in concentration:
                if entry["named_top_authors"]:
                    names = ", ".join(
                        f"{a['author']} ({a['pushes']})" for a in entry["named_top_authors"]
                    )
                    add(f"    {entry['repository'][:26]:<26} {names}")
        add("")

    load = stats["repository_review_load"]
    add("-" * 78)
    add("REVIEW LOAD BY REPOSITORY (ranked by excess, not by rate)")
    add("-" * 78)
    add(f"  Excess = actual - (PRs x org average). Averages: {load['org_pushes_per_pr']} pushes/PR,")
    add(f"  {load['org_reviews_per_pr']} reviews/PR. A large repo behaving normally scores near zero;")
    add("  a small repo behaving badly scores small.")
    add("")
    add("  'sim rev' and 'exc rev' are MODELLED - what the debounce would let through, not a")
    add("  count of reviews that happened. 'pushes' and 'exc push' are measured from the API.")
    add("")
    add(
        f"  {load['total_excess_reviews']} reviews sit above what PR counts predict; "
        f"{load['repos_holding_half_the_excess']} repositories hold half of that."
    )
    add("")
    add(
        f"  {'repository':<28} {'PRs':>5} {'pushes':>7} {'sim rev':>8} {'rev/PR':>7} "
        f"{'exc rev':>8} {'exc push':>9} {'cum%':>6}"
    )
    for entry in load["repositories"]:
        add(
            f"  {entry['repository'][:28]:<28} {entry['prs']:>5} {entry['pushes']:>7} "
            f"{entry['reviews']:>8} {entry['reviews_per_pr']:>7} "
            f"{entry['excess_reviews']:>8} {entry['excess_pushes']:>9} "
            f"{entry['cumulative_pct_of_excess']:>5}%"
        )
    add("")

    add("-" * 78)
    add(f"PUSH BEHAVIOUR BY REPOSITORY (measured only, min {MIN_PRS_FOR_PROFILE} PRs)")
    add("-" * 78)
    add("  Ranked by excess pushes over the org average. Every column here is measured from")
    add("  the API - nothing modelled - so it stands up in a conversation about process.")
    add("")
    add("  Read 'med' with 'gap h' and 'life h'. Many pushes at short gaps on a short-lived")
    add("  pull request is fast iteration. The same push count at long gaps on a pull request")
    add("  open for days is drip-feeding - and that is usually caused by something upstream")
    add("  of the team (review turnaround, flaky CI), not by the team.")
    add("")
    add(
        f"  {'repository':<28} {'PRs':>4} {'push':>6} {'med':>4} {'p90':>5} "
        f"{'exc':>6} {'gap h':>7} {'life h':>7}"
    )
    for entry in stats["top_repositories_by_pushes_per_pr"]:
        profile = entry["pushes_per_pr"]
        add(
            f"  {entry['repository'][:28]:<28} {entry['prs']:>4} {entry['pushes']:>6} "
            f"{profile['median']:>4} {profile['p90']:>5} {entry['excess_pushes']:>6} "
            f"{entry['median_gap_hours']:>7} {entry['median_lifetime_hours']:>7}"
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
        help="Organisation URL (https://dev.azure.com/contoso) or bare organisation name. "
        "Required unless a populated --cache is being read.",
    )
    parser.add_argument(
        "--cache",
        help="Raw per-PR timeline cache. Read from it when it exists, written after a fetch. "
        "Lets every later policy question be answered without touching the API. "
        "Contains per-PR push timestamps — keep it local, share the --json instead.",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Ignore an existing --cache and re-fetch from the API, then overwrite it.",
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
    parser.add_argument(
        "--rereview-quiet-seconds",
        type=int,
        help="Quiet period for the SECOND and later reviews of a pull request, making the "
        "wait asymmetric. The first review competes only on latency; a re-review competes "
        "with the change the author is still making. Omit for one window throughout.",
    )
    parser.add_argument("--jitter-seconds", type=int, default=DEFAULT_JITTER_SECONDS)
    parser.add_argument("--max-staleness-minutes", type=int, default=DEFAULT_MAX_STALENESS_MINUTES)
    parser.add_argument("--top-repos", type=int, default=10)
    parser.add_argument("--anonymise-repos", action="store_true", help="Replace repo names with repo-N.")
    parser.add_argument(
        "--name-authors",
        action="store_true",
        help="Also print the three busiest authors per repository BY NAME. Off by default: "
        "the concentration figures answer 'person or team?' without identifying anyone, "
        "and output produced with this flag should not be shared.",
    )
    parser.add_argument(
        "--no-draft-detection",
        action="store_true",
        help="Skip the threads call. Halves the request count, loses the draft timeline.",
    )
    parser.add_argument("--json", dest="json_path", help="Also write the aggregates to this JSON file.")
    parser.add_argument(
        "--flat",
        action="store_true",
        help="Print the measured-only aggregates as dense prefixed lines instead of JSON - "
        "built to be screenshotted and read back when the file cannot leave the machine.",
    )
    parser.add_argument(
        "--measured-only",
        dest="measured_path",
        help="Write a JSON containing ONLY figures measured from the API - no debounce "
        "simulation, no policy tables, no review counts. Use this when handing the data to "
        "anyone who should reach their own conclusions rather than inherit ours.",
    )
    return parser.parse_args(argv)


def write_measured(args: argparse.Namespace, stats: dict[str, Any]) -> None:
    if args.flat:
        print()
        print(render_flat(stats))
    if not args.measured_path:
        return
    with open(args.measured_path, "w", encoding="utf-8") as handle:
        json.dump(measured_only(stats), handle, indent=2, sort_keys=True)
    print(f"Measured-only aggregates written to {args.measured_path}", file=sys.stderr)


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)

    use_cache = bool(args.cache) and os.path.exists(args.cache) and not args.refresh

    if use_cache:
        try:
            records, meta = load_cache(args.cache)
        except (AdoError, KeyError, json.JSONDecodeError) as exc:
            print(f"Could not read {args.cache}: {exc}", file=sys.stderr)
            return 1
        print(
            f"Loaded {len(records)} pull requests from {args.cache} "
            f"(collected {meta.get('collected_at', 'unknown')}, no API calls made).",
            file=sys.stderr,
        )
        # The window and status filter are properties of the fetch, not of the scoring, so
        # they come from the cache rather than from this run's flags. Only the policy
        # parameters below are re-read, which is the whole point of the cache.
        config = {
            **meta,
            "quiet_seconds": args.quiet_seconds,
            "rereview_quiet_seconds": args.rereview_quiet_seconds,
            "jitter_seconds": args.jitter_seconds,
            "max_staleness_minutes": args.max_staleness_minutes,
            "top_repos": args.top_repos,
            "anonymise_repos": args.anonymise_repos,
            "name_authors": args.name_authors,
            "source": f"cache: {args.cache}",
        }
        stats = build_report(records, config)
        print(render(stats))
        if args.json_path:
            with open(args.json_path, "w", encoding="utf-8") as handle:
                json.dump(stats, handle, indent=2, sort_keys=True)
            print(f"Aggregates written to {args.json_path}", file=sys.stderr)
        write_measured(args, stats)
        return 0

    pat = os.environ.get("AZDO_PAT", "").strip()
    if not pat:
        print("AZDO_PAT is not set. Create a PAT with Code (read) scope and export it.", file=sys.stderr)
        return 2

    if not args.org:
        print("Pass --org (no cache to read from).", file=sys.stderr)
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
        "rereview_quiet_seconds": args.rereview_quiet_seconds,
        "jitter_seconds": args.jitter_seconds,
        "max_staleness_minutes": args.max_staleness_minutes,
        "top_repos": args.top_repos,
        "anonymise_repos": args.anonymise_repos,
        "name_authors": args.name_authors,
        "draft_detection": not args.no_draft_detection,
        "prs_skipped_no_iterations": failures,
        "source": "azure devops api",
    }

    if args.cache:
        save_cache(
            args.cache,
            records,
            {
                "collected_at": datetime.now(timezone.utc).isoformat(),
                "days": args.days,
                "since": since.isoformat(),
                "projects": projects,
                "statuses": sorted(statuses),
                "draft_detection": not args.no_draft_detection,
                "prs_skipped_no_iterations": failures,
            },
        )
        print(
            f"Raw timelines cached to {args.cache} — re-score policies with "
            f"--cache {args.cache} and no API calls.",
            file=sys.stderr,
        )

    stats = build_report(records, config)
    print(render(stats))

    if args.json_path:
        with open(args.json_path, "w", encoding="utf-8") as handle:
            json.dump(stats, handle, indent=2, sort_keys=True)
        print(f"Aggregates written to {args.json_path}", file=sys.stderr)

    write_measured(args, stats)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
