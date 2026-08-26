# ado-pr-push-stats

Aggregates how often people push to their Azure DevOps pull requests, so decisions about
an automated code reviewer's push debounce — a longer quiet period, a per-pull-request
review cap, a different starvation bound — are made against the org's actual behaviour
rather than an intuition about it.

Read-only against the Azure DevOps REST API. It touches no database, no build and no
reviewer deployment; the only credential it wants is a PAT with `Code (read)`.

The debounce it replays is the one in Argus (an internal AI code review platform), whose
defaults are reproduced here as constants — but every parameter is a flag, so it models
any quiet-period-plus-starvation-bound scheme.

## Running it

```bash
export AZDO_PAT=...          # PAT with Code (read) scope
python3 pr_push_stats.py \
    --org https://dev.azure.com/contoso \
    --project MyProject \
    --days 30 \
    --json /tmp/pr-push-stats.json
```

Python 3.10+ and the standard library. No packages to install.

Useful flags:

| Flag | Effect |
| --- | --- |
| `--all-projects` | Scan every project in the organisation instead of named ones. |
| `--project NAME` | Repeatable, for scanning a few projects but not all. |
| `--status active` | Repeatable. Default is `completed` + `abandoned`. |
| `--exclude-repo NAME` | Repeatable. Skip sandboxes, archives, forks. |
| `--quiet-seconds N` | Re-run the debounce simulation against a different quiet period. |
| `--max-staleness-minutes N` | Likewise for the starvation bound. |
| `--anonymise-repos` | Replace repository names with `repo-1`…`repo-N` in the output. |
| `--no-draft-detection` | Skip the per-PR threads call. Halves the request count. |
| `--concurrency N` | Parallel per-PR fetches. Default 8. Lower it if you get throttled. |

Cost: two API calls per pull request (iterations + threads), plus one per repository and
one page per hundred pull requests. A 30-day window over a mid-sized org is typically a
few thousand requests and a couple of minutes.

## What counts as a push

A **push** is one Azure DevOps *pull request iteration* — a source-ref update — regardless
of how many commits it carried. Ten commits pushed together are one push; the same ten
pushed one at a time are ten. That is the number that matters here, because it is what
triggers a review.

Iteration 1 is created with the pull request, so `pushes == iterations` and
`pushes after creation == iterations - 1`. Each iteration also carries a `reason`
(`push`, `forcePush`, `rebase`, `retarget`, `resolveConflicts`), reported as a breakdown
so force-push and rebase noise is separable from ordinary work.

## How the draft timeline is reconstructed

Azure DevOps exposes `isDraft` as a *current* state and has no `publishedAt` field. Draft
transitions are recorded as system threads on the pull request, so the timeline is read
back out of `GET .../pullRequests/{id}/threads`: any thread carrying a draft-ish property
key, or a system comment mentioning draft/publish, is treated as a transition, and the
direction of the *first* one determines the starting state (first transition "to
published" ⇒ it started as a draft).

That thread shape is not a documented contract, so the report prints the property keys it
actually observed and how many pull requests yielded no draft event at all. If that count
is zero across a whole scan, the draft split is not a finding — it means the detection
missed, and the section says so in the output.

## The debounce simulation

The `DEBOUNCE SIMULATION` section replays each pull request's push timeline through the
same rules Argus' `ReviewDebounceScheduler` and `ReviewDebounceCoordinator` apply:

- every push registers as the pull request's latest and schedules its own message for
  `QuietPeriodSeconds` (+ half the jitter) later;
- on firing, a message that is no longer the latest coalesces away;
- unless the burst has been running longer than `MaxStalenessMinutes`, in which case it is
  forced through against head, and the burst clock restarts.

Defaults (150s quiet, 15s jitter, 20m starvation bound) are Argus' shipped values and are
overridable, so the same 30 days of data can be re-scored against a candidate configuration
without re-fetching anything if the JSON dump is kept.

Draft pushes are excluded from the trigger set, matching a reviewer configured to defer or
ignore drafts; publication itself counts as one trigger, since Azure DevOps re-notifies
with `isDraft` cleared. The alternative — reviewing drafts too — is reported as a single
counterfactual total alongside it.

## Output and data handling

The report and the `--json` dump contain aggregates only: counts, histograms, percentiles
and per-repository rollups. No pull request ids, titles, branch names, authors or
timestamps of individual pushes are emitted at any point, so the output can be pasted into
a chat, a ticket or a planning document as-is. `--anonymise-repos` removes the only
identifying strings that remain.

## Caveats worth knowing before acting on the numbers

- **Open pull requests are excluded by default.** Their push counts are still accumulating,
  so including them biases the distribution downwards. `--status active` adds them back if
  you want the in-flight picture; do not mix the two in one comparison.
- **The simulation assumes every push produced a webhook the reviewer accepted.** Filtered
  repositories, gatekeeper rejections, paused pull requests and dropped webhooks all mean
  the real review count is lower than the simulated one. Read it as a ceiling.
- **Jitter is modelled as its average**, not sampled, so a burst that sits exactly on the
  quiet-period boundary resolves deterministically here and probabilistically in production.
- **Iteration timestamps are server-side push times**, which is what the debounce sees, but
  a retarget or a merge-conflict resolution creates an iteration without the author having
  pushed anything. The `by_reason` breakdown is there to size that.
