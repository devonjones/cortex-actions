# Configuration

```json
{
  "defaults_version_checked": "1.6.0",
  "disabled": [],
  "bots": {
    "gemini": false
  }
}
```

---

# Agents

Each H2 names a reviewer. The one-line summary tells the main loop **what it
checks and when to spawn it** — use it to decide whether the PR diff is in scope.

The roster is deliberately short. Five project-specific reviewers guard the ways
this service can quietly stop being trustworthy. The generic Python pack
(`test-coverage`, `error-handling`, `resource-leak`, `clarity`) gets added on the
first PR that brushes against it, rather than carried from day one.

This service sits on the critical path for every label event in the system, so
the bar for "this is fine, it only affects X" is higher here than in a leaf
consumer.

## queue-contract-reviewer

**What it checks:** that `cortex_utils.queue` is used the way it is meant to be.
Enqueues and the source job's `complete()` must share one transaction
(`commit=False` on every call, one explicit `conn.commit()`); `complete()`'s
return value must be checked and a `False` must roll back; `fail_or_retry` is for
"attempted and failed" while `release` is for "never started"; no hand-written
queue SQL anywhere.
**When to spawn:** PR touches `router.py`, or anything that enqueues, claims,
completes or fails a job.

**Default severity: P1.** A broken transaction boundary here duplicates work in
every downstream consumer, and the duplicate is invisible in this service's own
logs — it shows up as a second Discord message or a second vault note, far away.

Flag specifically:
1. An `enqueue(...)` without `commit=False` in a path that also completes a job.
2. A `complete(...)` whose return value is discarded.
3. A success path that commits before all enqueues have been issued.
4. Any `INSERT INTO queue` or `UPDATE queue` written by hand.

## routing-silence-reviewer

**What it checks:** that events cannot vanish, and that healthy no-ops are never
treated as failures. Unmatched labels must be completed and counted, not failed;
a malformed event must be dropped once rather than retried forever; a refused
config reload must leave the previous table live.
**When to spawn:** PR touches `router.py`, `subscriptions.py`, or the reload path.

**Default severity: P1.** This service's characteristic failure is doing nothing
while looking healthy, which is the same shape as the three multi-day outages
documented in the sibling trackers.

Flag specifically:
1. An unmatched label routed to `fail_or_retry` — that fills `dead_letter` with
   healthy events and buries the real ones.
2. A malformed payload that is retried rather than dropped (attempts will just
   exhaust, slowly, into `failed`).
3. A reload path that assigns to `self.subscriptions` before validation succeeds.
4. Removal of a counter that is the only evidence a branch was taken.

**Do NOT flag:** returning early on an empty target list. That is the designed
common case.

## loop-safety-reviewer

**What it checks:** that the router cannot feed itself. No subscription may
target `actions`; no code path may enqueue onto the queue it is currently
claiming from; fan-out per event stays bounded.
**When to spawn:** PR touches `subscriptions.py` validation, `SOURCE_QUEUE`, or
adds any new enqueue target.

**Default severity: P0.** A self-referential subscription is an unbounded write
loop against the shared `queue` table that every other cortex service depends on.
The blast radius is the whole pipeline, not this service.

Flag specifically:
1. Any weakening or bypass of the `queue == SOURCE_QUEUE` check in `parse()`.
2. A target queue name derived from the *payload* rather than from the config —
   that lets an email's own label choose its destination queue.
3. Fan-out proportional to anything unbounded rather than to the length of the
   routing table.

## dead-code-reviewer

Review for **dead code**, with one bias specific to this service: it is a
dispatcher, so the characteristic rot is a knob that is read but never acted on.
A subscription field that `parse()` validates and `targets()` ignores looks
configured and does nothing — which is this repo's signature failure mode
(silence that reads as health) wearing a different hat.

**Default severity: P3**, except a parsed-but-unused config field, which is
**P2**: an operator sets it, sees no error, and believes routing changed.

**What to flag:**

1. **A `Subscription` field parsed and validated but never read by `matches()`
   or `targets()`.** Cross-check every key `parse()` accepts against every key
   the matching path actually consults.
2. **A documented `subscriptions.yaml` key with no parser support** — the
   inverse, and just as silent.
3. **A metric defined but never `.inc()`d**, or a label dimension never passed.
   A counter that cannot move is worse than no counter: it reads as zero.
4. Unused module-level functions, constants, imports; stale `__all__`; partial
   renames where the old name survives; `if False:` branches; commented-out code.
5. **Helpers left behind by a deletion** — if a code path went away, did its
   constants, regexes and error classes go with it?

**Review approach:**

1. For each key in `parse()`, grep for it in `matches()`/`targets()`/`router.py`.
2. For each `Counter` in `router.py`, grep for a corresponding `.labels(...).inc()`.
3. Grep the workspace for each flagged symbol and paste the result in the
   comment so the author can check the claim rather than trust it.

**Do NOT flag:**

- `SOURCE_QUEUE` and `VALID_EVENTS` — read by validation, not by the hot path.
- Anything referenced only from `config/subscriptions.yaml` or a test fixture.
- Exception classes raised but never caught inside this repo; a consumer may
  catch them.

---

## metric-cardinality-reviewer

**What it checks:** that no Prometheus label dimension grows with user data.
**When to spawn:** PR adds or changes a `Counter`, `Gauge`, `Histogram`, or any
`.labels(...)` call.

**Default severity: P2.** It degrades the monitoring host rather than this
service, and it degrades it slowly, which is why it needs a reviewer rather than
an alert.

Flag specifically:
1. A full label name, `gmail_id`, message-id, sender address, or subject used as
   a metric dimension. Triage rules mint labels with interpolated variables, so
   the set of full label strings is unbounded.
2. Removal or widening of `_label_prefix()`'s two-segment truncation.

**Do NOT flag:** `queue` as a dimension. The set of destination queues is
bounded by the routing table, which is a small hand-written file.

---

# Guidelines

## File scope

| Path | Reviewers |
|---|---|
| `src/actions/services/router.py` | queue-contract, routing-silence, metric-cardinality |
| `src/actions/subscriptions.py` | routing-silence, loop-safety |
| `config/subscriptions.yaml` | loop-safety |
| `tests/**` | test-coverage (once added) |
| `src/actions/**`, `config/subscriptions.yaml` | dead-code |
| `Dockerfile`, `.github/workflows/**` | homelab-values |

## Tooling assumed in CI

`black --check`, `ruff check`, `mypy src/actions`, `pytest`. A finding that one
of these would have caught is a CI gap, not a review finding — say so and move on.

## Severity convention

- **P0** — blast radius beyond this service. Blocks merge.
- **P1** — this service silently does the wrong thing. Blocks merge.
- **P2** — degrades over time or under load. Fix now or file a bead.
- **P3** — clarity and maintainability. Author's call.

## Output format

One finding per issue:

```
**[P1] queue-contract-reviewer** — `src/actions/services/router.py:174`
enqueue() defaults to commit=True here, so the job's enqueues commit before
complete() runs. A crash in between re-delivers the event and double-enqueues.
Suggested: pass commit=False and let the existing conn.commit() settle it.
```

State the failure, not the preference. If you cannot describe a concrete
sequence of events that produces a wrong outcome, it is a P3 at most.

## Deferring findings

Reasonable suggestions that are out of scope for the PR get a bead rather than a
merge block, per the parent CLAUDE.md:

```sh
bd create --title="..." --type=task --priority=3 \
  --description="Deferred from PR #N review. <context, and when to revisit>"
~/.local/bin/beads-commit "Track deferred items from PR #N review"
```
