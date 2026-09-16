# cortex-actions — working notes

The dispatcher between "a label changed" and "something happens about it."

Read `README.md` first for what it does. This file is the reasoning that is easy
to undo by accident.

## This service is on the critical path for every label event

Every `Cortex/*` label change in the whole system passes through here. That
shapes what belongs in it: matching and enqueueing, nothing else. Every feature
added here is a feature that can break dispatch for all consumers at once.

**Handlers do not belong here.** The original design (cortex-aje) had one worker
own a registry of handler functions — ticketing, ArchiveBox, webhooks, Obsidian.
That was superseded deliberately. If you find yourself adding a branch that
*does* something with an event rather than routing it, the thing you want is a
new consumer service with its own queue and its own two lines of
`subscriptions.yaml`.

## Silence is the failure mode

A router that routes nothing looks exactly like a router with nothing to route.
Three specific guards exist because of that, and none of them are optional:

- **An unmatched label is completed and counted, never failed.** Most `Cortex/*`
  traffic has no subscriber. Failing those would fill `dead_letter` with healthy
  events and bury the real ones. `cortex_actions_unmatched_total` is how you see
  them.
- **A bad `subscriptions.yaml` on SIGHUP is refused, and the running table stays
  live.** Reloading into an empty table would silently stop all routing. At
  startup the same file is fatal instead, because there is no good table to keep.
- **Routing to `actions` is rejected at parse time.** It is this service's own
  input queue; a subscription pointing at it re-enqueues every event it just
  claimed, forever.

## Atomicity is load-bearing, not a nicety

`_route()` puts every `enqueue` and the source job's `complete` in **one
transaction**, with `commit=False` on each and a single explicit `conn.commit()`.

This is not tidiness. `dedup_key` only suppresses a duplicate while the target
row is still `pending` or `processing`. If the router enqueued, then died before
completing, the visibility timeout would re-deliver the event — and a consumer
that had already finished the first copy would do the work twice. For
cortex-school that means a duplicate Discord message and a duplicated vault note.

If `complete()` returns `False` the claim was lost mid-job: roll back. Whoever
holds the claim now will redo the enqueues.

## Metric labels must not grow with user data

Label names can carry interpolated variables from triage rules, so the set of
full label strings is unbounded. `_label_prefix()` truncates to two segments on
purpose. Never add a metric dimension carrying a full label, a gmail_id, or a
sender.

## Deployment order

`ACTIONS_QUEUE_ENABLED=false` in postmark is not a leftover — it is the guard
described in cortex-s8sd. With no consumer, the producer only grows the queue
unbounded. **Ship and verify this service first, then flip the flag**, and watch
queue depth after.

Rolling this service back without also turning the flag off re-creates exactly
the condition the flag exists to prevent.

## Project rules that apply here

From the parent `~/Projects/cortex/CLAUDE.md`:

- **Never push to main.** Feature branch, PR, review, merge.
- **No hardcoded homelab IPs or hostnames** — this repo is public. Host and port
  come from the environment, with no defaults that name internal addresses.
- **Shared code goes to cortex-utils**, never cross-imported between services.
  This service imports `cortex_utils.queue` and nothing from postmark or triage.
- Queue priority convention: `0` real-time, `-100` backfill.

## Testing

The routing decision is a pure function — `actions.subscriptions.targets()` —
so the interesting behaviour needs no database. `tests/test_router.py` stubs the
queue library and asserts the **seven** outcomes in `router.OUTCOMES`, which
differ in what they cost the event: `routed`, `unmatched` and `dropped` settle
it; `failed` charges an attempt toward dead_letter; `released`, `lost` and
`unsettled` hand it back uncharged.

**A stub must match the function it replaces**, signature and defaults —
`test_the_stubs_match_the_library` asserts that against `inspect.signature`,
and `test_the_job_helper_matches_what_claim_actually_returns` reads `claim()`'s
`RETURNING` clause. Ten router tests once passed against a job shape the queue
library has never produced, while the service could not process a single real
job; every fixture here is one edit away from being that again.

`test_shipped_config_is_valid_and_routes_the_real_label` asserts that the YAML
actually in `config/` parses and routes `Cortex/Family/School/DPS`. Keep it: a
routing table that ships broken is the one failure no unit test of the parser
would catch.
