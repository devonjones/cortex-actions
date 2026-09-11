# cortex-actions

Routes Cortex label events to downstream workflow queues.

This is the missing half of the project's stated principle — *"Labels are the API.
Any label application (triage rules, Gmail filters, manual drag-and-drop) can
trigger workflows."* The producer for that already existed and was correct;
nothing consumed it, so it was switched off. This consumes it.

```
gmail-sync  ──[Cortex/* label added or removed]──▶  queue: actions
                                                         │
                                                   actions-router
                                                  (subscriptions.yaml)
                                            ┌────────────┼────────────┐
                                            ▼            ▼            ▼
                                       queue: school  queue: …   queue: …
```

## What it does, and deliberately does not do

It matches a label against a routing table and enqueues onto the named queues.
That is all. It knows nothing about what any downstream service does with the
event.

This is narrower than the original design (cortex-aje), which had one actions
worker own a registry of handlers — ticketing, ArchiveBox, webhooks, Obsidian —
and dispatch to them directly. Fanning out to *queues* instead means:

- a new integration is two lines of YAML plus a service that already knows how
  to claim from a queue, rather than a change to the component sitting on the
  critical path for every label event;
- a slow or broken consumer backs up only its own queue, instead of blocking
  dispatch for everything else;
- each consumer gets the queue's own retry, visibility timeout and dead-letter
  handling, rather than sharing one worker's error semantics.

## Why the event source is Gmail history, not the labeling worker

The original spec recommended having `labeling-worker` emit an event after it
applies a label, with Gmail polling as a backup. The implementation went the
other way, and should stay that way: reading label changes out of the Pub/Sub
history stream catches a triage rule, a Gmail filter and a hand drag-and-drop
identically, because all three are just label mutations by the time Gmail
reports them. Emitting from the labeling worker would see only the first, which
is precisely the case the principle above exists to cover.

Producer: `postmark/src/postmark/services/gmail_sync.py` → `_process_label_event()`.
It enqueues atomically with the `emails_raw.label_ids` update, so the queue can
never disagree with the stored label state.

## The routing table

`config/subscriptions.yaml`, mounted at runtime:

```yaml
subscriptions:
  - label: "Cortex/Family/School/*"
    queue: school
    events: [added]
```

- **`label`** is a glob. `*` spans `/`, so `Cortex/Family/School/*` covers
  `.../DPS` and anything deeper. It does **not** match the bare parent
  `Cortex/Family/School` — subscribe to that separately if it is ever applied.
  Matching is case-sensitive.
- **`events`** defaults to `["added"]`. Un-labelling is real information, so
  `removed` is carried through to the consumer, but a subscription has to opt in.
- **`queue`** cannot be `actions`. That is this service's own input, and routing
  to it would re-enqueue every event it just claimed, forever.

`SIGHUP` reloads the table. **A malformed file is refused and the running table
stays live** — routing to nowhere because a typo emptied the table is a silent
outage, and refusing the reload is a loud one. At *startup* a malformed file is
fatal, because there is no previous good table to keep.

A label matching no subscription is the normal case, not an error: most
`Cortex/*` traffic has no subscriber. Those events are completed and counted
(`cortex_actions_unmatched_total`), never failed — failing them would fill
`dead_letter` with healthy events and bury the real ones.

## Atomicity

Enqueues and the completion of the source job share one transaction. If the
router enqueued and then died before completing, the visibility timeout would
re-deliver the event and it would be enqueued a second time — `dedup_key` only
suppresses duplicates while a row is still pending or processing, so a consumer
that had already finished would get the work twice.

If the claim is lost to the visibility timeout mid-job, the enqueues are rolled
back; whoever holds the claim now will redo them.

## Deployment order matters

`ACTIONS_QUEUE_ENABLED` is `false` in postmark for one reason: with no consumer,
enqueueing only grows the queue unbounded (cortex-s8sd). **Deploy this service
and verify it drains before flipping that flag.**

## Configuration

| var | meaning | default |
|---|---|---|
| `POSTGRES_HOST` / `_DB` / `_USER` / `_PASSWORD` | required; no defaults | — |
| `POSTGRES_PORT` | | `5432` |
| `SUBSCRIPTIONS_PATH` | routing table | `/app/config/subscriptions.yaml` |
| `BATCH_SIZE` | jobs claimed per pass | `50` |
| `POLL_INTERVAL` | idle seconds between claims | `5` |
| `METRICS_PORT` | Prometheus `/metrics` | `8000` |
| `LOG_LEVEL` | | `INFO` |

## Metrics

- `cortex_actions_routed_total{label_prefix,queue}`
- `cortex_actions_unmatched_total{label_prefix}`
- plus the standard `cortex_queue_processed_total` / `cortex_errors_total`

`label_prefix` is the first two label segments, not the full label. Labels can
carry interpolated variables, so a full-label dimension would grow a new time
series per email — a metric that grows with user data is how a Prometheus
instance falls over.

## Development

```sh
uv sync --all-extras
uv run pytest
uv run mypy src/actions
uv run black --check . && uv run ruff check .
```

The routing decision is a pure function (`actions.subscriptions.targets`), so
most of the behaviour is testable without a database.
