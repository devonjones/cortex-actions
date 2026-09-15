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
- **Unknown keys are refused**, and so is an empty `subscriptions:` list. Both
  are typos that parse cleanly and route nothing: `event:` for `events:` takes
  the default and silently stops carrying `removed`, and a table with no rules
  sends every event down the `unmatched` path, which is completed and counted
  as success. A router with no rules has no reason to be running.

`SIGHUP` reloads the table. **A malformed file is refused and the running table
stays live** — routing to nowhere because a typo emptied the table is a silent
outage, and refusing the reload is a loud one. Unreadable counts as malformed:
a bind mount that lands as a directory is a refused reload, not a dead process.
At *startup* a malformed file is fatal, because there is no previous good table
to keep.

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

The downstream job keeps the priority the source event carried, so a `-100`
backfill row stays behind real-time mail on its way through **this service**.
The pipeline does not deliver that yet: the only producer for the `actions`
queue inserts without a priority column, so every event arrives at the table
default of 0 (`cortex-0oya`). This end is ready for the fix at the other end.

## Failure, and what it costs the event

An event is only ever charged an attempt for a fault in the event itself. The
queue retires a job after three, so charging an outage retires healthy work —
ninety seconds of Postgres being away would dead-letter every event in flight.

- **Infrastructure failed** (`OperationalError`, `InterfaceError`) — released,
  no attempt charged, retried after the poll interval.
- **No partition for the row** (`CheckViolation` naming no constraint) — the
  missing partition is created and *then* the event is released, whether or not
  the heal worked. `enqueue(commit=False)` gives up the library's own
  self-heal, because creating a partition needs a commit that would commit our
  pending work, and `ensure_queue_schema` runs only at boot; releasing without
  creating it is a livelock, not a recovery. Charging a failed heal is worse
  still: a role that cannot `CREATE TABLE`, a shadowed partition name or a lock
  timeout would dead-letter every matched event in three passes.

  The heal runs **at most once per poll interval**, because it is DDL:
  `CREATE TABLE … PARTITION OF` takes an AccessExclusiveLock on the parent
  `queue` table, Postgres's lock queue is FIFO, and an ungranted
  AccessExclusive stalls every reader behind it — so one router error would
  otherwise become a pipeline-wide stall. The connection carries a 2s
  `lock_timeout` for the same reason.
- **A CHECK violation naming a real constraint** — charged. Two different
  faults share SQLSTATE 23514, and the library's own predicate separates them
  on `exc.diag.constraint_name`.
- **A fault in the event** — one attempt charged, backed off, eventually
  dead-lettered.
- **Malformed beyond repair** (no label, an id the queue cannot dedup on, a
  non-string event type) — dropped once and completed. Retrying cannot make it
  well-formed, and charging it buries an unfixable row among the real failures.
- **Nothing could be said to the queue at all** — the connection is dropped and
  the claim's visibility timeout returns the job, uncharged.

A batch in which nothing settled sleeps for the poll interval before claiming
again — insurance, not the primary bound. `release()` sets
`next_attempt_at = now + delay`, so a released row is already deferred; what
this stops is a backlog of *different* rows all failing the same way being
walked at database round-trip speed.

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

- `cortex_actions_routed_total{label_prefix,queue}` — events **enqueued**
  downstream, counted after the transaction commits, so a rolled-back route
  counts nothing. Not the same as events routed: an enqueue the queue's dedup
  suppressed because identical work is already pending counts below instead.
- `cortex_actions_suppressed_total{label_prefix,queue}` — the other half.
  `routed + suppressed` is the number of (event, destination) pairs handled;
  `routed` alone is the number of new downstream jobs. Read both before
  switching the producer on, or a sweep that mostly deduplicated looks like a
  sweep that mostly failed.
- `cortex_actions_unmatched_total{label_prefix}` — no subscription matched.
  The common case, and not a failure.
- `cortex_queue_processed_total{queue="actions",status}` — **exactly one
  increment per claimed job**, using the fleet's vocabulary rather than this
  service's own words, so a cross-queue expression sees this queue:
  - `success` — routed, unmatched or dropped; the job is settled and gone
  - `error` — one attempt charged: a fault in the event, or a CHECK violation
    naming a real constraint
  - `skipped` — claimed but deliberately not processed: released after an
    infrastructure failure, lost to another worker, or left to the visibility
    timeout. No attempt charged, so the same job comes back.
- `cortex_errors_total{service="actions-router",error_type}` — `never_started`,
  `no_connection`, `job_failed`, `claim_lost`, `malformed_event`,
  `settle_error`, `partition_heal_failed`, `bad_subscriptions`, `claim_error`.

  `never_started` and `no_connection` are both outages and are deliberately
  separate: a `never_started` job is back in a poll interval, a
  `no_connection` one sits in `processing` until the five-minute visibility
  timeout. `partition_heal_failed` counts *heal attempts*, which are rate
  limited to one per poll interval, not affected events — so it under-reports
  by design, and at a `POLL_INTERVAL` above 10s it falls below the fleet's
  `rate(cortex_errors_total[5m]) > 0.1` alert threshold on its own. The events
  it describes still page through `never_started`, which is proportional.

The seven-way detail behind those three statuses is in the per-batch INFO log
line, not in a metric: `batch claimed=50 routed=3 unmatched=47`.

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
