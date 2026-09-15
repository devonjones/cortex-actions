"""Drain the `actions` queue and fan label events out to per-service queues.

This service is the missing half of "Labels are the API". postmark's gmail_sync
already turns every Cortex/* label change -- from a triage rule, a Gmail filter,
or a hand drag-and-drop -- into a row on the `actions` queue. Nothing consumed
it, so it was gated off (cortex-s8sd). This consumes it.

It knows nothing about what any downstream service does. It matches a label
against a routing table and enqueues. That is the whole job: a new integration
is two lines of subscriptions.yaml plus a service that can claim from a queue,
and a slow consumer backs up only its own queue rather than blocking dispatch
for everything else.

There is no retry logic here beyond the queue's own. A crash mid-job changes
nothing (see `_route` on atomicity), so the visibility timeout re-delivers and
the next pass does the same work.
"""

from __future__ import annotations

import collections
import os
import signal
import sys
import threading
import time
from types import FrameType
from typing import Any

import psycopg2
import psycopg2.errors
from cortex_utils.logging import configure_logging, get_logger
from cortex_utils.metrics import ERRORS, QUEUE_PROCESSED, start_metrics_server
from cortex_utils.queue import (
    PartitionManager,
    claim,
    complete,
    enqueue,
    ensure_queue_schema,
    fail_or_retry,
    release,
    worker_identity,
)
from cortex_utils.queue.ops import PARTITION_LOCK_TIMEOUT_MS, is_dedup_value
from cortex_utils.queue.ops import _is_missing_partition as is_missing_partition
from prometheus_client import Counter

from actions.subscriptions import (
    SOURCE_QUEUE,
    Subscription,
    SubscriptionError,
    load,
    targets,
)

logger = get_logger(__name__)

SERVICE = "actions-router"

ROUTED = Counter(
    "cortex_actions_routed_total",
    "Label events routed to a downstream queue",
    ["label_prefix", "queue"],
)
SUPPRESSED = Counter(
    "cortex_actions_suppressed_total",
    "Enqueues that dedup suppressed because the work was already queued",
    ["label_prefix", "queue"],
)
UNMATCHED = Counter(
    "cortex_actions_unmatched_total",
    "Label events that matched no subscription (this is normal)",
    ["label_prefix"],
)


# The payload field enqueue() dedups on. Used twice: in the enqueue call, and
# in the index that makes the enqueue call cheap. They must not drift.
DEDUP_KEY = "gmail_id"

DEDUP_INDEX = (
    "idx_queue_dedup",
    "CREATE INDEX IF NOT EXISTS idx_queue_dedup ON queue "
    f"(queue_name, (payload->>'{DEDUP_KEY}')) "
    "WHERE status IN ('pending', 'processing')",
)

# The library's own number for partition DDL, taken from the library rather
# than copied: long enough for an uncontended lock, short enough that a stalled
# router does not stall the pipeline behind it. (An earlier comment here
# credited `DDL_LOCK_TIMEOUT_MS`, which is 5000 and belongs to dead-lettering.
# Naming a constant that does not govern the value is how someone later
# "restores the match" by changing the value.)
#
# Deliberately NOT set on the connection: a session-wide lock_timeout would
# also govern `ensure_queue_schema` at boot, where the library allows
# SCHEMA_LOCK_TIMEOUT_MS = 60s precisely because CREATE INDEX and the first
# partition wait behind ordinary writers. Measured: boot under contention
# raised LockNotAvailable at 2.02s and propagated out of main(), turning a
# deploy against a busy queue into a restart loop. Boot should wait; the heal
# should not.
LOCK_TIMEOUT = f"{PARTITION_LOCK_TIMEOUT_MS}ms"

# Outcomes that mean this batch actually moved work. Anything else hands the
# job back uncharged, so the same rows can come straight back -- see run().
PROGRESS = ("routed", "unmatched", "dropped", "failed")

# EVERY claimed job increments `cortex_queue_processed_total` exactly once, and
# the value comes from here. The vocabulary is the fleet's, not this service's:
# cortex_utils.metrics documents `status` as success | error | skipped, and the
# other four workers emit those words. Inventing `failed`/`released`/`lost`
# here made this queue's failures invisible to any expression written across
# queues -- an `actions` row that reads 0 errors forever. The seven-way detail
# is not lost, it just belongs in the batch log and in ERRORS, which are keyed
# per service and cost nothing to widen.
_STATUS = {
    "routed": "success",
    "unmatched": "success",
    "dropped": "success",
    "failed": "error",
    "released": "skipped",  # claimed, deliberately not processed
    "lost": "skipped",
    "unsettled": "skipped",
}

# Likewise: `job_failed` and `claim_lost` are what postmark and triage emit.
_ERROR_TYPE = {"released": "never_started", "failed": "job_failed"}

# What one claimed event ended up as. The point of naming them is the batch
# summary in run(): after the producer is switched on, "8000 claimed, 8000
# unmatched" and "8000 claimed, 8000 failed" look identical in a metric you have
# not built a dashboard for yet, and identical in a log that only reports the
# interesting case.
#
#   routed     enqueued to at least one queue, completed
#   unmatched  no subscription, completed (the common case, not a failure)
#   dropped    malformed beyond repair, completed so it cannot be retried
#   lost       claim expired under us; whoever holds it now will redo the work
#   released   infrastructure failed, handed back with no attempt charged
#   failed     a fault in this event; one attempt charged
#   unsettled  we could not even tell the queue what happened -- see _settle
OUTCOMES = (
    "routed",
    "unmatched",
    "dropped",
    "lost",
    "released",
    "failed",
    "unsettled",
)


def _label_prefix(label: str) -> str:
    """First two segments, e.g. "Cortex/Family" -- bounded metric cardinality.

    The full label is unbounded (every variable-interpolated label a rule can
    mint becomes a new series), and a metric that grows with user data is how a
    Prometheus instance falls over.
    """
    return "/".join(label.split("/")[:2])


# Failures that mean the work NEVER STARTED, so the event is released rather
# than charged an attempt. NOT the whole of that category any more: a
# CheckViolation naming no constraint is a missing partition, handled ahead of
# this tuple, and it releases too. What is left over is a fault in the event.
#
# This is the distinction cortex-school spent five review rounds and four P0s
# getting right, in both directions. The primitive's own docstring names the
# incident; getting it backwards retires healthy work, and getting it backwards
# the other way re-queues broken work forever.
_NEVER_STARTED = (
    psycopg2.OperationalError,  # connection gone, server restarting
    psycopg2.InterfaceError,  # connection already closed
)


class Router:
    def __init__(self, dsn: str, subs_path: str, batch_size: int, poll_interval: int):
        self.dsn = dsn
        self.subs_path = subs_path
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self._worker = worker_identity(SERVICE)
        self._stop = threading.Event()
        self._reload_requested = threading.Event()
        self.subscriptions: list[Subscription] = load(subs_path)
        self._conn: Any = None
        self._last_heal = float("-inf")

    # -- lifecycle ---------------------------------------------------------

    def stop(self) -> None:
        self._stop.set()

    def request_reload(self) -> None:
        self._reload_requested.set()

    def reload(self) -> None:
        """Re-read the routing table. A bad file keeps the running one.

        Routing to nowhere because a typo emptied the table is a silent
        outage; refusing the reload is a loud one.
        """
        try:
            self.subscriptions = load(self.subs_path)
            logger.info(
                "reloaded subscriptions",
                count=len(self.subscriptions),
                path=self.subs_path,
            )
        except SubscriptionError as e:
            ERRORS.labels(service=SERVICE, error_type="bad_subscriptions").inc()
            logger.error(
                "subscriptions reload REFUSED, keeping previous table",
                error=str(e),
                active_count=len(self.subscriptions),
            )

    def _connect(self) -> Any:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self.dsn)
        return self._conn

    def _ensure_schema(self) -> None:
        # Not a manual migration step: a schema that only existed as a CLI
        # command the deploy never ran caused a crash-loop once (cortex-rgmk).
        #
        # DEDUP_INDEX is not optional tuning. enqueue(dedup_key=...) tests
        # `NOT EXISTS (... WHERE queue_name = %s AND payload->>%s = %s AND
        # status IN ('pending','processing'))` before every insert, and the
        # canonical indexes do not cover a payload expression. The first thing
        # this router does in production is sweep 82,288 Cortex/*-labelled
        # messages (counted on hades, 2026-09-15) into their target queues;
        # without this index each of those enqueues scans every pending row
        # already there, which is the quadratic shape that looks
        # like "the router is slow" and is actually "the router will not
        # finish". The library provides extra_indexes for exactly this.
        #
        # The deployment already has unique dedup indexes of this shape, but
        # each names its queues as a literal predicate -- idx_queue_dedup_gmail
        # is ('parse', 'triage', 'actions'), and `school` is not in it. A
        # router cannot use that shape: its target queues come from a YAML file
        # that changes without a migration. Hence one non-unique index over all
        # queue names; the uniqueness those carry is not what enqueue() relies
        # on anyway (see its docstring on the advisory lock).
        ensure_queue_schema(self._connect(), extra_indexes=[DEDUP_INDEX])

    # -- work --------------------------------------------------------------

    def _route(self, conn: Any, job: dict[str, Any]) -> str:
        """Route one event, returning one of OUTCOMES.

        `job` IS A DICT, because that is what `cortex_utils.queue.claim`
        returns. This read it as an object -- `job.payload`, `job.id` -- and
        every test hand-built a dataclass with those attributes, a shape the
        queue library has never produced. The annotation was `Any`, which
        turned off the strict mypy config for the one contract that mattered,
        and nothing drove `run()`, so the suite agreed with the fiction.

        The consequence was not one failed job. `_route` raised AttributeError,
        the handler in `process_job` caught it and then raised AGAIN on
        `job.id` while logging, which escaped `run()` and `main()`. The process
        exited, the row stayed `processing` until the visibility timeout, and
        came back with its attempt budget intact to kill the router again --
        forever, at zero throughput, while the queue grew.

        Enqueues and the completion share ONE transaction. If we enqueued and
        then died before completing, the visibility timeout would re-deliver
        the event and we would enqueue it a second time -- dedup_key only
        suppresses duplicates while a row is still pending or processing, so a
        consumer that had already finished would get the work twice.
        """
        raw = job.get("payload")
        # A payload that is a list or a scalar cannot become well-formed by
        # being retried, and reaching `.get` on it would raise into the handler
        # -- which is how a row nobody can fix reaches dead_letter.
        payload = raw if isinstance(raw, dict) else {}
        label = payload.get("label")
        event_type = payload.get("event_type", "added")
        gmail_id = payload.get("gmail_id")

        # Types, not truthiness. A non-str label or a gmail_id the queue cannot
        # dedup on reaches enqueue and raises QueueError, which the handler
        # turns into fail_or_retry and eventually dead_letter -- so two equally
        # unfixable rows took opposite paths, one dropped cleanly and one buried
        # among the real failures.
        #
        # `is_dedup_value` rather than isinstance(gmail_id, str), because the
        # queue accepts int too and a stricter second copy of someone else's
        # rule drops rows the queue would have taken. One predicate, theirs.
        #
        # event_type is checked for the same reason one level further on: it
        # goes to `Subscription.matches`, where a list raises TypeError out of
        # destination selection -- charging three attempts and dead-lettering a
        # row that is simply malformed.
        if (
            not isinstance(label, str)
            or not is_dedup_value(gmail_id)
            or not isinstance(event_type, str)
        ):
            # Malformed rows cannot become well-formed by being retried.
            # `raw`, not `payload`: the substitution above would print {} for
            # exactly the non-dict case, erasing the only evidence of the one
            # outcome that cannot be undone.
            logger.error(
                "dropping malformed action event", job_id=job.get("id"), payload=raw
            )
            outcome = self._complete(conn, job, "dropped")
            if outcome == "dropped":
                # Same reason, and it matters more here: a dropped event counts
                # `success` in QUEUE_PROCESSED, so this counter is the only
                # metric that distinguishes a discarded event from a routed one.
                ERRORS.labels(service=SERVICE, error_type="malformed_event").inc()
            return outcome

        # CLAUDE.md: 0 for real-time mail, -100 for backfill, "so backfill
        # doesn't block real-time mail processing". Dropping it here would
        # re-prioritise a -100 replay to 0 on the way downstream, putting the
        # 82,288-message historical sweep in front of the mail that arrived
        # this morning -- on `school`, that is a week-old newsletter ahead of a
        # 504 meeting invitation. An int the producer set is data, not config,
        # so a non-int falls back to the default rather than propagating.
        raw_priority = job.get("priority", 0)
        priority = raw_priority if isinstance(raw_priority, int) else 0

        prefix = _label_prefix(label)
        queues = targets(self.subscriptions, label, event_type)

        if not queues:
            # THE COMMON CASE, and deliberately not a failure. Most Cortex/*
            # traffic has no subscriber; failing here would fill dead_letter
            # with healthy events and bury the real ones.
            logger.debug("no subscription", label=label, event_type=event_type)
            outcome = self._complete(conn, job, "unmatched")
            if outcome == "unmatched":
                # After the settle, like ROUTED and SUPPRESSED. This branch
                # runs for the overwhelming majority of events, so it is the
                # one whose shape gets copied -- and a lost claim counted the
                # event here and again on the retry.
                UNMATCHED.labels(label_prefix=prefix).inc()
            return outcome

        enqueued: list[tuple[str, bool]] = []
        for queue_name in queues:
            # None is not failure: it means an identical job is already pending
            # or processing on that queue, so the work is covered. Counting it
            # as ROUTED would have made the sweep look like it enqueued one
            # job per labelled message when it enqueued far fewer -- and that
            # count is what we read before switching the producer on.
            job_id = enqueue(
                conn,
                queue_name,
                {"gmail_id": gmail_id, "label": label, "event_type": event_type},
                priority=priority,
                dedup_key=DEDUP_KEY,
                commit=False,
            )
            enqueued.append((queue_name, job_id is None))

        outcome = self._complete(conn, job, "routed")
        if outcome != "routed":
            # Rolled back. Counting these would inflate `routed` exactly when
            # the router is struggling, and the retry counts them again -- and
            # `routed` is the number README says to read before switching the
            # producer on.
            return outcome

        for queue_name, suppressed in enqueued:
            metric = SUPPRESSED if suppressed else ROUTED
            metric.labels(label_prefix=prefix, queue=queue_name).inc()
        logger.info("routed", label=label, event_type=event_type, queues=queues)
        return outcome

    def _complete(self, conn: Any, job: dict[str, Any], outcome: str) -> str:
        """Settle one claimed job cleanly. The only complete/commit in the service."""
        if not complete(conn, job["id"], self._worker, commit=False):
            # Claim lost to the visibility timeout while we worked. Discard
            # everything; whoever holds the claim now will redo it.
            conn.rollback()
            ERRORS.labels(service=SERVICE, error_type="claim_lost").inc()
            logger.warning(
                "claim lost, rolled back", job_id=job.get("id"), outcome=outcome
            )
            return self._counted("lost")

        conn.commit()
        return self._counted(outcome)

    @staticmethod
    def _counted(outcome: str) -> str:
        """Count one claimed job, exactly once, and return its outcome.

        The only `QUEUE_PROCESSED` increment in the service. Three paths settle
        a job cleanly and each used to carry its own copy of the decision to
        count; only one of the three did, so the queue's own throughput metric
        saw the routed third of the traffic and the majority path was invisible.
        """
        QUEUE_PROCESSED.labels(queue=SOURCE_QUEUE, status=_STATUS[outcome]).inc()
        return outcome

    def process_job(self, job: dict[str, Any]) -> str:
        """Route one job and settle it. Returns one of OUTCOMES, never raises.

        `self._connect()` is INSIDE the try. It was one line above it, which
        defeated everything below: `_settle` drops a dead connection, so the
        very next job reconnects here, and a Postgres that is still down raises
        `OperationalError` from outside every handler -- out of `process_job`,
        out of `run()`, out of `main()`. The same escape the P0 this branch
        fixes had, reintroduced by its own fix, one frame up.
        """
        conn = None
        try:
            conn = self._connect()
            return self._route(conn, job)
        except psycopg2.errors.CheckViolation as e:
            # TWO DIFFERENT FAULTS SHARE SQLSTATE 23514, and treating them alike
            # was wrong in both directions: a genuine violation released forever
            # and never dead-lettered, a missing partition charged and
            # dead-lettered healthy events. The library already owns the
            # predicate -- `enqueue()` itself gates its self-heal on it -- and
            # it keys on `exc.diag.constraint_name` rather than message text,
            # which is locale-dependent and reworded between Postgres versions.
            # `queue_new_valid_status` is the real named CHECK on the
            # deployed schema -- read off hades rather than guessed. An earlier
            # comment here called it `queue_valid_status`, which exists nowhere,
            # and that name was then copied into a test and a ticket.
            if not is_missing_partition(e):
                return self._settle(conn, job, e, kind="failed")
            return self._heal_partition_then_settle(conn, job, e)
        except _NEVER_STARTED as e:
            # THE WORK NEVER STARTED, so the event must not be charged.
            #
            # `cortex_utils.queue.ops.release` documents the incident in its own
            # docstring: using fail_or_retry here "charges the work for an
            # outage, which is how four healthy videos reached terminal on
            # 2026-08-18". max_attempts is 3 with 30s then 60s of backoff, so
            # ninety seconds of Postgres being unreachable would dead-letter
            # every school event in flight -- filling dead_letter with exactly
            # the healthy events this project exists to never lose.
            return self._settle(conn, job, e, kind="released")
        except Exception as e:  # noqa: BLE001 - the loop must survive one bad job
            return self._settle(conn, job, e, kind="failed")

    def _heal_partition_then_settle(
        self, conn: Any, job: dict[str, Any], exc: Exception
    ) -> str:
        """Create the missing partition, at most once per poll interval, then
        hand the job back uncharged whether or not the heal worked.

        A release with nothing fixed is a LIVELOCK: enqueue() self-heals a
        missing partition, except under `commit=False`, which we pass so the
        enqueues and the completion settle atomically, and which deliberately
        gives that up because creating a partition needs a commit that would
        commit the caller's pending work. `ensure_queue_schema` runs once at
        boot. So nothing else in this process ever creates tomorrow's
        partition.

        RATE LIMITED, because the heal is DDL. `CREATE TABLE ... PARTITION OF`
        takes an AccessExclusiveLock on the parent `queue` table that every
        other cortex service claims from, and once per failing job is once per
        job in the backlog: measured against a real Postgres with the heal
        failing, 981 attempts in 3.09 seconds. Postgres's lock queue is FIFO,
        so an ungranted AccessExclusive stalls every later reader too, whether
        or not it conflicts with the lock actually held -- one router error
        becoming a pipeline-wide stall. `_connect()` sets a lock_timeout for
        the same reason.

        A FAILED HEAL STILL RELEASES. Charging it would dead-letter every
        matched event in three passes for an infrastructure fault -- a
        least-privilege role that cannot CREATE TABLE, a shadowed partition
        name, a lock timeout -- and those are exactly the healthy events this
        project exists not to lose. The backstop against spinning is `run()`'s
        no-progress sleep plus this rate limit, not the attempt budget, and
        `partition_heal_failed` is loud enough to page on.
        """
        if time.monotonic() - self._last_heal >= self.poll_interval:
            self._last_heal = time.monotonic()
            try:
                # The CheckViolation aborted the transaction, and the library's
                # partition calls use bare cursors -- without this they raise
                # InFailedSqlTransaction and every heal "fails".
                conn.rollback()
                # A committed session SET, not SET LOCAL: `create_partition`
                # runs its own transactions with bare cursors and sets no
                # timeout of its own, so SET LOCAL would expire before the DDL
                # it is meant to bound. Committing is what makes it survive
                # into them -- measured, a blocked CREATE TABLE ... PARTITION OF
                # then raises LockNotAvailable at 2.00s instead of queueing
                # ahead of every reader of `queue`.
                self._set_lock_timeout(conn, LOCK_TIMEOUT)
                created = PartitionManager(conn).create_future_partitions(days_ahead=3)
                # `created` is logged, not branched on, and that is the
                # invariant: on a violation naming no constraint, zero created
                # means another worker won the race, so releasing is still
                # right. It would stop being right if a row could carry a
                # `created_at` outside the window this heal covers. Nothing
                # sets one today -- the column is NOW() on the server -- and if
                # anything ever does, this is the line that has to change.
                logger.warning(
                    "created missing queue partitions",
                    job_id=job.get("id"),
                    created=created,
                )
            except Exception as heal_exc:  # noqa: BLE001 - see the docstring
                ERRORS.labels(service=SERVICE, error_type="partition_heal_failed").inc()
                logger.error(
                    "could not create the missing partition",
                    job_id=job.get("id"),
                    error=str(exc)[:300],
                    heal_error=str(heal_exc)[:300],
                )
            finally:
                self._set_lock_timeout(conn, None)
        return self._settle(conn, job, exc, kind="released")

    @staticmethod
    def _set_lock_timeout(conn: Any, value: str | None) -> None:
        """Set or reset the session lock_timeout. Never raises.

        Called from the heal's handler, so a failure here must not replace the
        error being handled -- and when the connection is what failed, this
        fails too.
        """
        try:
            with conn.cursor() as cur:
                if value is None:
                    cur.execute("RESET lock_timeout")
                else:
                    cur.execute("SET lock_timeout = %s", (value,))
            conn.commit()
        except Exception:  # noqa: BLE001 - see the docstring
            pass

    def _settle(
        self, conn: Any, job: dict[str, Any], exc: Exception, *, kind: str
    ) -> str:
        """Record the outcome of a failed route. THIS MUST NOT RAISE.

        `run()` calls `process_job` bare. An exception escaping from here
        escapes `run()` and `main()` and the process exits -- which is exactly
        how the original `job.id` bug took the router down: the handler for the
        error raised the same error again.

        So the settle is guarded too, and it has to be: when Postgres is what
        failed, `rollback()` and `release()` fail the same way. The fallback is
        to drop the connection and say nothing to the queue -- the claim's
        5 minute visibility timeout returns the job on its own, with no attempt
        charged, which is the outcome `release()` was going to ask for anyway.
        """
        job_id = job.get("id")
        if conn is None:
            # Its OWN error_type, not `never_started`, for two reasons. The
            # recovery profiles differ sixty-fold -- a released job is back in
            # `poll_interval` seconds, this one sits in `processing` until the
            # five-minute visibility timeout -- and `never_started` is the
            # Postgres-outage series, so an on-call following the alert
            # annotation would be sent to a healthy database. The old hardcoded
            # value also lied for `kind="failed"`: a fault in the event that we
            # could not record is not an outage.
            ERRORS.labels(service=SERVICE, error_type="no_connection").inc()
            logger.error(
                "no connection to settle on; leaving it to the visibility timeout",
                job_id=job_id,
                kind=kind,
                error=str(exc)[:300],
            )
            # `conn` is the local `_route` never received; `self._conn` may
            # still hold a live object whose connect-time setup failed. Drop it
            # rather than reuse a connection we know nothing good about.
            self._drop_conn()
            return self._counted("unsettled")
        try:
            conn.rollback()
            if kind == "released":
                logger.warning(
                    "releasing, the work never started",
                    job_id=job_id,
                    error=str(exc)[:300],
                )
                held = release(conn, job["id"], self.poll_interval, self._worker)
            else:
                logger.error("routing failed", job_id=job_id, error=str(exc)[:300])
                held = fail_or_retry(conn, job["id"], str(exc), self._worker) != "stale"
        except Exception as settle_exc:  # noqa: BLE001 - see the docstring
            # Counted `skipped`, not `error`: the row is still `processing`
            # and comes back when its visibility timeout expires, with no
            # attempt charged. One increment per claimed job either way.
            ERRORS.labels(service=SERVICE, error_type="settle_error").inc()
            logger.error(
                "could not settle the job; leaving it to the visibility timeout",
                job_id=job_id,
                error=str(exc)[:300],
                settle_error=str(settle_exc)[:300],
            )
            self._drop_conn()
            return self._counted("unsettled")

        if not held:
            # The claim expired under us and someone else owns the job now.
            # Reporting our own outcome for their work is how one slow batch
            # becomes a `failed` count nobody can trace to a failure -- which
            # is why the ERRORS increment lives HERE and on the branch below,
            # rather than at the top of this function where it fired before
            # anything knew whether we still held the claim.
            ERRORS.labels(service=SERVICE, error_type="claim_lost").inc()
            logger.warning("claim lost before settling", job_id=job_id, outcome=kind)
            return self._counted("lost")

        ERRORS.labels(service=SERVICE, error_type=_ERROR_TYPE[kind]).inc()
        return self._counted(kind)

    def _drop_conn(self) -> None:
        if self._conn is None:
            return
        try:
            self._conn.close()
        except Exception:  # noqa: BLE001 - closing a dead socket is not news
            pass
        finally:
            self._conn = None

    def _sleep(self) -> None:
        """Idle in 1s ticks so a SIGTERM lands promptly."""
        for _ in range(self.poll_interval):
            if self._stop.is_set() or self._reload_requested.is_set():
                return
            time.sleep(1)

    def run(self) -> None:
        self._ensure_schema()
        logger.info(
            "router starting",
            worker=self._worker,
            subscriptions=len(self.subscriptions),
            queue=SOURCE_QUEUE,
        )
        while not self._stop.is_set():
            if self._reload_requested.is_set():
                self._reload_requested.clear()
                self.reload()

            try:
                jobs = claim(
                    self._connect(),
                    SOURCE_QUEUE,
                    worker=self._worker,
                    limit=self.batch_size,
                    visibility_timeout_min=5,
                )
            except Exception as e:  # noqa: BLE001 - a dead DB must not kill the loop
                ERRORS.labels(service=SERVICE, error_type="claim_error").inc()
                logger.error("claim failed", error=str(e))
                self._drop_conn()
                self._sleep()
                continue

            if not jobs:
                self._sleep()
                continue

            tally: collections.Counter[str] = collections.Counter()
            for job in jobs:
                if self._stop.is_set():
                    break
                tally[self.process_job(job)] += 1

            # One INFO line per batch, always -- the per-event logs cannot show
            # this. `unmatched` is DEBUG because it is the overwhelming majority
            # and `routed` is INFO because it is rare, which means the quiet
            # steady state and a router settling nothing look the same in a log
            # at INFO. This is the line to watch when ACTIONS_QUEUE_ENABLED is
            # switched on and the historical sweep starts moving.
            logger.info(
                "batch",
                claimed=len(jobs),
                **{k: tally[k] for k in OUTCOMES if tally[k]},
            )

            # A BATCH THAT SETTLED NOTHING SLEEPS. Insurance, not the
            # primary bound, and the distinction is worth stating because the
            # measurement that first motivated this line was an artefact:
            # `release()` sets `next_attempt_at = now + delay`, and `claim()`
            # filters on it, so a released row is ALREADY deferred and a
            # stubbed claim that ignores that column exaggerates the spin.
            #
            # What it does bound is a backlog of DIFFERENT rows all failing the
            # same way -- a missing partition across 82k pending events -- being
            # walked at database round-trip speed. That one is real: 981 heal
            # attempts in 3.09s against a real Postgres, before the rate limit
            # in `_heal_partition_then_settle` existed.
            if not any(tally[k] for k in PROGRESS):
                logger.warning(
                    "no progress in this batch, backing off", claimed=len(jobs)
                )
                self._sleep()

        logger.info("router stopped")


def main() -> None:
    configure_logging(service_name=SERVICE, level=os.environ.get("LOG_LEVEL", "INFO"))

    missing = [
        v
        for v in ("POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD")
        if not os.environ.get(v)
    ]
    if missing:
        sys.exit(f"missing required environment: {', '.join(missing)}")

    dsn = (
        f"host={os.environ['POSTGRES_HOST']} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={os.environ['POSTGRES_DB']} "
        f"user={os.environ['POSTGRES_USER']} "
        f"password={os.environ['POSTGRES_PASSWORD']}"
    )

    subs_path = os.environ.get("SUBSCRIPTIONS_PATH", "/app/config/subscriptions.yaml")
    try:
        router = Router(
            dsn=dsn,
            subs_path=subs_path,
            batch_size=int(os.environ.get("BATCH_SIZE", "50")),
            poll_interval=int(os.environ.get("POLL_INTERVAL", "5")),
        )
    except SubscriptionError as e:
        # Fatal at startup, unlike a reload: there is no previous good table to
        # fall back to, and a router with no table routes nothing, silently.
        sys.exit(f"cannot start: {e}")

    def on_term(signum: int, frame: FrameType | None) -> None:
        logger.info("shutdown signal", signal=signum)
        router.stop()

    def on_hup(signum: int, frame: FrameType | None) -> None:
        logger.info("reload signal")
        router.request_reload()

    signal.signal(signal.SIGTERM, on_term)
    signal.signal(signal.SIGINT, on_term)
    signal.signal(signal.SIGHUP, on_hup)

    start_metrics_server(port=int(os.environ.get("METRICS_PORT", "8000")))
    router.run()


if __name__ == "__main__":
    main()
