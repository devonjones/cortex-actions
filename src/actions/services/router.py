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

import os
import signal
import sys
import threading
import time
from types import FrameType
from typing import Any

import psycopg2
from cortex_utils.logging import configure_logging, get_logger
from cortex_utils.metrics import ERRORS, QUEUE_PROCESSED, start_metrics_server
from cortex_utils.queue import (
    claim,
    complete,
    enqueue,
    ensure_queue_schema,
    fail_or_retry,
    worker_identity,
)
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
UNMATCHED = Counter(
    "cortex_actions_unmatched_total",
    "Label events that matched no subscription (this is normal)",
    ["label_prefix"],
)


def _label_prefix(label: str) -> str:
    """First two segments, e.g. "Cortex/Family" -- bounded metric cardinality.

    The full label is unbounded (every variable-interpolated label a rule can
    mint becomes a new series), and a metric that grows with user data is how a
    Prometheus instance falls over.
    """
    return "/".join(label.split("/")[:2])


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
        ensure_queue_schema(self._connect())

    # -- work --------------------------------------------------------------

    def _route(self, conn: Any, job: Any) -> bool:
        """Route one event. Returns True if the job settled cleanly.

        Enqueues and the completion share ONE transaction. If we enqueued and
        then died before completing, the visibility timeout would re-deliver
        the event and we would enqueue it a second time -- dedup_key only
        suppresses duplicates while a row is still pending or processing, so a
        consumer that had already finished would get the work twice.
        """
        payload = job.payload or {}
        label = payload.get("label")
        event_type = payload.get("event_type", "added")
        gmail_id = payload.get("gmail_id")

        if not label or not gmail_id:
            # Malformed rows cannot become well-formed by being retried.
            logger.error(
                "dropping malformed action event", job_id=job.id, payload=payload
            )
            ERRORS.labels(service=SERVICE, error_type="malformed_event").inc()
            if complete(conn, job.id, self._worker, commit=False):
                conn.commit()
                return True
            conn.rollback()
            return False

        prefix = _label_prefix(label)
        queues = targets(self.subscriptions, label, event_type)

        if not queues:
            # THE COMMON CASE, and deliberately not a failure. Most Cortex/*
            # traffic has no subscriber; failing here would fill dead_letter
            # with healthy events and bury the real ones.
            UNMATCHED.labels(label_prefix=prefix).inc()
            logger.debug("no subscription", label=label, event_type=event_type)
            if complete(conn, job.id, self._worker, commit=False):
                conn.commit()
                return True
            conn.rollback()
            return False

        for queue_name in queues:
            enqueue(
                conn,
                queue_name,
                {"gmail_id": gmail_id, "label": label, "event_type": event_type},
                dedup_key="gmail_id",
                commit=False,
            )
            ROUTED.labels(label_prefix=prefix, queue=queue_name).inc()

        if not complete(conn, job.id, self._worker, commit=False):
            # Claim lost to the visibility timeout while we worked. Discard the
            # enqueues; whoever holds the claim now will redo them.
            conn.rollback()
            ERRORS.labels(service=SERVICE, error_type="claim_lost").inc()
            logger.warning("claim lost, rolled back", job_id=job.id, label=label)
            return False

        conn.commit()
        logger.info("routed", label=label, event_type=event_type, queues=queues)
        QUEUE_PROCESSED.labels(queue=SOURCE_QUEUE, status="completed").inc()
        return True

    def process_job(self, job: Any) -> bool:
        conn = self._connect()
        try:
            return self._route(conn, job)
        except Exception as e:  # noqa: BLE001 - the loop must survive one bad job
            conn.rollback()
            ERRORS.labels(service=SERVICE, error_type="route_error").inc()
            logger.error("routing failed", job_id=job.id, error=str(e))
            fail_or_retry(conn, job.id, str(e), self._worker)
            return False

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
                if self._conn is not None:
                    try:
                        self._conn.close()
                    finally:
                        self._conn = None
                self._sleep()
                continue

            if not jobs:
                self._sleep()
                continue

            for job in jobs:
                if self._stop.is_set():
                    break
                self.process_job(job)

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
