"""Router behaviour, with the queue library stubbed.

What matters here is not the SQL -- cortex_utils.queue owns and tests that --
but the four decisions this service makes: route, drop silently, drop loudly,
and give the claim back. Each has a different failure cost.
"""

import datetime
from typing import Any

import psycopg2
import pytest

from actions.services import router as R
from actions.subscriptions import parse

SUBS = parse(
    {
        "subscriptions": [
            {"label": "Cortex/Family/School/*", "queue": "school"},
            {"label": "Cortex/Family/*", "queue": "family-archive"},
        ]
    }
)


class FakeConn:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0
        self.closed = 0

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


@pytest.fixture
def spy(monkeypatch):
    """Capture enqueue/complete/fail calls instead of touching Postgres."""
    calls: dict[str, list] = {
        "enqueue": [],
        "complete": [],
        "fail": [],
        "release": [],
    }
    state = {"complete_returns": True}

    def fake_enqueue(conn, queue_name, payload, dedup_key=None, commit=True, **kw):
        calls["enqueue"].append((queue_name, payload, dedup_key, commit))
        return 1

    def fake_complete(conn, job_id, worker, commit=True):
        calls["complete"].append((job_id, worker, commit))
        return state["complete_returns"]

    def fake_fail(conn, job_id, error, worker):
        calls["fail"].append((job_id, error))
        return "pending"

    def fake_release(conn, job_id, delay_s, worker):
        calls["release"].append((job_id, delay_s))
        return True

    monkeypatch.setattr(R, "enqueue", fake_enqueue)
    monkeypatch.setattr(R, "complete", fake_complete)
    monkeypatch.setattr(R, "fail_or_retry", fake_fail)
    monkeypatch.setattr(R, "release", fake_release)
    return calls, state


def make_router(monkeypatch, subs=SUBS) -> R.Router:
    monkeypatch.setattr(R, "load", lambda path: subs)
    r = R.Router(dsn="", subs_path="ignored", batch_size=10, poll_interval=1)
    return r


def job(payload: dict[str, Any], job_id: int = 1) -> dict[str, Any]:
    """A row exactly as `cortex_utils.queue.claim` returns it.

    THE SHAPE IS THE POINT. The first version of this helper was a dataclass
    with `.id` and `.payload` attributes -- a shape claim() has never produced.
    Every router test passed against it while the service crashed on its first
    real job, because the handler for the AttributeError raised the same
    AttributeError and took the process with it. Keys here are copied from
    claim()'s RETURNING clause; nothing in the suite may invent a field.
    """
    return {
        "id": job_id,
        "queue_name": R.SOURCE_QUEUE,
        "payload": payload,
        "attempts": 0,
        "priority": 0,
        "created_at": datetime.datetime(2026, 9, 15, 12, 0),
    }


def event(label: str, event_type: str = "added", gmail_id: str = "abc123"):
    return job({"gmail_id": gmail_id, "label": label, "event_type": event_type})


def test_routes_to_every_matching_queue_in_one_transaction(monkeypatch, spy):
    calls, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()

    assert r._route(conn, event("Cortex/Family/School/DPS")) == "routed"

    queues = [c[0] for c in calls["enqueue"]]
    assert queues == ["school", "family-archive"]
    # Every enqueue defers its commit so the whole job settles atomically.
    assert all(c[3] is False for c in calls["enqueue"])
    assert all(c[2] == "gmail_id" for c in calls["enqueue"])
    assert conn.commits == 1
    assert conn.rollbacks == 0


def test_payload_carries_label_and_event_type_through(monkeypatch, spy):
    calls, _ = spy
    r = make_router(monkeypatch)
    r._route(FakeConn(), event("Cortex/Family/School/DPS", gmail_id="deadbeef"))

    _, payload, _, _ = calls["enqueue"][0]
    assert payload == {
        "gmail_id": "deadbeef",
        "label": "Cortex/Family/School/DPS",
        "event_type": "added",
    }


def test_unmatched_label_completes_and_never_fails(monkeypatch, spy):
    """The common case. Failing here would fill dead_letter with healthy events."""
    calls, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()

    assert r._route(conn, event("Cortex/Automated/Social/Nextdoor")) == "unmatched"

    assert calls["enqueue"] == []
    assert calls["fail"] == []
    assert len(calls["complete"]) == 1
    assert conn.commits == 1


def test_removed_event_is_not_routed_unless_opted_in(monkeypatch, spy):
    calls, _ = spy
    r = make_router(monkeypatch)
    assert (
        r._route(FakeConn(), event("Cortex/Family/School/DPS", "removed"))
        == "unmatched"
    )
    assert calls["enqueue"] == []


def test_removed_event_routes_when_opted_in(monkeypatch, spy):
    calls, _ = spy
    subs = parse(
        {
            "subscriptions": [
                {
                    "label": "Cortex/Family/School/*",
                    "queue": "school",
                    "events": ["added", "removed"],
                }
            ]
        }
    )
    r = make_router(monkeypatch, subs)
    r._route(FakeConn(), event("Cortex/Family/School/DPS", "removed"))
    assert [c[0] for c in calls["enqueue"]] == ["school"]
    assert calls["enqueue"][0][1]["event_type"] == "removed"


def test_lost_claim_rolls_back_the_enqueues(monkeypatch, spy):
    """Whoever holds the claim now will redo them; ours must not survive."""
    calls, state = spy
    state["complete_returns"] = False
    r = make_router(monkeypatch)
    conn = FakeConn()

    assert r._route(conn, event("Cortex/Family/School/DPS")) == "lost"
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_malformed_event_is_dropped_not_retried(monkeypatch, spy):
    """A row with no label cannot become well-formed by being retried."""
    calls, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()

    assert r._route(conn, job({"gmail_id": "abc"})) == "dropped"
    assert calls["fail"] == []
    assert len(calls["complete"]) == 1


def test_unexpected_error_charges_an_attempt(monkeypatch, spy):
    calls, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(
        R, "enqueue", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
    )

    assert r.process_job(event("Cortex/Family/School/DPS")) == "failed"
    assert calls["fail"] and "boom" in calls["fail"][0][1]
    assert conn.rollbacks == 1


def test_bad_reload_keeps_the_running_table(monkeypatch):
    """A typo that empties the table is a silent outage; refusing is a loud one."""
    r = make_router(monkeypatch)
    assert len(r.subscriptions) == 2

    def boom(path):
        raise R.SubscriptionError("bad yaml")

    monkeypatch.setattr(R, "load", boom)
    r.reload()

    assert len(r.subscriptions) == 2


def test_label_prefix_bounds_metric_cardinality():
    assert R._label_prefix("Cortex/Family/School/DPS") == "Cortex/Family"
    assert R._label_prefix("Cortex") == "Cortex"


# -- the job shape ---------------------------------------------------------


def test_the_job_helper_matches_what_claim_actually_returns():
    """The fiction this suite used to assert against, asserted against reality.

    Ten tests passed while the router read `job.payload` on a dict. Nothing in
    the suite could see it, because the suite defined the shape it tested.
    """
    import inspect
    import re

    from cortex_utils.queue import claim

    src = inspect.getsource(claim)
    returning = src[src.index("RETURNING") : src.index('"""', src.index("RETURNING"))]
    columns = set(re.findall(r"\bq\.(\w+)", returning))

    assert columns, "could not read claim()'s RETURNING clause; check this parse"
    assert set(job({}).keys()) == columns


def test_run_routes_a_real_claim_row(monkeypatch, spy):
    """End to end through run(), on the shape the library hands us.

    This is the test that was missing. `_route` was always called directly, so
    nothing ever exercised the path a deployed router takes.
    """
    calls, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(R, "ensure_queue_schema", lambda c, extra_indexes=(): None)

    batches = [[event("Cortex/Family/School/DPS")], []]

    def fake_claim(conn, queue_name, worker, limit, visibility_timeout_min):
        batch = batches.pop(0)
        if not batches:
            r._stop.set()
        return batch

    monkeypatch.setattr(R, "claim", fake_claim)
    r.run()

    assert [c[0] for c in calls["enqueue"]] == ["school", "family-archive"]
    assert len(calls["complete"]) == 1
    assert calls["fail"] == []


# -- started vs not started ------------------------------------------------


@pytest.mark.parametrize(
    "exc",
    [
        psycopg2.OperationalError("server closed the connection"),
        psycopg2.InterfaceError("connection already closed"),
        psycopg2.errors.CheckViolation("no partition for 2026-09-16"),
    ],
)
def test_infrastructure_failure_releases_and_charges_nothing(monkeypatch, spy, exc):
    """The work never started, so the event keeps its whole attempt budget.

    Charging these is how healthy work reaches dead_letter: three attempts,
    30s and 60s apart, means ninety seconds of Postgres being away retires
    every school event in flight.
    """
    calls, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(R, "enqueue", lambda *a, **k: (_ for _ in ()).throw(exc))

    assert r.process_job(event("Cortex/Family/School/DPS")) == "released"
    assert calls["release"] == [(1, r.poll_interval)]
    assert calls["fail"] == []
    assert conn.rollbacks == 1


def test_a_fault_in_the_event_still_charges_an_attempt(monkeypatch, spy):
    """The other direction. Released forever is as bad as retired at once."""
    calls, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(
        R, "enqueue", lambda *a, **k: (_ for _ in ()).throw(TypeError("bad payload"))
    )

    assert r.process_job(event("Cortex/Family/School/DPS")) == "failed"
    assert calls["fail"] and "bad payload" in calls["fail"][0][1]
    assert calls["release"] == []


# -- the handler must not raise --------------------------------------------


def test_a_settle_that_fails_does_not_kill_the_loop(monkeypatch, spy):
    """When Postgres is gone, rollback() and release() are gone with it.

    The original P0 was a handler that raised the error it was handling, which
    escaped run() and exited the process. Same shape, one level down.
    """
    calls, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()

    def dead_rollback():
        raise psycopg2.InterfaceError("connection already closed")

    conn.rollback = dead_rollback  # type: ignore[method-assign]
    r._conn = conn
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(
        R,
        "enqueue",
        lambda *a, **k: (_ for _ in ()).throw(psycopg2.OperationalError("gone")),
    )
    before = {
        s: counter("cortex_queue_processed", queue=R.SOURCE_QUEUE, status=s)
        for s in ("success", "released", "failed", "lost")
    }

    assert r.process_job(event("Cortex/Family/School/DPS")) == "unsettled"
    assert calls["release"] == []
    # Dropped, so the next claim reconnects; the claim's visibility timeout
    # returns the job with no attempt charged.
    assert r._conn is None
    # And nothing was processed, so nothing is counted as processed. The row is
    # still `processing` in the queue.
    for status in ("success", "released", "failed", "lost"):
        assert (
            counter("cortex_queue_processed", queue=R.SOURCE_QUEUE, status=status)
            == before[status]
        )


# -- what the counters say -------------------------------------------------


def counter(name: str, **labels: str) -> float:
    """Read a counter through the public registry API, defaulting to 0."""
    from prometheus_client import REGISTRY

    return REGISTRY.get_sample_value(name + "_total", labels) or 0.0


def test_a_suppressed_enqueue_is_not_counted_as_routed(monkeypatch, spy):
    """enqueue() returns None when the work is already queued. That is not a
    new job, and counting it as one would inflate the number we intend to read
    before switching the producer on."""
    _, _ = spy
    r = make_router(monkeypatch)
    monkeypatch.setattr(R, "enqueue", lambda *a, **k: None)

    before_r = counter(
        "cortex_actions_routed", label_prefix="Cortex/Family", queue="school"
    )
    before_s = counter(
        "cortex_actions_suppressed", label_prefix="Cortex/Family", queue="school"
    )

    assert r._route(FakeConn(), event("Cortex/Family/School/DPS")) == "routed"

    assert (
        counter("cortex_actions_routed", label_prefix="Cortex/Family", queue="school")
        == before_r
    )
    assert (
        counter(
            "cortex_actions_suppressed", label_prefix="Cortex/Family", queue="school"
        )
        == before_s + 1
    )


@pytest.mark.parametrize(
    "label,expected",
    [
        ("Cortex/Family/School/DPS", "routed"),
        ("Cortex/Automated/Social/X", "unmatched"),
    ],
)
def test_every_settled_path_counts_the_job(monkeypatch, spy, label, expected):
    """The unmatched path IS the traffic. It used to count nothing at all."""
    _, _ = spy
    r = make_router(monkeypatch)
    before = counter("cortex_queue_processed", queue=R.SOURCE_QUEUE, status="success")

    assert r._route(FakeConn(), event(label)) == expected

    assert (
        counter("cortex_queue_processed", queue=R.SOURCE_QUEUE, status="success")
        == before + 1
    )


def test_outcomes_are_a_closed_set(monkeypatch, spy):
    """run()'s batch summary reads OUTCOMES; an outcome missing from it would
    vanish from the only line that shows the steady state."""
    calls, state = spy
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)

    seen = {
        r._route(FakeConn(), event("Cortex/Family/School/DPS")),
        r._route(FakeConn(), event("Cortex/Automated/Social/X")),
        r._route(FakeConn(), job({"gmail_id": "abc"})),
        r.process_job(event("Cortex/Family/School/DPS")),
    }
    state["complete_returns"] = False
    seen.add(r._route(FakeConn(), event("Cortex/Family/School/DPS")))

    assert seen == {"routed", "unmatched", "dropped", "lost"}
    assert seen <= set(R.OUTCOMES)


def test_every_batch_logs_a_summary_at_info(monkeypatch, spy):
    """Otherwise a steady state of unmatched events is indistinguishable at
    INFO from a router that is settling nothing at all: `routed` is INFO but
    rare, `unmatched` is the traffic but DEBUG."""
    _, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(R, "ensure_queue_schema", lambda c, extra_indexes=(): None)

    lines: list[tuple[str, dict]] = []

    class Recorder:
        def info(self, event, **kw):
            lines.append((event, kw))

        def __getattr__(self, _name):
            return lambda *a, **kw: None

    monkeypatch.setattr(R, "logger", Recorder())

    batches = [
        [event("Cortex/Automated/Social/X"), event("Cortex/Family/School/DPS")],
        [],
    ]

    def fake_claim(conn, queue_name, worker, limit, visibility_timeout_min):
        batch = batches.pop(0)
        if not batches:
            r._stop.set()
        return batch

    monkeypatch.setattr(R, "claim", fake_claim)
    r.run()

    summary = [kw for name, kw in lines if name == "batch"]
    assert summary == [{"claimed": 2, "routed": 1, "unmatched": 1}]


# -- the dedup index -------------------------------------------------------


def test_the_dedup_index_matches_the_query_it_exists_for():
    """Tie the index to enqueue()'s actual dedup predicate, not to a string.

    An index whose expression or predicate does not match the query is not a
    slower index, it is no index -- and nothing in a green suite or a healthy
    container would say so. This reads the predicate out of the library.
    """
    import inspect

    from cortex_utils.queue import ops

    src = inspect.getsource(ops._insert)
    start = src.index("WHERE NOT EXISTS")
    where = src[start : src.index("RETURNING id", start)]

    assert "queue_name = %s" in where
    assert "payload->>%s = %s" in where
    assert "status IN ('pending', 'processing')" in where

    _, statement = R.DEDUP_INDEX
    assert "(queue_name, (payload->>'gmail_id'))" in statement
    assert "WHERE status IN ('pending', 'processing')" in statement


def test_the_index_and_the_enqueue_agree_on_the_field(monkeypatch, spy):
    """The expression index is on a literal field name; the enqueue passes one
    as a parameter. Different files, and nothing but this connects them."""
    calls, _ = spy
    r = make_router(monkeypatch)
    r._route(FakeConn(), event("Cortex/Family/School/DPS"))

    assert {c[2] for c in calls["enqueue"]} == {R.DEDUP_KEY}
    assert f"payload->>'{R.DEDUP_KEY}'" in R.DEDUP_INDEX[1]


def test_schema_setup_asks_for_the_dedup_index(monkeypatch, spy):
    """It is created on startup or it is never created at all."""
    r = make_router(monkeypatch)
    monkeypatch.setattr(r, "_connect", lambda: FakeConn())
    seen: list = []
    monkeypatch.setattr(
        R,
        "ensure_queue_schema",
        lambda conn, extra_indexes=(): seen.append(extra_indexes),
    )

    r._ensure_schema()

    assert seen == [[R.DEDUP_INDEX]]
