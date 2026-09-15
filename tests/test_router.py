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
    """A connection that records the ORDER of what happened to it.

    `commits`/`rollbacks` counters alone let a mutation that commits each
    enqueue separately pass: the count is still one at the end. The sequence is
    what the atomicity claim is actually about.
    """

    def __init__(self) -> None:
        self.log: list[str] = []
        self.closes = 0
        self.closed = False

    def commit(self) -> None:
        self.log.append("commit")

    def rollback(self) -> None:
        self.log.append("rollback")

    def close(self) -> None:
        self.closes += 1
        self.closed = True

    @property
    def commits(self) -> int:
        return self.log.count("commit")

    @property
    def rollbacks(self) -> int:
        return self.log.count("rollback")


@pytest.fixture
def spy(monkeypatch):
    """Capture the queue primitives instead of touching Postgres.

    EVERY STUB HERE MATCHES THE REAL SIGNATURE, positionally. The suite this
    replaces passed for a year against a job shape the library never produced;
    a stub that accepts what the real function rejects is the same defect with
    a different face, so `test_the_stubs_match_the_library` checks these
    against `inspect.signature` rather than trusting this comment.
    """
    calls: dict[str, list] = {
        "enqueue": [],
        "complete": [],
        "fail": [],
        "release": [],
        "heal": [],
    }
    state: dict[str, Any] = {
        "complete_returns": True,
        "release_returns": True,
        "fail_returns": "pending",
        "heal_raises": None,
    }

    def fake_enqueue(
        conn, queue_name, payload, priority=0, dedup_key=None, commit=True
    ):
        conn.log.append(f"enqueue:{queue_name}")
        calls["enqueue"].append((queue_name, payload, dedup_key, commit, priority))
        return 1

    def fake_complete(conn, job_id, worker, commit=True):
        conn.log.append("complete")
        calls["complete"].append((job_id, worker, commit))
        return state["complete_returns"]

    def fake_fail(
        conn,
        job_id,
        error,
        worker,
        base_seconds=30,
        cap_seconds=900,
        jitter_ratio=0.2,
    ):
        calls["fail"].append((job_id, error))
        return state["fail_returns"]

    def fake_release(conn, job_id, delay_s, worker):
        calls["release"].append((job_id, delay_s))
        return state["release_returns"]

    class FakePartitionManager:
        def __init__(self, conn):
            self.conn = conn

        def create_future_partitions(self, days_ahead=3, dry_run=False, days_back=0):
            calls["heal"].append(days_ahead)
            if state["heal_raises"] is not None:
                raise state["heal_raises"]
            return 1

    monkeypatch.setattr(R, "enqueue", fake_enqueue)
    monkeypatch.setattr(R, "complete", fake_complete)
    monkeypatch.setattr(R, "fail_or_retry", fake_fail)
    monkeypatch.setattr(R, "release", fake_release)
    monkeypatch.setattr(R, "PartitionManager", FakePartitionManager)
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

    payload = calls["enqueue"][0][1]
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
        for s in ("success", "error", "skipped")
    }

    assert r.process_job(event("Cortex/Family/School/DPS")) == "unsettled"
    assert calls["release"] == []
    # Dropped, so the next claim reconnects; the claim's visibility timeout
    # returns the job with no attempt charged.
    assert r._conn is None
    # Counted `skipped` -- claimed, deliberately not processed -- and counted
    # exactly once, like every other disposition of a claimed job. Not `error`:
    # nothing failed about the event, and the row comes back uncharged.
    assert (
        counter("cortex_queue_processed", queue=R.SOURCE_QUEUE, status="skipped")
        == before["skipped"] + 1
    )
    for status in ("success", "error"):
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

    # The literal, spelled out rather than interpolated from DEDUP_KEY: an
    # assertion built from the same constant as the thing it checks passes
    # whatever that constant becomes.
    _, statement = R.DEDUP_INDEX
    assert "(queue_name, (payload->>'gmail_id'))" in statement
    assert "WHERE status IN ('pending', 'processing')" in statement
    assert R.DEDUP_KEY == "gmail_id"


def test_the_index_and_the_enqueue_agree_on_the_field(monkeypatch, spy):
    """The expression index is on a literal field name; the enqueue passes one
    as a parameter. Different files, and nothing but this connects them."""
    calls, _ = spy
    r = make_router(monkeypatch)
    r._route(FakeConn(), event("Cortex/Family/School/DPS"))

    assert {c[2] for c in calls["enqueue"]} == {R.DEDUP_KEY}
    # The field must be IN the payload we enqueue, or enqueue() raises
    # QueueError and the event is charged into dead_letter.
    assert all(R.DEDUP_KEY in c[1] for c in calls["enqueue"])
    assert "payload->>'gmail_id'" in R.DEDUP_INDEX[1]


def test_the_index_statement_cannot_be_anything_but_ddl():
    """DEDUP_INDEX is an f-string over DEDUP_KEY. That is fine while the key is
    a module constant and becomes injection the day someone makes it
    configurable, so pin the property rather than trusting the habit."""
    assert R.DEDUP_KEY.isidentifier()
    assert R.DEDUP_INDEX[0].isidentifier()
    assert R.DEDUP_INDEX[1].count(";") == 0


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


# -- the stubs themselves --------------------------------------------------


def test_the_stubs_match_the_library(spy):
    """The suite's own fixtures, checked against the functions they replace.

    A stub that accepts an argument the real function rejects (or orders its
    positionals differently) is exactly the defect this PR exists to fix, moved
    from the job shape into the fixture.
    """
    import inspect

    from cortex_utils import queue as Q

    for name, real in [
        ("enqueue", Q.enqueue),
        ("complete", Q.complete),
        ("fail_or_retry", Q.fail_or_retry),
        ("release", Q.release),
    ]:
        stub = getattr(R, name)
        assert [p.name for p in inspect.signature(stub).parameters.values()] == [
            p.name for p in inspect.signature(real).parameters.values()
        ], name


# -- the missing partition -------------------------------------------------


def test_a_missing_partition_is_healed_before_the_job_is_handed_back(monkeypatch, spy):
    """A release with nothing fixed is a livelock, not a recovery.

    enqueue(commit=False) gives up partition self-heal and ensure_queue_schema
    runs only at boot, so without this the same job comes back every poll
    interval forever at zero throughput.
    """
    calls, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(
        R,
        "enqueue",
        lambda *a, **k: (_ for _ in ()).throw(
            psycopg2.errors.CheckViolation("no partition for 2026-09-16")
        ),
    )

    assert r.process_job(event("Cortex/Family/School/DPS")) == "released"
    assert calls["heal"] == [3]
    assert calls["release"] == [(1, r.poll_interval)]
    assert calls["fail"] == []


def test_a_heal_that_fails_charges_the_attempt(monkeypatch, spy):
    """Releasing what we cannot make good on is the livelock again."""
    calls, state = spy
    state["heal_raises"] = RuntimeError("cannot create partition")
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(
        R,
        "enqueue",
        lambda *a, **k: (_ for _ in ()).throw(
            psycopg2.errors.CheckViolation("no partition for 2026-09-16")
        ),
    )

    assert r.process_job(event("Cortex/Family/School/DPS")) == "failed"
    assert calls["release"] == []
    assert calls["fail"]


# -- the connection ---------------------------------------------------------


def test_a_dead_connection_does_not_escape_process_job(monkeypatch, spy):
    """`_connect()` used to sit one line above the try.

    `_settle` drops a dead connection, so the next job reconnects -- and a
    Postgres still down raised OperationalError from outside every handler,
    out of process_job, out of run(), out of main(). The P0 this branch fixes,
    reintroduced by its own fix.
    """
    calls, _ = spy
    r = make_router(monkeypatch)

    def dead():
        raise psycopg2.OperationalError("could not connect to server")

    monkeypatch.setattr(r, "_connect", dead)
    before = counter("cortex_queue_processed", queue=R.SOURCE_QUEUE, status="skipped")

    assert r.process_job(event("Cortex/Family/School/DPS")) == "unsettled"
    assert calls["release"] == [] and calls["fail"] == []
    # Counted once, like every other disposition of a claimed job: there is no
    # connection to say anything on, so the visibility timeout returns it.
    assert (
        counter("cortex_queue_processed", queue=R.SOURCE_QUEUE, status="skipped")
        == before + 1
    )


def test_connect_reuses_a_live_connection_and_replaces_a_closed_one(monkeypatch):
    r = make_router(monkeypatch)
    made: list[FakeConn] = []

    def fake_connect(dsn):
        made.append(FakeConn())
        return made[-1]

    monkeypatch.setattr(R.psycopg2, "connect", fake_connect)

    first = r._connect()
    assert r._connect() is first  # reused, not reconnected per call
    first.closed = True
    assert r._connect() is not first
    assert len(made) == 2


def test_dropping_the_connection_actually_closes_it(monkeypatch):
    """Otherwise the backend leaks against the shared cortex-postgres."""
    r = make_router(monkeypatch)
    conn = FakeConn()
    r._conn = conn

    r._drop_conn()

    assert conn.closes == 1
    assert r._conn is None


# -- atomicity --------------------------------------------------------------


def test_nothing_commits_until_every_enqueue_has_been_issued(monkeypatch, spy):
    """The ORDER, not the count.

    `complete(commit=True)` leaves the count at one and passes a counter-based
    test -- while committing the enqueues before the claim is checked, so a
    lost claim leaves them durable and the downstream consumer gets the work
    twice: a second Discord message, a second vault note.
    """
    calls, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()

    r._route(conn, event("Cortex/Family/School/DPS"))

    assert conn.log == [
        "enqueue:school",
        "enqueue:family-archive",
        "complete",
        "commit",
    ]
    assert all(c[3] is False for c in calls["enqueue"])
    assert calls["complete"][0][2] is False


def test_a_lost_claim_while_settling_is_reported_as_lost(monkeypatch, spy):
    """release() returning False means someone else owns the job now.

    Counting our own outcome for their work is how one slow batch becomes a
    `failed` count nobody can trace back to a failure.
    """
    calls, state = spy
    state["release_returns"] = False
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(
        R,
        "enqueue",
        lambda *a, **k: (_ for _ in ()).throw(psycopg2.OperationalError("gone")),
    )

    assert r.process_job(event("Cortex/Family/School/DPS")) == "lost"


def test_a_stale_fail_report_is_reported_as_lost(monkeypatch, spy):
    calls, state = spy
    state["fail_returns"] = "stale"
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(
        R, "enqueue", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
    )

    assert r.process_job(event("Cortex/Family/School/DPS")) == "lost"


# -- the malformed-event guard ---------------------------------------------


@pytest.mark.parametrize(
    "payload,why",
    [
        ({"gmail_id": "abc", "event_type": "added"}, "no label at all"),
        ({"gmail_id": "abc", "label": 7, "event_type": "added"}, "label is not a str"),
        ({"gmail_id": None, "label": "Cortex/Family/School/DPS"}, "gmail_id is null"),
        (
            {"gmail_id": True, "label": "Cortex/Family/School/DPS"},
            "bool: jsonb says 'true'",
        ),
        (
            {"gmail_id": 1.5, "label": "Cortex/Family/School/DPS"},
            "float does not dedup",
        ),
        ({"gmail_id": ["a"], "label": "Cortex/Family/School/DPS"}, "container"),
        (
            {
                "gmail_id": "abc",
                "label": "Cortex/Family/School/DPS",
                "event_type": ["added"],
            },
            "event_type unhashable: TypeError out of matches()",
        ),
    ],
)
def test_unfixable_rows_are_dropped_rather_than_charged(monkeypatch, spy, payload, why):
    """Every one of these raises inside enqueue() or matches() if it gets past
    the guard, which the handler turns into fail_or_retry and three attempts
    later into dead_letter -- burying it among the real failures."""
    calls, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)

    assert r.process_job(job(payload)) == "dropped", why
    assert calls["fail"] == []
    assert calls["enqueue"] == []


@pytest.mark.parametrize("gmail_id", ["abc123", 12345])
def test_the_ids_the_queue_accepts_are_not_dropped(monkeypatch, spy, gmail_id):
    """The direction that must still be ACCEPTED.

    `is_dedup_value` rather than isinstance(str): the queue dedups on int too,
    and a stricter second copy of someone else's rule drops rows they would
    have taken.
    """
    calls, _ = spy
    r = make_router(monkeypatch)

    outcome = r._route(
        FakeConn(),
        job({"gmail_id": gmail_id, "label": "Cortex/Family/School/DPS"}),
    )

    assert outcome == "routed"
    assert calls["enqueue"][0][1]["gmail_id"] == gmail_id


def test_a_non_dict_payload_is_dropped_with_its_evidence_intact(monkeypatch, spy):
    """The drop is irreversible, so the log is the only record of what was in
    the row. Logging the substituted `{}` erases exactly the case it is for."""
    calls, _ = spy
    r = make_router(monkeypatch)
    lines: list[tuple[str, dict]] = []

    class Recorder:
        def error(self, event, **kw):
            lines.append((event, kw))

        def __getattr__(self, _name):
            return lambda *a, **kw: None

    monkeypatch.setattr(R, "logger", Recorder())

    assert r._route(FakeConn(), job(["not", "a", "dict"])) == "dropped"

    dropped = [kw for name, kw in lines if name == "dropping malformed action event"]
    assert dropped and dropped[0]["payload"] == ["not", "a", "dict"]


def test_the_destination_never_comes_from_the_event(monkeypatch, spy):
    """The barrier, in this service's terms: an email's own label may not
    choose the queue it is dispatched to. A payload naming a queue must be
    ignored, not honoured."""
    calls, _ = spy
    r = make_router(monkeypatch)

    r._route(
        FakeConn(),
        job(
            {
                "gmail_id": "abc",
                "label": "Cortex/Automated/Social/X",
                "event_type": "added",
                "queue": "school",
                "target": "school",
            }
        ),
    )

    assert calls["enqueue"] == []


def test_the_producers_priority_is_carried_downstream(monkeypatch, spy):
    """-100 is the backfill convention. Dropping it puts an 82k historical
    sweep in front of the mail that arrived this morning."""
    calls, _ = spy
    r = make_router(monkeypatch)

    backfill = event("Cortex/Family/School/DPS")
    backfill["priority"] = -100
    r._route(FakeConn(), backfill)

    assert {c[4] for c in calls["enqueue"]} == {-100}


def test_a_nonsense_priority_falls_back_to_the_default(monkeypatch, spy):
    calls, _ = spy
    r = make_router(monkeypatch)

    weird = event("Cortex/Family/School/DPS")
    weird["priority"] = "high"
    r._route(FakeConn(), weird)

    assert {c[4] for c in calls["enqueue"]} == {0}


# -- the loop ---------------------------------------------------------------


def drive(monkeypatch, r, batches, conn=None):
    """Run run() over a fixed list of batches, then stop. Returns the claims."""
    conn = conn or FakeConn()
    claims: list[dict] = []
    remaining = list(batches)
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(R, "ensure_queue_schema", lambda c, extra_indexes=(): None)

    def fake_claim(conn, queue_name, worker, limit, visibility_timeout_min):
        claims.append(
            {
                "queue": queue_name,
                "worker": worker,
                "limit": limit,
                "visibility": visibility_timeout_min,
            }
        )
        batch = remaining.pop(0)
        if not remaining:
            r._stop.set()
        return batch

    monkeypatch.setattr(R, "claim", fake_claim)
    r.run()
    return claims


def test_claim_asks_for_what_the_router_was_configured_with(monkeypatch, spy):
    """The visibility timeout is the deadline `unsettled` depends on, and the
    batch size is the throughput. Neither was asserted anywhere."""
    r = make_router(monkeypatch)
    claims = drive(monkeypatch, r, [[event("Cortex/Family/School/DPS")], []])

    assert claims[0]["queue"] == R.SOURCE_QUEUE
    assert claims[0]["limit"] == r.batch_size == 10
    assert claims[0]["visibility"] == 5
    assert claims[0]["worker"] == r._worker and r._worker


def test_a_batch_that_settles_nothing_backs_off(monkeypatch, spy):
    """THE SPIN. released/lost/unsettled all hand the job back uncharged, so
    the next claim returns the same rows. Sleeping only on an empty batch meant
    a wholly-released batch ran at database round-trip speed against the table
    every other cortex service claims from."""
    _, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(
        R,
        "enqueue",
        lambda *a, **k: (_ for _ in ()).throw(psycopg2.OperationalError("gone")),
    )
    slept: list[int] = []
    monkeypatch.setattr(r, "_sleep", lambda: slept.append(1))

    drive(monkeypatch, r, [[event("Cortex/Family/School/DPS")] * 3, []], conn=conn)

    # Once for the released batch, once for the empty one that follows.
    assert len(slept) == 2


def test_a_batch_that_settles_work_does_not_back_off(monkeypatch, spy):
    """The direction that must still work: a draining queue must not sleep
    between batches, or the 82k sweep takes a poll interval per batch."""
    _, _ = spy
    r = make_router(monkeypatch)
    slept: list[int] = []
    monkeypatch.setattr(r, "_sleep", lambda: slept.append(1))

    drive(monkeypatch, r, [[event("Cortex/Family/School/DPS")], []])

    assert len(slept) == 1  # only the empty batch


def test_run_creates_the_schema_before_it_claims(monkeypatch, spy):
    """DEDUP_INDEX exists because of this call. Nothing tied it to run()."""
    r = make_router(monkeypatch)
    seen: list = []
    monkeypatch.setattr(r, "_connect", lambda: FakeConn())
    monkeypatch.setattr(
        R,
        "ensure_queue_schema",
        lambda c, extra_indexes=(): seen.append(list(extra_indexes)),
    )
    monkeypatch.setattr(R, "claim", lambda *a, **k: (r._stop.set(), [])[1])
    r.run()

    assert seen == [[R.DEDUP_INDEX]]


def test_a_dead_database_does_not_kill_the_loop(monkeypatch, spy):
    """claim() raising must drop the connection and sleep, not re-raise.

    A reused dead connection means a router that logs nothing and routes
    nothing while its container stays green.
    """
    r = make_router(monkeypatch)
    conn = FakeConn()
    r._conn = conn
    monkeypatch.setattr(R, "ensure_queue_schema", lambda c, extra_indexes=(): None)
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(r, "_sleep", lambda: r._stop.set())

    def dead_claim(*a, **k):
        raise psycopg2.OperationalError("server closed the connection")

    monkeypatch.setattr(R, "claim", dead_claim)
    r.run()  # must return, not raise

    assert conn.closes == 1 and r._conn is None


def test_a_stop_mid_batch_leaves_the_rest_of_the_batch_alone(monkeypatch, spy):
    """SIGTERM during a batch: the unprocessed rows keep their claim and come
    back on the visibility timeout, rather than being raced to the exit."""
    calls, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(R, "ensure_queue_schema", lambda c, extra_indexes=(): None)

    real = r.process_job

    def stop_after_one(j):
        out = real(j)
        r._stop.set()
        return out

    monkeypatch.setattr(r, "process_job", stop_after_one)
    monkeypatch.setattr(
        R, "claim", lambda *a, **k: [event("Cortex/Family/School/DPS")] * 5
    )
    r.run()

    assert len(calls["complete"]) == 1


# -- reload -----------------------------------------------------------------


def test_a_good_reload_replaces_the_table(monkeypatch, spy):
    """The other half of the refusal test. A reload that validates and then
    discards its result is a config file nobody can change."""
    r = make_router(monkeypatch)
    assert len(r.subscriptions) == 2

    new = parse({"subscriptions": [{"label": "Cortex/Other/*", "queue": "other"}]})
    monkeypatch.setattr(R, "load", lambda path: new)
    r.reload()

    assert [s.queue for s in r.subscriptions] == ["other"]
    assert r._route(FakeConn(), event("Cortex/Other/Thing")) == "routed"


def test_run_acts_on_a_requested_reload(monkeypatch, spy):
    """request_reload() sets a flag; something has to read it."""
    r = make_router(monkeypatch)
    loads: list[int] = []
    monkeypatch.setattr(R, "load", lambda path: (loads.append(1), SUBS)[1])
    r.request_reload()

    drive(monkeypatch, r, [[]])

    assert loads == [1]
    assert not r._reload_requested.is_set()


def test_stop_and_reload_flags_are_wired(monkeypatch):
    r = make_router(monkeypatch)
    assert not r._stop.is_set()
    r.stop()
    assert r._stop.is_set()

    assert not r._reload_requested.is_set()
    r.request_reload()
    assert r._reload_requested.is_set()


def test_sleep_wakes_for_stop_and_for_reload(monkeypatch):
    """A SIGTERM must not wait out a poll interval, and neither must a SIGHUP."""
    import time as time_mod

    r = make_router(monkeypatch)
    r.poll_interval = 30
    slept: list[float] = []
    monkeypatch.setattr(time_mod, "sleep", lambda s: slept.append(s))
    monkeypatch.setattr(R.time, "sleep", lambda s: slept.append(s))

    r._stop.set()
    r._sleep()
    assert slept == []

    r._stop.clear()
    r._reload_requested.set()
    r._sleep()
    assert slept == []


# -- main() -----------------------------------------------------------------


def test_main_refuses_to_start_without_its_environment(monkeypatch):
    """A router that boots with no password connects to nothing, forever."""
    for v in ("POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"):
        monkeypatch.delenv(v, raising=False)

    with pytest.raises(SystemExit) as e:
        R.main()

    assert "POSTGRES_HOST" in str(e.value)


def test_main_refuses_to_start_on_a_bad_routing_table(monkeypatch):
    """Fatal at boot, unlike a reload: there is no previous good table."""
    for v in ("POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"):
        monkeypatch.setenv(v, "x")
    monkeypatch.setattr(
        R, "load", lambda path: (_ for _ in ()).throw(R.SubscriptionError("empty"))
    )

    with pytest.raises(SystemExit) as e:
        R.main()

    assert "cannot start" in str(e.value)


def test_main_installs_the_signal_handlers(monkeypatch):
    """SIGTERM must stop the router and SIGHUP must reload it -- swapping them
    means a config reload kills the service, and a redeploy reloads config and
    then gets SIGKILLed by Docker."""
    import signal as signal_mod

    for v in ("POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"):
        monkeypatch.setenv(v, "x")
    monkeypatch.setattr(R, "load", lambda path: SUBS)
    monkeypatch.setattr(R, "start_metrics_server", lambda port: None)

    handlers: dict[int, Any] = {}
    monkeypatch.setattr(
        R.signal, "signal", lambda sig, fn: handlers.__setitem__(sig, fn)
    )

    captured: list[R.Router] = []
    monkeypatch.setattr(R.Router, "run", lambda self: captured.append(self))
    R.main()

    router = captured[0]
    assert set(handlers) == {
        signal_mod.SIGTERM,
        signal_mod.SIGINT,
        signal_mod.SIGHUP,
    }

    handlers[signal_mod.SIGHUP](signal_mod.SIGHUP, None)
    assert router._reload_requested.is_set() and not router._stop.is_set()

    handlers[signal_mod.SIGTERM](signal_mod.SIGTERM, None)
    assert router._stop.is_set()


# -- the vocabulary ---------------------------------------------------------


def test_every_outcome_has_a_fleet_status_and_a_place_in_the_summary():
    """An outcome missing from OUTCOMES vanishes from the batch line CLAUDE.md
    tells you to watch; one missing from _STATUS raises KeyError inside the
    settle path, from a branch only an outage reaches."""
    assert set(R.OUTCOMES) == set(R._STATUS)
    assert set(R.PROGRESS) <= set(R.OUTCOMES)
    # The fleet vocabulary, not this service's own words: a `status` value the
    # other workers never emit makes this queue invisible to any expression
    # written across queues.
    assert set(R._STATUS.values()) == {"success", "error", "skipped"}
    assert R._STATUS["failed"] == "error"
    assert set(R._ERROR_TYPE.values()) == {"never_started", "job_failed"}


@pytest.mark.parametrize(
    "outcome",
    ["routed", "unmatched", "dropped", "failed", "released", "lost", "unsettled"],
)
def test_each_outcome_counts_exactly_one_claimed_job(monkeypatch, spy, outcome):
    """Every claimed job increments cortex_queue_processed_total exactly once.
    Three settle paths used to carry their own copy of that decision and only
    one of them did it, so the metric saw a third of the traffic."""
    _, _ = spy
    before = sum(
        counter("cortex_queue_processed", queue=R.SOURCE_QUEUE, status=st)
        for st in ("success", "error", "skipped")
    )

    R.Router._counted(outcome)

    after = sum(
        counter("cortex_queue_processed", queue=R.SOURCE_QUEUE, status=st)
        for st in ("success", "error", "skipped")
    )
    assert after == before + 1


def test_a_row_with_no_payload_at_all_is_dropped(monkeypatch, spy):
    """`payload` is NOT NULL in the queue table, so this is defensive -- but
    the defence is one `.get`, and without it the KeyError lands in the generic
    handler and charges three attempts into dead_letter for a row that could
    never have been routed."""
    calls, _ = spy
    r = make_router(monkeypatch)
    row = job({})
    del row["payload"]

    assert r._route(FakeConn(), row) == "dropped"
    assert calls["fail"] == []
