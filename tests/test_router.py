"""Router behaviour, with the queue library stubbed.

What matters here is not the SQL -- cortex_utils.queue owns and tests that --
but what each outcome COSTS THE EVENT. `routed`, `unmatched` and `dropped`
settle it; `failed` charges an attempt toward dead_letter; `released`, `lost`
and `unsettled` hand it back uncharged. Getting that wrong in either direction
retires healthy work or re-queues broken work forever.

Every stub here matches the real signature AND defaults of what it replaces --
see `test_the_stubs_match_the_library`. The suite this grew out of passed
against a job shape `claim()` has never returned.
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
        # Postgres's rule, not a positional one: after a statement fails, every
        # later statement on the connection raises InFailedSqlTransaction until
        # a rollback. Modelling it is what makes `_set_lock_timeout`'s rollback
        # load-bearing in the suite -- deleting that line makes every heal fail
        # with "current transaction is aborted", forever, which is what a real
        # Postgres does. An earlier version asserted "the last logged op was a
        # rollback", which was both too strict (a SET may legitimately follow
        # it) and too loose (an empty log passed).
        self.aborted = False

    def commit(self) -> None:
        self.log.append("commit")

    def rollback(self) -> None:
        self.log.append("rollback")
        self.aborted = False

    def close(self) -> None:
        self.closes += 1
        self.closed = True

    def cursor(self) -> Any:
        conn = self

        class Cur:
            def __enter__(self) -> Any:
                return self

            def __exit__(self, *a: Any) -> None:
                return None

            def execute(self, sql: str, args: Any = None) -> None:
                # No aborted check. It has been added and removed three times
                # on argument; this time it was settled by running the two
                # mutations it could plausibly catch, with and without it:
                # deleting `_set_lock_timeout`'s rollback, and moving that
                # rollback after its own statement. Both die either way -- the
                # first on FakePartitionManager's precondition, the second on
                # this test's explicit ordering assertion. A check that catches
                # nothing the suite does not already catch is the defect this
                # branch is about, in a test double.
                conn.log.append(f"exec:{sql}:{args}")

        return Cur()

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
        "heal_creates": 1,
    }

    def fake_enqueue(
        conn, queue_name, payload, priority=0, dedup_key=None, commit=True
    ):
        conn.log.append(f"enqueue:{queue_name}")
        # The real one commits when asked. A fake that ignores the flag makes
        # the atomicity assertion rest on the flag alone, and the ordering log
        # -- the thing that is supposed to catch a commit in the wrong place --
        # cannot see the mutation its own docstring names.
        if commit:
            conn.commit()
        calls["enqueue"].append((queue_name, payload, dedup_key, commit, priority))
        return 1

    def fake_complete(conn, job_id, worker, commit=True):
        conn.log.append("complete")
        if commit:
            conn.commit()
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
        """Stands in for the real one, including its preconditions.

        The real `create_partition` runs bare cursors and commits. A
        CheckViolation has aborted the transaction, so without the caller's
        rollback first the real thing raises InFailedSqlTransaction and every
        heal 'fails' -- which is why this fake refuses to work if the rollback
        did not happen. A fake with no preconditions made that rollback
        deletable with the suite green.

        The lock-timeout test now asserts that ordering explicitly too, so
        deleting this check alone is invisible. Kept deliberately: it is what
        makes any FUTURE heal test enforce the precondition without having to
        remember it. Modelling a real constraint in a double is not the same
        thing as a production guard that cannot fire.
        """

        def __init__(self, conn):
            self.conn = conn

        def create_future_partitions(self, days_ahead=3, dry_run=False, days_back=0):
            if self.conn.aborted:
                raise psycopg2.errors.InFailedSqlTransaction(
                    "current transaction is aborted"
                )
            calls["heal"].append(days_ahead)
            # The DDL leaves a footprint, so a test can assert the timeout was
            # set BEFORE it rather than merely somewhere in the log -- moving
            # the SET after the DDL passed every assertion that did not.
            self.conn.log.append("DDL")
            if state["heal_raises"] is not None:
                # A failed CREATE TABLE aborts the transaction. create_partition
                # only rolls back for DuplicateTable, so every other DDL error
                # leaves it aborted for the caller.
                self.conn.aborted = True
                raise state["heal_raises"]
            return state["heal_creates"]

    monkeypatch.setattr(R, "enqueue", fake_enqueue)
    monkeypatch.setattr(R, "complete", fake_complete)
    monkeypatch.setattr(R, "fail_or_retry", fake_fail)
    monkeypatch.setattr(R, "release", fake_release)
    monkeypatch.setattr(R, "PartitionManager", FakePartitionManager)
    return calls, state


def raising_enqueue(exc):
    """An enqueue that fails the way a real one does: the statement aborts the
    transaction, so everything after it raises until someone rolls back."""

    def _raise(conn, *a, **k):
        conn.aborted = True
        raise exc

    return _raise


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
    monkeypatch.setattr(R, "enqueue", raising_enqueue(RuntimeError("boom")))

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
    monkeypatch.setattr(R, "enqueue", raising_enqueue(exc))

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
    monkeypatch.setattr(R, "enqueue", raising_enqueue(TypeError("bad payload")))

    assert r.process_job(event("Cortex/Family/School/DPS")) == "failed"
    assert calls["fail"] and "bad payload" in calls["fail"][0][1]
    assert calls["release"] == []


# -- the handler must not raise --------------------------------------------


@pytest.mark.parametrize(
    "settle_error",
    [
        psycopg2.InterfaceError("connection already closed"),
        psycopg2.OperationalError("server closed the connection"),
        psycopg2.errors.InFailedSqlTransaction("current transaction is aborted"),
        RuntimeError("something nobody predicted"),
    ],
)
def test_a_settle_that_fails_does_not_kill_the_loop(monkeypatch, spy, settle_error):
    """When Postgres is gone, rollback() and release() are gone with it.

    The original P0 was a handler that raised the error it was handling, which
    escaped run() and exited the process. Same shape, one level down.
    """
    calls, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()

    def dead_rollback():
        raise settle_error

    conn.rollback = dead_rollback  # type: ignore[method-assign]
    r._conn = conn
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(
        R,
        "enqueue",
        raising_enqueue(psycopg2.OperationalError("gone")),
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
        ("claim", Q.claim),
        ("ensure_queue_schema", Q.ensure_queue_schema),
        ("PartitionManager", Q.PartitionManager),
    ]:
        stub = getattr(R, name)
        if stub is real:
            continue  # not stubbed in this test's fixture
        if isinstance(real, type):
            # A class stub: compare the method the router actually calls.
            ours = list(
                inspect.signature(stub.create_future_partitions).parameters.values()
            )[1:]
            theirs = list(
                inspect.signature(real.create_future_partitions).parameters.values()
            )[1:]
        else:
            ours = list(inspect.signature(stub).parameters.values())
            theirs = list(inspect.signature(real).parameters.values())
        assert [p.name for p in ours] == [p.name for p in theirs], name
        # AND the defaults. `commit=True` in the library with `commit=False` in
        # the fake means the atomicity assertion rests on a default nothing
        # checks -- a production call that stopped passing commit=False would
        # still land in a fake that behaves atomically.
        assert [p.default for p in ours] == [p.default for p in theirs], name


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
        raising_enqueue(psycopg2.errors.CheckViolation("no partition for 2026-09-16")),
    )

    assert r.process_job(event("Cortex/Family/School/DPS")) == "released"
    assert calls["heal"] == [3]
    assert calls["release"] == [(1, r.poll_interval)]
    assert calls["fail"] == []


@pytest.mark.parametrize(
    "heal_error",
    [
        psycopg2.errors.InsufficientPrivilege("permission denied for table queue"),
        psycopg2.errors.LockNotAvailable("canceling statement due to lock timeout"),
        psycopg2.errors.InFailedSqlTransaction("current transaction is aborted"),
        RuntimeError("cannot create partition"),
    ],
)
def test_a_heal_that_fails_still_releases(monkeypatch, spy, heal_error):
    """Charging it would dead-letter every matched event in three passes for
    an infrastructure fault -- a role that cannot CREATE TABLE, a shadowed
    partition name, a lock timeout. Those are the healthy events this project
    exists not to lose. The backstop against spinning is the rate limit and
    run()'s no-progress sleep, not the attempt budget.

    PARAMETRISED OVER REAL EXCEPTION TYPES, because a broad `except` narrows to
    whatever its test happens to raise. This one only ever saw a RuntimeError
    -- including one spelled `RuntimeError("no permission")` -- so narrowing it
    to RuntimeError passed the suite, and a real InsufficientPrivilege would
    then escape the heal, the CheckViolation handler, process_job, run() and
    main(). That is the P0 shape, coming out of the guard whose own docstring
    says a failed heal still releases.
    """
    calls, state = spy
    state["heal_raises"] = heal_error
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)
    # raising_enqueue, not a bare throw: a failed statement aborts the
    # transaction, which is the state the heal has to cope with.
    monkeypatch.setattr(
        R,
        "enqueue",
        raising_enqueue(psycopg2.errors.CheckViolation("no partition for 2026-09-16")),
    )

    assert r.process_job(event("Cortex/Family/School/DPS")) == "released"
    assert calls["fail"] == []
    assert calls["release"] == [(1, r.poll_interval)]
    assert (
        counter("cortex_errors", service=R.SERVICE, error_type="partition_heal_failed")
        > 0
    )


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
        raising_enqueue(psycopg2.OperationalError("gone")),
    )

    assert r.process_job(event("Cortex/Family/School/DPS")) == "lost"


def test_a_stale_fail_report_is_reported_as_lost(monkeypatch, spy):
    calls, state = spy
    state["fail_returns"] = "stale"
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(R, "enqueue", raising_enqueue(RuntimeError("boom")))
    before = counter("cortex_errors", service=R.SERVICE, error_type="job_failed")

    assert r.process_job(event("Cortex/Family/School/DPS")) == "lost"
    # Someone else owns the job now. Charging `job_failed` for their work is
    # how one slow batch becomes a failure count nobody can trace.
    assert (
        counter("cortex_errors", service=R.SERVICE, error_type="job_failed") == before
    )


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
    """Insurance against walking a backlog of rows that all fail the same way.

    Not against re-claiming the SAME rows -- `release()` defers them by
    `next_attempt_at` -- which is why the original measurement behind this
    guard was a stub artefact. The real case is 82k pending events and a fault
    that hits every one of them.
    """
    _, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(
        R,
        "enqueue",
        raising_enqueue(psycopg2.OperationalError("gone")),
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


@pytest.mark.parametrize(
    "claim_error",
    [
        psycopg2.OperationalError("server closed the connection"),
        psycopg2.InterfaceError("connection already closed"),
        psycopg2.errors.UndefinedTable('relation "queue" does not exist'),
        RuntimeError("something nobody predicted"),
    ],
)
def test_a_dead_database_does_not_kill_the_loop(monkeypatch, spy, claim_error):
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
        raise claim_error

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


@pytest.mark.parametrize(
    "missing",
    ["POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"],
)
def test_main_refuses_to_start_without_its_environment(monkeypatch, missing):
    """One variable at a time: deleting all four and asserting on one left the
    other three's presence in the check unconstrained -- dropping
    POSTGRES_PASSWORD from it passed. A router that boots with no password
    connects to nothing, forever."""
    for v in ("POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"):
        monkeypatch.setenv(v, "x")
    monkeypatch.delenv(missing)

    with pytest.raises(SystemExit) as e:
        R.main()

    assert missing in str(e.value)


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

    handlers: dict[int, Any] = {}
    monkeypatch.setattr(
        R.signal, "signal", lambda sig, fn: handlers.__setitem__(sig, fn)
    )

    captured: list[R.Router] = []
    ports: list[int] = []
    monkeypatch.setattr(R, "start_metrics_server", lambda port: ports.append(port))
    monkeypatch.setattr(R.Router, "run", lambda self: captured.append(self))
    R.main()

    # Prometheus has to be able to scrape it, and the metrics are the whole
    # argument for flipping the producer on.
    assert ports == [8000]

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


def produce(r, outcome, monkeypatch, spy):
    """Drive the router to one real outcome, through process_job."""
    calls, state = spy
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)

    def boom(exc):
        monkeypatch.setattr(R, "enqueue", raising_enqueue(exc))

    if outcome == "routed":
        return r.process_job(event("Cortex/Family/School/DPS"))
    if outcome == "unmatched":
        return r.process_job(event("Cortex/Automated/Social/X"))
    if outcome == "dropped":
        return r.process_job(job({"gmail_id": "abc"}))
    if outcome == "failed":
        boom(RuntimeError("boom"))
        return r.process_job(event("Cortex/Family/School/DPS"))
    if outcome == "released":
        boom(psycopg2.OperationalError("gone"))
        return r.process_job(event("Cortex/Family/School/DPS"))
    if outcome == "lost":
        state["complete_returns"] = False
        return r.process_job(event("Cortex/Family/School/DPS"))
    if outcome == "unsettled":
        monkeypatch.setattr(
            r,
            "_connect",
            lambda: (_ for _ in ()).throw(psycopg2.OperationalError("no server")),
        )
        return r.process_job(event("Cortex/Family/School/DPS"))
    raise AssertionError(outcome)


@pytest.mark.parametrize("outcome", R.OUTCOMES)
def test_each_outcome_counts_exactly_one_claimed_job(monkeypatch, spy, outcome):
    """Driven through `process_job`, not by calling the counter directly.

    The previous version of this test called `_counted` and asserted `_counted`
    increments -- so four of the five settle returns could stop calling it with
    the suite green. The defect it describes was in the callers, which is the
    same shape as the P0 this PR exists to fix: a test agreeing with the code
    instead of constraining it.
    """
    r = make_router(monkeypatch)
    before = {
        st: counter("cortex_queue_processed", queue=R.SOURCE_QUEUE, status=st)
        for st in ("success", "error", "skipped")
    }

    assert produce(r, outcome, monkeypatch, spy) == outcome

    after = {
        st: counter("cortex_queue_processed", queue=R.SOURCE_QUEUE, status=st)
        for st in ("success", "error", "skipped")
    }
    moved = {st: after[st] - before[st] for st in after}
    assert sum(moved.values()) == 1, moved
    assert moved[R._STATUS[outcome]] == 1


@pytest.mark.parametrize(
    "outcome,error_type",
    [("released", "never_started"), ("failed", "job_failed")],
)
def test_the_error_type_is_the_fleets_word(monkeypatch, spy, outcome, error_type):
    """`_ERROR_TYPE` was asserted as a set, so every individual mapping was
    free to be wrong -- and nothing read an ERRORS label at all."""
    r = make_router(monkeypatch)
    before = counter("cortex_errors", service=R.SERVICE, error_type=error_type)

    produce(r, outcome, monkeypatch, spy)

    assert counter("cortex_errors", service=R.SERVICE, error_type=error_type) == (
        before + 1
    )


@pytest.mark.parametrize("outcome,status", [(o, R._STATUS[o]) for o in R.OUTCOMES])
def test_each_outcome_maps_to_the_status_it_should(outcome, status):
    """Asserting the two sets are equal left every individual mapping free:
    `lost -> success` and `dropped -> error` both passed."""
    expected = {
        "routed": "success",
        "unmatched": "success",
        "dropped": "success",
        "failed": "error",
        "released": "skipped",
        "lost": "skipped",
        "unsettled": "skipped",
    }
    assert R._STATUS[outcome] == expected[outcome] == status


# -- the counters that measure the sweep ------------------------------------


def test_routing_increments_routed_not_suppressed(monkeypatch, spy):
    """Nothing asserted these move at all -- both `.inc()` calls could be
    deleted with the suite green, and these two counters are the pair README
    says to read before switching the producer on."""
    _, _ = spy
    r = make_router(monkeypatch)
    before = counter(
        "cortex_actions_routed", label_prefix="Cortex/Family", queue="school"
    )
    before_s = counter(
        "cortex_actions_suppressed", label_prefix="Cortex/Family", queue="school"
    )

    r._route(FakeConn(), event("Cortex/Family/School/DPS"))

    assert (
        counter("cortex_actions_routed", label_prefix="Cortex/Family", queue="school")
        == before + 1
    )
    assert (
        counter(
            "cortex_actions_suppressed", label_prefix="Cortex/Family", queue="school"
        )
        == before_s
    )


def test_an_unmatched_label_increments_unmatched(monkeypatch, spy):
    _, _ = spy
    r = make_router(monkeypatch)
    before = counter("cortex_actions_unmatched", label_prefix="Cortex/Automated")

    r._route(FakeConn(), event("Cortex/Automated/Social/X"))

    assert (
        counter("cortex_actions_unmatched", label_prefix="Cortex/Automated")
        == before + 1
    )


def test_metrics_carry_the_prefix_and_never_the_full_label(monkeypatch, spy):
    """CLAUDE.md forbids this by name: triage rules mint labels with
    interpolated variables, so a full-label dimension grows a series per email.
    Passing `label` instead of `prefix` survived every previous test."""
    _, _ = spy
    r = make_router(monkeypatch)
    label = "Cortex/Automated/Social/Nextdoor/Thread/12345"

    r._route(FakeConn(), event(label))

    assert counter("cortex_actions_unmatched", label_prefix="Cortex/Automated") > 0
    assert counter("cortex_actions_unmatched", label_prefix=label) == 0


def test_a_rolled_back_route_counts_no_enqueues(monkeypatch, spy):
    """`routed` must be enqueues that are DURABLE. Counting inside the
    transaction inflates it exactly when the router is struggling, and the
    retry counts them again."""
    _, state = spy
    state["complete_returns"] = False
    r = make_router(monkeypatch)
    before = counter(
        "cortex_actions_routed", label_prefix="Cortex/Family", queue="school"
    )

    assert r._route(FakeConn(), event("Cortex/Family/School/DPS")) == "lost"

    assert (
        counter("cortex_actions_routed", label_prefix="Cortex/Family", queue="school")
        == before
    )


# -- the sleep itself -------------------------------------------------------


def test_sleep_actually_sleeps_in_one_second_ticks(monkeypatch):
    """The backoff was asserted through a stub of itself.

    Both backoff tests monkeypatch `_sleep` and count calls, and the only test
    touching the real method set a stop flag first -- so the loop body never
    ran, and `_sleep -> return`, `range(poll_interval) -> range(1)` and
    `sleep(1) -> sleep(0)` all survived. One-second ticks are the reason a
    SIGTERM lands promptly instead of after a whole poll interval.
    """
    r = make_router(monkeypatch)
    r.poll_interval = 7
    slept: list[float] = []
    monkeypatch.setattr(R.time, "sleep", lambda s: slept.append(s))

    r._sleep()

    assert slept == [1] * 7


def test_sleep_stops_early_when_asked_to_stop(monkeypatch):
    r = make_router(monkeypatch)
    r.poll_interval = 10
    slept: list[float] = []

    def tick(s):
        slept.append(s)
        if len(slept) == 3:
            r._stop.set()

    monkeypatch.setattr(R.time, "sleep", tick)
    r._sleep()

    assert slept == [1, 1, 1]


def test_sleep_stops_early_for_a_reload(monkeypatch):
    r = make_router(monkeypatch)
    r.poll_interval = 10
    slept: list[float] = []

    def tick(s):
        slept.append(s)
        if len(slept) == 2:
            r.request_reload()

    monkeypatch.setattr(R.time, "sleep", tick)
    r._sleep()

    assert slept == [1, 1]


@pytest.mark.parametrize("member", ["routed", "unmatched", "dropped", "failed"])
def test_every_progress_member_disarms_the_backoff(monkeypatch, spy, member):
    """`PROGRESS <= OUTCOMES` left the interior free, and parametrising over
    PROGRESS itself only restated it. These four are spelled out: dropping
    `unmatched` -- the overwhelming majority path, by `_route`'s own comment --
    would idle a poll interval after almost every batch, adding hours to the
    82,288-message sweep, with nothing failing.
    """
    r = make_router(monkeypatch)
    slept: list[int] = []
    monkeypatch.setattr(r, "_sleep", lambda: slept.append(1))
    monkeypatch.setattr(R, "ensure_queue_schema", lambda c, extra_indexes=(): None)
    conn = FakeConn()

    outcomes = {
        "routed": event("Cortex/Family/School/DPS"),
        "unmatched": event("Cortex/Automated/Social/X"),
        "dropped": job({"gmail_id": "abc"}),
        "failed": event("Cortex/Family/School/DPS"),
    }
    if member == "failed":
        monkeypatch.setattr(R, "enqueue", raising_enqueue(RuntimeError("boom")))

    drive(monkeypatch, r, [[outcomes[member]], []], conn=conn)

    assert member in R.PROGRESS
    assert len(slept) == 1  # the empty batch only


@pytest.mark.parametrize("member", ["released", "lost", "unsettled"])
def test_no_outcome_that_hands_the_job_back_counts_as_progress(member):
    """The other direction, and the one that matters: an outcome that returns
    the job uncharged must never disarm the backoff."""
    assert member not in R.PROGRESS


# -- the partition heal -----------------------------------------------------


def test_a_named_constraint_violation_is_a_fault_not_an_outage(monkeypatch, spy):
    """Two faults share SQLSTATE 23514. `queue_new_valid_status` is the real
    named CHECK on the deployed schema (read off hades -- an earlier comment
    called it `queue_valid_status`, which exists nowhere, and the wrong name
    was copied from there into this test and into a ticket). Treating it as a
    missing partition released it forever, uncharged, never dead-lettered."""
    calls, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)

    exc = psycopg2.errors.CheckViolation("status check")
    monkeypatch.setattr(
        type(exc),
        "diag",
        property(lambda self: _Diag("queue_new_valid_status")),
        raising=False,
    )
    monkeypatch.setattr(R, "enqueue", raising_enqueue(exc))

    assert r.process_job(event("Cortex/Family/School/DPS")) == "failed"
    assert calls["heal"] == []
    assert calls["fail"]


class _Diag:
    def __init__(self, name):
        self.constraint_name = name


def test_the_heal_rolls_back_before_it_runs_ddl(monkeypatch, spy):
    """The CheckViolation aborted the transaction and the library's partition
    calls use bare cursors, so without the rollback every heal raises
    InFailedSqlTransaction -- and then every heal 'fails'."""
    calls, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(
        R,
        "enqueue",
        raising_enqueue(psycopg2.errors.CheckViolation("no partition")),
    )

    assert r.process_job(event("Cortex/Family/School/DPS")) == "released"
    assert calls["heal"] == [3]
    # rollback precedes the DDL, and the fake refuses to work otherwise
    assert "rollback" in conn.log


def test_the_heal_runs_at_most_once_per_poll_interval(monkeypatch, spy):
    """CREATE TABLE ... PARTITION OF takes an AccessExclusiveLock on the parent
    `queue` table every cortex service claims from. Once per failing job is
    once per job in the backlog -- measured at 981 attempts in 3.09s against a
    real Postgres, with a FIFO lock queue stalling every reader behind it."""
    calls, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(
        R,
        "enqueue",
        raising_enqueue(psycopg2.errors.CheckViolation("no partition")),
    )

    for _ in range(50):
        assert r.process_job(event("Cortex/Family/School/DPS")) == "released"

    assert calls["heal"] == [3]  # once, not fifty


def test_the_heals_ddl_is_given_a_lock_timeout(monkeypatch, spy):
    """An ungranted AccessExclusiveLock stalls every reader queued behind it,
    conflicting or not, because Postgres's lock queue is FIFO.

    It must be SET (committed), not SET LOCAL: `create_partition` runs its own
    transactions with bare cursors and sets no timeout of its own, so a LOCAL
    one would expire before the DDL it is meant to bound.
    """
    calls, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(
        R,
        "enqueue",
        raising_enqueue(psycopg2.errors.CheckViolation("no partition")),
    )

    assert r.process_job(event("Cortex/Family/School/DPS")) == "released"

    # `"SET lock_timeout" in line` also matches RE + SET lock_timeout, so the
    # RESET satisfied the assertion for the SET and deleting the SET passed.
    # startswith("exec:SET ") also matches SET LOCAL, which the next statement's
    # commit discards -- so the DDL would run with no bound at all, which is
    # the thing this test's own docstring says must not happen. Pin the exact
    # statement, and pin it BEFORE the DDL.
    assert "exec:SET lock_timeout = %s:('2000ms',)" in conn.log, conn.log
    assert not any("LOCAL" in line for line in conn.log), conn.log
    assert "DDL" in conn.log, conn.log
    assert conn.log.index("exec:SET lock_timeout = %s:('2000ms',)") < conn.log.index(
        "DDL"
    )
    # after the rollback that clears the aborted transaction, and committed so
    # the library's own transactions inherit it
    at = conn.log.index("exec:SET lock_timeout = %s:('2000ms',)")
    assert "rollback" in conn.log[:at]
    # BETWEEN the SET and the DDL. `"commit" in conn.log[at:]` was satisfied by
    # the commit the fake's own DDL makes, so deleting the one that publishes
    # the SET passed -- and an uncommitted session SET is rolled back with the
    # transaction, which is exactly the failure mode this line exists to stop.
    assert "commit" in conn.log[at : conn.log.index("DDL")]
    # Pinned exactly, like the SET above it: `startswith("exec:RESET ")` also
    # matches `RESET ALL`, which would clear every session setting the
    # connection carries rather than the one this code set. Fifth occurrence of
    # this shape on this branch, and the first on the reset side -- the four
    # before it were all fixed on the SET side of the same function.
    assert "exec:RESET lock_timeout:None" in conn.log, conn.log
    assert calls["heal"] == [3]


def test_the_lock_timeout_is_the_librarys_own_number():
    """Pinned as a literal, not read back from the constant it checks -- that
    shape passed with `LOCK_TIMEOUT = "0"`, which Postgres reads as *no*
    timeout. Derived from PARTITION_LOCK_TIMEOUT_MS so it cannot drift from the
    value the library uses for the same DDL.
    """
    from cortex_utils.queue.ops import PARTITION_LOCK_TIMEOUT_MS

    assert R.LOCK_TIMEOUT == "2000ms"
    assert f"{PARTITION_LOCK_TIMEOUT_MS}ms" == R.LOCK_TIMEOUT
    assert PARTITION_LOCK_TIMEOUT_MS > 0


def test_boot_is_not_given_the_heals_lock_timeout(monkeypatch):
    """A session-wide lock_timeout would also govern `ensure_queue_schema`.

    The library leaves boot's DDL unbounded -- `_tx` issues SET LOCAL, which is
    transaction-scoped, and `ensure_queue_table` and `_ensure_indexes` each open
    their own `_tx(conn)` with no bound, so SCHEMA_LOCK_TIMEOUT_MS covers only
    the advisory-lock front door. Measured with a session-wide 2s: boot under
    contention raised LockNotAvailable at 2.02s and propagated out of main(),
    turning a deploy against a busy queue into a restart loop. Boot should
    wait; the heal must not.

    `conn.log == []` rather than "no lock_timeout in the log": _connect must
    issue NOTHING, so a future statement added here fails this test and gets
    thought about.
    """
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(R.psycopg2, "connect", lambda dsn: conn)

    r._connect()

    assert conn.log == []


def test_a_row_with_no_payload_at_all_is_dropped(monkeypatch, spy):
    """`payload` is NOT NULL in the queue table, so this is defensive -- but
    the defence is one `.get`, and without it the KeyError lands in the generic
    handler and charges three attempts into dead_letter for a row that could
    never have been routed.

    (This test existed, was lost to an edit, and was found again by re-running
    the previous round's mutation set. A green suite is not proof the tests are
    still there.)
    """
    calls, _ = spy
    r = make_router(monkeypatch)
    row = job({})
    del row["payload"]

    assert r._route(FakeConn(), row) == "dropped"
    assert calls["fail"] == []


# -- every error has a name, and it is asserted ----------------------------


def drive_error(kind, r, monkeypatch, spy):
    """Drive exactly one ERRORS path. Returns nothing; asserts are the caller's."""
    calls, state = spy
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)

    if kind == "job_failed":
        monkeypatch.setattr(R, "enqueue", raising_enqueue(RuntimeError("boom")))
        assert r.process_job(event("Cortex/Family/School/DPS")) == "failed"
    elif kind == "never_started":
        monkeypatch.setattr(
            R, "enqueue", raising_enqueue(psycopg2.OperationalError("gone"))
        )
        assert r.process_job(event("Cortex/Family/School/DPS")) == "released"
    elif kind == "no_connection":
        monkeypatch.setattr(
            r,
            "_connect",
            lambda: (_ for _ in ()).throw(psycopg2.OperationalError("no server")),
        )
        assert r.process_job(event("Cortex/Family/School/DPS")) == "unsettled"
    elif kind == "claim_lost_completing":
        state["complete_returns"] = False
        assert r.process_job(event("Cortex/Family/School/DPS")) == "lost"
    elif kind == "claim_lost_settling":
        state["release_returns"] = False
        monkeypatch.setattr(
            R, "enqueue", raising_enqueue(psycopg2.OperationalError("gone"))
        )
        assert r.process_job(event("Cortex/Family/School/DPS")) == "lost"
    elif kind == "settle_error":
        monkeypatch.setattr(
            R, "enqueue", raising_enqueue(psycopg2.OperationalError("gone"))
        )
        monkeypatch.setattr(
            conn, "rollback", lambda: (_ for _ in ()).throw(psycopg2.InterfaceError())
        )
        assert r.process_job(event("Cortex/Family/School/DPS")) == "unsettled"
    elif kind == "malformed_event":
        assert r.process_job(job({"gmail_id": "abc"})) == "dropped"
    elif kind == "partition_heal_failed":
        state["heal_raises"] = RuntimeError("no permission")
        monkeypatch.setattr(
            R,
            "enqueue",
            raising_enqueue(psycopg2.errors.CheckViolation("no partition")),
        )
        assert r.process_job(event("Cortex/Family/School/DPS")) == "released"
    elif kind == "claim_error":
        monkeypatch.setattr(R, "ensure_queue_schema", lambda c, extra_indexes=(): None)
        monkeypatch.setattr(r, "_sleep", lambda: r._stop.set())
        monkeypatch.setattr(
            R,
            "claim",
            lambda *a, **k: (_ for _ in ()).throw(psycopg2.OperationalError("gone")),
        )
        r.run()
    elif kind == "bad_subscriptions":
        monkeypatch.setattr(
            R, "load", lambda path: (_ for _ in ()).throw(R.SubscriptionError("bad"))
        )
        r.reload()
    else:
        raise AssertionError(kind)


@pytest.mark.parametrize(
    "kind,error_type",
    [
        ("job_failed", "job_failed"),
        ("never_started", "never_started"),
        ("no_connection", "no_connection"),
        ("claim_lost_completing", "claim_lost"),
        ("claim_lost_settling", "claim_lost"),
        ("settle_error", "settle_error"),
        ("malformed_event", "malformed_event"),
        ("partition_heal_failed", "partition_heal_failed"),
        ("claim_error", "claim_error"),
        ("bad_subscriptions", "bad_subscriptions"),
    ],
)
def test_every_failure_increments_its_own_error_type(
    monkeypatch, spy, kind, error_type
):
    """Six of eight ERRORS increments were deletable with the suite green, and
    the `service` label was free -- `service="triage"` passed.

    Two of these paths have no other metric at all: `claim_error` (the dead-DB
    loop) and `malformed_event`, where the drop is irreversible and counts as
    `success` in QUEUE_PROCESSED.
    """
    r = make_router(monkeypatch)
    before = counter("cortex_errors", service=R.SERVICE, error_type=error_type)
    wrong_service = counter("cortex_errors", service="triage", error_type=error_type)

    drive_error(kind, r, monkeypatch, spy)

    assert (
        counter("cortex_errors", service=R.SERVICE, error_type=error_type) == before + 1
    )
    assert counter("cortex_errors", service="triage", error_type=error_type) == (
        wrong_service
    )


def test_a_claim_lost_while_settling_is_still_counted_once(monkeypatch, spy):
    """`produce()` reaches `lost` only through `_complete`, so the settle-side
    `lost` return was the one settle path whose `_counted` was deletable."""
    _, state = spy
    state["release_returns"] = False
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(
        R, "enqueue", raising_enqueue(psycopg2.OperationalError("gone"))
    )
    before = counter("cortex_queue_processed", queue=R.SOURCE_QUEUE, status="skipped")

    assert r.process_job(event("Cortex/Family/School/DPS")) == "lost"

    assert counter(
        "cortex_queue_processed", queue=R.SOURCE_QUEUE, status="skipped"
    ) == (before + 1)


# -- the rate limit, in both directions ------------------------------------


def test_a_failing_heal_is_rate_limited_too(monkeypatch, spy):
    """THE BRANCH AN OUTAGE ACTUALLY TAKES.

    The upper-bound test drives a heal that SUCCEEDS; moving
    `self._last_heal = ...` inside the try -- so only a successful heal spends
    the token -- passes that one, and measured 1280 heals against 8 on a real
    database. The failing heal is the case the rate limit exists for.
    """
    calls, state = spy
    state["heal_raises"] = RuntimeError("cannot create partition")
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(
        R,
        "enqueue",
        raising_enqueue(psycopg2.errors.CheckViolation("no partition")),
    )

    for _ in range(50):
        assert r.process_job(event("Cortex/Family/School/DPS")) == "released"

    assert calls["heal"] == [3]


def test_the_rate_limit_reopens(monkeypatch, spy):
    """The gate has to close AND open. `_last_heal = float("inf")` -- heal once
    per process, ever -- satisfies an upper-bound assertion on its own, and
    would leave the partition uncreated for the life of the container."""
    calls, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(
        R,
        "enqueue",
        raising_enqueue(psycopg2.errors.CheckViolation("no partition")),
    )

    now = [1000.0]
    monkeypatch.setattr(R.time, "monotonic", lambda: now[0])

    r.process_job(event("Cortex/Family/School/DPS"))
    r.process_job(event("Cortex/Family/School/DPS"))
    assert calls["heal"] == [3]

    now[0] += r.poll_interval
    r.process_job(event("Cortex/Family/School/DPS"))

    assert calls["heal"] == [3, 3]


@pytest.mark.parametrize("created", [0, 1])
def test_the_heal_releases_whatever_it_created(monkeypatch, spy, created):
    """`created` is logged, not branched on: on a violation naming no
    constraint, zero created means another worker won the race."""
    calls, state = spy
    state["heal_creates"] = created
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(
        R,
        "enqueue",
        raising_enqueue(psycopg2.errors.CheckViolation("no partition")),
    )

    assert r.process_job(event("Cortex/Family/School/DPS")) == "released"
    assert calls["fail"] == []


def test_a_settle_with_no_connection_drops_the_one_we_may_still_hold(monkeypatch, spy):
    """`conn` is the local that `_route` never received, and `self._conn` must
    not be left behind.

    Belt and braces rather than a live defect: `_connect()` does no
    post-connect work now, so `self._conn` here is already None or closed.
    Asserted so the assumption is written down rather than remembered the next
    time `_connect` grows a line."""
    _, _ = spy
    r = make_router(monkeypatch)
    r._conn = FakeConn()
    monkeypatch.setattr(
        r,
        "_connect",
        lambda: (_ for _ in ()).throw(psycopg2.OperationalError("no server")),
    )

    assert r.process_job(event("Cortex/Family/School/DPS")) == "unsettled"
    # Belt and braces rather than a live defect: `_connect()` does no
    # post-connect work now, so this is normally already None. Asserted so the
    # assumption is written down rather than remembered the next time
    # `_connect` grows a line.
    assert r._conn is None


def test_a_lost_claim_counts_no_unmatched(monkeypatch, spy):
    """The majority path, and the one whose shape gets copied. Counted before
    the settle, a lost claim counts the event here and again on the retry."""
    _, state = spy
    state["complete_returns"] = False
    r = make_router(monkeypatch)
    before = counter("cortex_actions_unmatched", label_prefix="Cortex/Automated")

    assert r._route(FakeConn(), event("Cortex/Automated/Social/X")) == "lost"

    assert (
        counter("cortex_actions_unmatched", label_prefix="Cortex/Automated") == before
    )


def test_a_lost_claim_counts_no_malformed_drop(monkeypatch, spy):
    """A drop is irreversible and counts `success`, so this counter is the only
    record that an event was discarded. It must describe drops that happened."""
    _, state = spy
    state["complete_returns"] = False
    r = make_router(monkeypatch)
    before = counter("cortex_errors", service=R.SERVICE, error_type="malformed_event")

    assert r._route(FakeConn(), job({"gmail_id": "abc"})) == "lost"

    assert (
        counter("cortex_errors", service=R.SERVICE, error_type="malformed_event")
        == before
    )


def test_the_lock_timeout_is_reset_after_a_heal_that_failed(monkeypatch, spy):
    """THE PATH THE RESET EXISTS FOR.

    A failed CREATE TABLE aborts the transaction, so a RESET issued without
    rolling back first is refused, swallowed, and the committed 2s bound stays
    on the connection for the rest of its life -- onto claim, enqueue, complete
    and release. Measured with another session holding ACCESS EXCLUSIVE on
    `queue` for 6s: a matched event took 2s in enqueue and 2s in release and
    settled `unsettled`, leaving the row stuck for the five-minute visibility
    timeout, where the same event on an unbounded connection routed in 5.5s.

    The old test drove a heal that SUCCEEDS, where the reset works by accident.
    """
    calls, state = spy
    state["heal_raises"] = RuntimeError("cannot create partition")
    r = make_router(monkeypatch)
    conn = FakeConn()
    monkeypatch.setattr(r, "_connect", lambda: conn)
    monkeypatch.setattr(
        R,
        "enqueue",
        raising_enqueue(psycopg2.errors.CheckViolation("no partition")),
    )

    assert r.process_job(event("Cortex/Family/School/DPS")) == "released"

    assert "exec:RESET lock_timeout:None" in conn.log, conn.log
    assert not conn.aborted


@pytest.mark.parametrize(
    "boom",
    [
        psycopg2.InterfaceError("connection already closed"),
        psycopg2.OperationalError("server closed the connection"),
        psycopg2.errors.InFailedSqlTransaction("current transaction is aborted"),
        RuntimeError("something nobody predicted"),
    ],
)
def test_setting_the_lock_timeout_can_never_raise(monkeypatch, boom):
    """It is called from a `finally` inside an exception handler.

    Anything escaping there escapes process_job, run() and main() -- and a
    sibling `except` on the same `try` cannot catch it. That is the
    handler-raises-again shape this whole branch was opened to fix, which is
    why the swallow is deliberate rather than lazy.
    """
    r = make_router(monkeypatch)

    class Hostile:
        """Fails on the first thing touched. `rollback()` is now the first
        statement of the try, so the rest is unreachable -- and that is the
        point: whatever comes first, nothing escapes.

        Parametrised over several types because a broad `except` narrows to
        whatever its test raises, and every one in this file did."""

        def rollback(self):
            raise boom

    r._set_lock_timeout(Hostile(), R.LOCK_TIMEOUT)
    r._set_lock_timeout(Hostile(), None)


def test_the_worker_token_identifies_this_process(monkeypatch):
    """`worker_identity(SERVICE)`, not `SERVICE`.

    The token is what `complete`, `release` and `fail_or_retry` match a claim
    against, so two routers sharing one would each settle the other's jobs --
    and `claim()` refuses an empty one outright. Nothing asserted it was more
    than the service name.
    """
    r = make_router(monkeypatch)
    other = make_router(monkeypatch)

    assert r._worker and r._worker != R.SERVICE
    assert R.SERVICE in r._worker
    assert r._worker != other._worker or "-" in r._worker


def test_main_passes_its_environment_through_to_the_router(monkeypatch):
    """The DSN, batch size, poll interval and subscriptions path are all read
    from the environment in `main()` and nothing checked any of them arrive."""
    for v in ("POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"):
        monkeypatch.setenv(v, f"value-of-{v}")
    monkeypatch.setenv("POSTGRES_PORT", "6543")
    monkeypatch.setenv("BATCH_SIZE", "7")
    monkeypatch.setenv("POLL_INTERVAL", "11")
    monkeypatch.setenv("SUBSCRIPTIONS_PATH", "/somewhere/subs.yaml")
    seen: list = []
    monkeypatch.setattr(R, "load", lambda path: (seen.append(path), SUBS)[1])
    monkeypatch.setattr(R, "start_metrics_server", lambda port: None)
    monkeypatch.setattr(R.signal, "signal", lambda sig, fn: None)
    built: list = []
    monkeypatch.setattr(R.Router, "run", lambda self: built.append(self))

    R.main()

    r = built[0]
    assert seen == ["/somewhere/subs.yaml"]
    assert r.batch_size == 7
    assert r.poll_interval == 11
    assert "host=value-of-POSTGRES_HOST" in r.dsn
    assert "port=6543" in r.dsn
    assert "dbname=value-of-POSTGRES_DB" in r.dsn
    assert "password=value-of-POSTGRES_PASSWORD" in r.dsn
