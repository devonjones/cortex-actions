"""Router behaviour, with the queue library stubbed.

What matters here is not the SQL -- cortex_utils.queue owns and tests that --
but the four decisions this service makes: route, drop silently, drop loudly,
and give the claim back. Each has a different failure cost.
"""

from dataclasses import dataclass, field
from typing import Any

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


@dataclass
class FakeJob:
    id: int = 1
    payload: dict[str, Any] = field(default_factory=dict)


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
    calls: dict[str, list] = {"enqueue": [], "complete": [], "fail": []}
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

    monkeypatch.setattr(R, "enqueue", fake_enqueue)
    monkeypatch.setattr(R, "complete", fake_complete)
    monkeypatch.setattr(R, "fail_or_retry", fake_fail)
    return calls, state


def make_router(monkeypatch, subs=SUBS) -> R.Router:
    monkeypatch.setattr(R, "load", lambda path: subs)
    r = R.Router(dsn="", subs_path="ignored", batch_size=10, poll_interval=1)
    return r


def event(label: str, event_type: str = "added", gmail_id: str = "abc123") -> FakeJob:
    return FakeJob(
        payload={"gmail_id": gmail_id, "label": label, "event_type": event_type}
    )


def test_routes_to_every_matching_queue_in_one_transaction(monkeypatch, spy):
    calls, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()

    assert r._route(conn, event("Cortex/Family/School/DPS")) is True

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

    assert r._route(conn, event("Cortex/Automated/Social/Nextdoor")) is True

    assert calls["enqueue"] == []
    assert calls["fail"] == []
    assert len(calls["complete"]) == 1
    assert conn.commits == 1


def test_removed_event_is_not_routed_unless_opted_in(monkeypatch, spy):
    calls, _ = spy
    r = make_router(monkeypatch)
    assert r._route(FakeConn(), event("Cortex/Family/School/DPS", "removed")) is True
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

    assert r._route(conn, event("Cortex/Family/School/DPS")) is False
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_malformed_event_is_dropped_not_retried(monkeypatch, spy):
    """A row with no label cannot become well-formed by being retried."""
    calls, _ = spy
    r = make_router(monkeypatch)
    conn = FakeConn()

    assert r._route(conn, FakeJob(payload={"gmail_id": "abc"})) is True
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

    assert r.process_job(event("Cortex/Family/School/DPS")) is False
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
