"""The routing table: which label events go to which downstream queue.

Deliberately a git-versioned YAML file rather than gateway-managed config. The
triage rules live behind the Gateway API because they change constantly; a table
of label globs changes a few times a year, and git is the right versioning for
that -- a diff, a PR and a blame line beat a version number in a database.

ponytail: if this ever starts churning, the upgrade path is cortex_utils.triage_config
(versioned, hash-deduplicated, one-active-at-a-time, with diff and rollback
endpoints already built). Do not build a second half of that here.
"""

from __future__ import annotations

import fnmatch
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

# The queue this service consumes. A subscription targeting it would re-enqueue
# every event it just claimed, forever, at whatever rate the router can manage.
SOURCE_QUEUE = "actions"

VALID_EVENTS = frozenset({"added", "removed"})


class SubscriptionError(ValueError):
    """The routing table is malformed. Always fatal -- see load()."""


@dataclass(frozen=True)
class Subscription:
    """One routing rule: a label glob, a destination queue, and which events."""

    label: str
    queue: str
    events: frozenset[str]

    def matches(self, label: str, event_type: str) -> bool:
        if event_type not in self.events:
            return False
        # fnmatch's `*` spans `/`, so "Cortex/Family/School/*" covers both
        # ".../DPS" and any deeper ".../DPS/Whatever". It does NOT match the
        # bare prefix "Cortex/Family/School" -- subscribe to that separately if
        # the parent label is ever applied on its own.
        return fnmatch.fnmatchcase(label, self.label)


def parse(raw: Any) -> list[Subscription]:
    """Validate a loaded YAML document into subscriptions.

    Every problem here is a typo in a routing table that silently misdirects
    mail, so all of them raise rather than warn-and-continue.
    """
    if not isinstance(raw, dict):
        raise SubscriptionError(
            "top level must be a mapping with a 'subscriptions' key"
        )

    entries = raw.get("subscriptions")
    if entries is None:
        raise SubscriptionError("missing 'subscriptions' key")
    if not isinstance(entries, list):
        raise SubscriptionError("'subscriptions' must be a list")

    subs: list[Subscription] = []
    seen: set[tuple[str, str]] = set()

    for i, entry in enumerate(entries):
        where = f"subscriptions[{i}]"
        if not isinstance(entry, dict):
            raise SubscriptionError(f"{where}: must be a mapping")

        label = entry.get("label")
        if not isinstance(label, str) or not label.strip():
            raise SubscriptionError(f"{where}: 'label' must be a non-empty string")

        queue = entry.get("queue")
        if not isinstance(queue, str) or not queue.strip():
            raise SubscriptionError(f"{where}: 'queue' must be a non-empty string")
        if queue != queue.strip() or any(c.isspace() for c in queue):
            raise SubscriptionError(
                f"{where}: queue name {queue!r} contains whitespace"
            )
        if queue == SOURCE_QUEUE:
            raise SubscriptionError(
                f"{where}: cannot route to {SOURCE_QUEUE!r} -- that is this "
                f"service's own input queue, and would loop forever"
            )

        raw_events = entry.get("events", ["added"])
        if not isinstance(raw_events, list) or not raw_events:
            raise SubscriptionError(f"{where}: 'events' must be a non-empty list")
        bad = [e for e in raw_events if e not in VALID_EVENTS]
        if bad:
            raise SubscriptionError(
                f"{where}: unknown event(s) {bad}; valid: {sorted(VALID_EVENTS)}"
            )

        key = (label, queue)
        if key in seen:
            raise SubscriptionError(
                f"{where}: duplicate rule for {label!r} -> {queue!r}"
            )
        seen.add(key)

        subs.append(
            Subscription(label=label, queue=queue, events=frozenset(raw_events))
        )

    return subs


def load(path: str | Path) -> list[Subscription]:
    """Read and validate the routing table.

    A malformed table is fatal on startup and, on SIGHUP, leaves the previous
    table in place -- see the router's reload(). Routing to nowhere because a
    reload silently emptied the table is worse than refusing to reload.
    """
    p = Path(path)
    try:
        raw = yaml.safe_load(p.read_text())
    except FileNotFoundError as e:
        raise SubscriptionError(f"no subscriptions file at {p}") from e
    except yaml.YAMLError as e:
        raise SubscriptionError(f"{p} is not valid YAML: {e}") from e
    return parse(raw)


def targets(subs: list[Subscription], label: str, event_type: str) -> list[str]:
    """Queues this event should be routed to, in table order, deduplicated.

    An empty list is the normal case, not an error: most Cortex/* label traffic
    matches no subscription at all.
    """
    out: list[str] = []
    for s in subs:
        if s.matches(label, event_type) and s.queue not in out:
            out.append(s.queue)
    return out
