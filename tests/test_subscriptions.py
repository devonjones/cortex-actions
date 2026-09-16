import pytest

from actions.subscriptions import (
    VALID_EVENTS,
    Subscription,
    SubscriptionError,
    load,
    parse,
    targets,
)

SCHOOL = {"subscriptions": [{"label": "Cortex/Family/School/*", "queue": "school"}]}


def test_defaults_to_added_only():
    (sub,) = parse(SCHOOL)
    assert sub.events == frozenset({"added"})
    assert sub.matches("Cortex/Family/School/DPS", "added")
    assert not sub.matches("Cortex/Family/School/DPS", "removed")


def test_glob_spans_slashes_but_not_the_bare_parent():
    (sub,) = parse(SCHOOL)
    assert sub.matches("Cortex/Family/School/DPS", "added")
    assert sub.matches("Cortex/Family/School/DPS/Forms", "added")
    # Gmail child labels do not imply the parent, and nothing applies the bare
    # parent -- but if that changes it must be subscribed to explicitly.
    assert not sub.matches("Cortex/Family/School", "added")
    assert not sub.matches("Cortex/Family/Medical/Kaiser", "added")


def test_matching_is_case_sensitive():
    # fnmatchcase, not fnmatch: on a case-insensitive filesystem the latter
    # would quietly route Cortex/family/... too.
    (sub,) = parse(SCHOOL)
    assert not sub.matches("cortex/family/school/dps", "added")


def test_unmatched_yields_no_targets_and_is_not_an_error():
    subs = parse(SCHOOL)
    assert targets(subs, "Cortex/Automated/Social/Nextdoor", "added") == []


def test_targets_are_deduplicated_and_ordered():
    subs = parse(
        {
            "subscriptions": [
                {"label": "Cortex/Family/*", "queue": "school"},
                {"label": "Cortex/Family/School/*", "queue": "school"},
                {"label": "Cortex/*", "queue": "archive"},
            ]
        }
    )
    assert targets(subs, "Cortex/Family/School/DPS", "added") == ["school", "archive"]


def test_opt_in_to_removed():
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
    assert targets(subs, "Cortex/Family/School/DPS", "removed") == ["school"]


def test_refuses_to_route_back_into_its_own_queue():
    # Would re-enqueue every event it just claimed, forever.
    with pytest.raises(SubscriptionError, match="loop forever"):
        parse({"subscriptions": [{"label": "Cortex/*", "queue": "actions"}]})


@pytest.mark.parametrize(
    "doc,match",
    [
        ([], "must be a mapping"),
        ({}, "missing 'subscriptions'"),
        ({"subscriptions": "school"}, "must be a list"),
        ({"subscriptions": [{"queue": "school"}]}, "'label' must be"),
        ({"subscriptions": [{"label": "Cortex/*"}]}, "'queue' must be"),
        (
            {"subscriptions": [{"label": "Cortex/*", "queue": "two words"}]},
            "whitespace",
        ),
        (
            {"subscriptions": [{"label": "Cortex/*", "queue": "s", "events": []}]},
            "non-empty list",
        ),
        (
            {
                "subscriptions": [
                    {"label": "Cortex/*", "queue": "s", "events": ["moved"]}
                ]
            },
            "unknown event",
        ),
        # Unhashable: `["added"] in frozenset(...)` raises TypeError rather
        # than returning False, so without an isinstance guard this escapes as
        # an unhandled TypeError and SIGHUP kills the router.
        (
            {
                "subscriptions": [
                    {"label": "Cortex/*", "queue": "s", "events": [["added"]]}
                ]
            },
            "unknown event",
        ),
        (
            {
                "subscriptions": [
                    {"label": "Cortex/*", "queue": "s", "events": [{"a": 1}]}
                ]
            },
            "unknown event",
        ),
    ],
)
def test_malformed_tables_are_fatal(doc, match):
    # Every one of these is a typo that would silently misdirect mail.
    with pytest.raises(SubscriptionError, match=match):
        parse(doc)


def test_duplicate_rule_is_rejected():
    with pytest.raises(SubscriptionError, match="duplicate"):
        parse(
            {
                "subscriptions": [
                    {"label": "Cortex/Family/School/*", "queue": "school"},
                    {"label": "Cortex/Family/School/*", "queue": "school"},
                ]
            }
        )


def test_shipped_config_is_valid_and_routes_the_real_label():
    """The config/ file in this repo must actually work."""
    from pathlib import Path

    from actions.subscriptions import load

    subs = load(Path(__file__).parent.parent / "config" / "subscriptions.yaml")
    assert targets(subs, "Cortex/Family/School/DPS", "added") == ["school"]


def test_subscription_is_hashable_and_frozen():
    sub = Subscription("Cortex/*", "school", frozenset({"added"}))
    assert {sub, sub} == {sub}


def test_an_unreadable_file_is_refused_not_a_crash(tmp_path):
    """A bad bind mount must be refusable, not fatal.

    reload() catches SubscriptionError and keeps the running table; anything
    else escapes reload() and exits the process. A `docker cp` that lands the
    config as a directory should not take the router down.
    """
    d = tmp_path / "subs.yaml"
    d.mkdir()
    with pytest.raises(SubscriptionError, match="cannot read"):
        load(d)


def test_a_missing_file_still_says_so_plainly(tmp_path):
    with pytest.raises(SubscriptionError, match="no subscriptions file"):
        load(tmp_path / "nope.yaml")


def test_a_valid_file_still_loads(tmp_path):
    """The direction that must keep working."""
    f = tmp_path / "subs.yaml"
    f.write_text(
        "subscriptions:\n  - label: 'Cortex/Family/School/*'\n    queue: school\n"
    )
    assert [s.queue for s in load(f)] == ["school"]


def test_an_empty_table_is_refused():
    """The silent outage this module exists to refuse.

    Every event would take the `unmatched` path, which the router completes
    cleanly and counts as success -- so a reload that deleted every rule logs
    `reloaded subscriptions count=0` at INFO and the events are gone, with
    nothing anywhere saying so.
    """
    with pytest.raises(SubscriptionError, match="empty"):
        parse({"subscriptions": []})


@pytest.mark.parametrize(
    "entry,why",
    [
        ({"label": "Cortex/*", "queue": "s", "event": ["removed"]}, "events -> event"),
        ({"label": "Cortex/*", "queue": "s", "queues": ["x"]}, "queue -> queues"),
        (
            {"label": "Cortex/*", "queue": "s", "priority": -100},
            "a knob we do not read",
        ),
    ],
)
def test_a_key_we_do_not_know_is_a_typo(entry, why):
    """`event:` for `events:` parses clean, defaults to ['added'], reloads
    without complaint, and routes nothing that was meant to be routed."""
    with pytest.raises(SubscriptionError, match="unknown key"):
        parse({"subscriptions": [entry]})
    assert why


def test_the_keys_we_do_know_are_still_accepted():
    """The direction that must still work."""
    subs = parse(
        {
            "subscriptions": [
                {"label": "Cortex/*", "queue": "s", "events": ["added", "removed"]}
            ]
        }
    )
    assert subs[0].events == frozenset({"added", "removed"})


@pytest.mark.parametrize("entry", ["a string", ["a", "list"], 7, None])
def test_an_entry_that_is_not_a_mapping_is_refused(entry):
    """Without the guard this is an AttributeError, which escapes reload() and
    kills the process on SIGHUP rather than being a refused reload."""
    with pytest.raises(SubscriptionError, match="must be a mapping"):
        parse({"subscriptions": [entry]})


@pytest.mark.parametrize("label", ["", "   ", "\t"])
def test_a_blank_label_is_refused(label):
    """fnmatch('anything', '') is False, so a blank label is a rule that can
    never fire -- configured, and silent."""
    with pytest.raises(SubscriptionError, match="'label' must be"):
        parse({"subscriptions": [{"label": label, "queue": "s"}]})


@pytest.mark.parametrize("queue", ["", "   "])
def test_a_blank_queue_is_refused(queue):
    """An empty queue_name inserts rows nothing ever claims."""
    with pytest.raises(SubscriptionError, match="'queue' must be|whitespace"):
        parse({"subscriptions": [{"label": "Cortex/*", "queue": queue}]})


def test_malformed_yaml_is_refused_not_raised(tmp_path):
    """An indentation typo must be a refused reload, not a crash-loop.

    reload() catches SubscriptionError and keeps the running table; a bare
    yaml.YAMLError escapes it, run() and main(). Nothing put malformed YAML
    through load() before, so the conversion was deletable.
    """
    f = tmp_path / "subs.yaml"
    f.write_text("subscriptions:\n  - label: 'Cortex/*'\n   queue: school\n")
    with pytest.raises(SubscriptionError, match="not valid YAML"):
        load(f)


def test_a_binary_file_is_refused_not_raised(tmp_path):
    f = tmp_path / "subs.yaml"
    f.write_bytes(b"\xff\xfe\x00\x01 not text")
    with pytest.raises(SubscriptionError, match="cannot read|not valid YAML"):
        load(f)


def test_matching_does_not_go_through_the_case_folding_matcher(monkeypatch):
    """Behavioural, not a source-text check.

    `fnmatch.fnmatch` lowercases through os.path.normcase, which is the
    identity on POSIX -- so no input distinguishes the two here and the
    previous test asserted the substring 'fnmatchcase' appeared in the source,
    which a comment satisfies. Poisoning the case-folding function proves which
    one is actually called.
    """
    import fnmatch as fnmatch_mod

    from actions.subscriptions import Subscription

    def poisoned(*a, **k):
        raise AssertionError("matches() used the case-folding matcher")

    monkeypatch.setattr(fnmatch_mod, "fnmatch", poisoned)

    sub = Subscription(
        label="Cortex/Family/*", queue="school", events=frozenset({"added"})
    )
    assert sub.matches("Cortex/Family/School", "added") is True


def test_the_loader_will_not_construct_python_objects(tmp_path):
    """`yaml.safe_load`, not `load`/`unsafe_load`.

    The routing table is a bind mount, re-read on every SIGHUP, so whoever can
    write that file decides what the loader does with it. Swapping in
    `unsafe_load` survived every other test in this module, because a
    constructed object fails `parse()`'s mapping check a moment later and the
    caller sees a `SubscriptionError` either way -- the difference is that the
    tag has already run by then.

    So this asserts the side effect did not happen, not the exception.
    """
    marker = tmp_path / "executed"
    f = tmp_path / "subs.yaml"
    # os.makedirs, not a Path constructor: the tag has to DO something
    # observable, or the test passes under both loaders and proves nothing.
    f.write_text(f"subscriptions: !!python/object/apply:os.makedirs ['{marker}']\n")

    with pytest.raises(SubscriptionError):
        load(f)

    assert not marker.exists(), "the loader executed a tag in the routing table"


def test_every_valid_event_is_actually_accepted():
    """`VALID_EVENTS` is a closed set that nothing closes: dropping a member
    leaves the suite green, and the member that would go is `removed` -- the
    one a subscription has to opt into, so losing it silently stops carrying
    un-labelling to consumers that asked for it."""
    for event in VALID_EVENTS:
        subs = parse(
            {"subscriptions": [{"label": "Cortex/*", "queue": "s", "events": [event]}]}
        )
        assert subs[0].matches("Cortex/Thing", event)
    assert frozenset({"added", "removed"}) == VALID_EVENTS


def test_one_label_may_fan_out_to_several_queues():
    """The duplicate-rule check keys on (label, queue), and narrowing it to
    (label,) survived -- which would refuse the fan-out this service exists to
    do. Both directions: two queues for one label is legal, the same pair twice
    is not."""
    subs = parse(
        {
            "subscriptions": [
                {"label": "Cortex/Family/School/*", "queue": "school"},
                {"label": "Cortex/Family/School/*", "queue": "vault"},
            ]
        }
    )
    assert targets(subs, "Cortex/Family/School/DPS", "added") == ["school", "vault"]

    with pytest.raises(SubscriptionError, match="duplicate"):
        parse(
            {
                "subscriptions": [
                    {"label": "Cortex/Family/School/*", "queue": "school"},
                    {"label": "Cortex/Family/School/*", "queue": "school"},
                ]
            }
        )
