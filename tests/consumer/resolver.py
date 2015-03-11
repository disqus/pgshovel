import json

from pgshovel.consumer.resolver import create_simple_snapshot_builder
from pgshovel.consumer.worker import Event
from pgshovel.interfaces.groups_pb2 import GroupConfiguration
from pgshovel.snapshot import (
    Snapshot,
    State,
    Transaction,
)
from pgshovel.utilities.protobuf import TextCodec


configuration = TextCodec(GroupConfiguration).decode("""\
database {
  name: "default"
  connection {
    dsn: "postgresql://default"
  }
}
table {
  name: "user"
  primary_key: "id"
  columns: "username"
}
""")

builder = create_simple_snapshot_builder(configuration)


def test_snapshot_insert():
    event = Event(1, json.dumps({
        "operation": "INSERT",
        "transaction": {
            "id": 1,
            "time": 1,
        },
        "table": "user",
        "state": {
            "old": None,
            "new": {
                "id": 1,
                "username": "username",
                "email": "user@example.org",
            },
        },
        "version": "version",
    }))

    snapshots = builder(None, (event,))
    assert snapshots == [
        Snapshot(
            key=1,
            state=State({  # note the lack of other keys not present in configuration
                u"username": u"username",
            }),
            transaction=Transaction(id=1, timestamp=1),
        )
    ]


def test_snapshot_update_in_place():
    event = Event(1, json.dumps({
        "operation": "UPDATE",
        "transaction": {
            "id": 1,
            "time": 1,
        },
        "table": "user",
        "state": {
            "old": {
                "id": 1,
                "username": "username",
            },
            "new": {
                "id": 1,
                "username": "new username",
            },
        },
        "version": "version",
    }))

    snapshots = builder(None, (event,))
    assert snapshots == [
        Snapshot(
            key=1,
            state=State({
                u"username": u"new username",
            }),
            transaction=Transaction(id=1, timestamp=1),
        ),
    ]


def test_snapshot_update_moved():
    event = Event(1, json.dumps({
        "operation": "UPDATE",
        "transaction": {
            "id": 1,
            "time": 1,
        },
        "table": "user",
        "state": {
            "old": {
                "id": 1,
                "username": "username",
            },
            "new": {
                "id": 2,
                "username": "username",
            },
        },
        "version": "version",
    }))

    snapshots = builder(None, (event,))
    assert sorted(snapshots) == [
        Snapshot(
            key=1,
            state=None,
            transaction=Transaction(id=1, timestamp=1),
        ),
        Snapshot(
            key=2,
            state=State({
                u"username": u"username",
            }),
            transaction=Transaction(id=1, timestamp=1),
        ),
    ]


def test_snapshot_delete():
    event = Event(1, json.dumps({
        "operation": "DELETE",
        "transaction": {
            "id": 1,
            "time": 1,
        },
        "table": "user",
        "state": {
            "old": {
                "id": 1,
                "username": "username",
            },
        },
        "version": "version",
    }))

    snapshots = builder(None, (event,))
    assert snapshots == [
        Snapshot(
            key=1,
            state=None,
            transaction=Transaction(id=1, timestamp=1),
        ),
    ]


events = (
    Event(1, json.dumps({
        "operation": "INSERT",
        "transaction": {
            "id": 1,
            "time": 1,
        },
        "table": "user",
        "state": {
            "new": {
                "id": 1,
                "username": "username",
            },
        },
        "version": "version",
    })),
    Event(2, json.dumps({
        "operation": "UPDATE",
        "transaction": {
            "id": 1,
            "time": 1,
        },
        "table": "user",
        "state": {
            "old": {
                "id": 1,
                "username": "username",
            },
            "new": {
                "id": 1,
                "username": "new username",
            },
        },
        "version": "version",
    })),
)


def test_snapshot_no_compact():
    builder = create_simple_snapshot_builder(configuration, compact=False)
    snapshots = builder(None, events)
    assert len(snapshots) == 2

    assert snapshots[0].state == State({
        u"username": u"username",
    })

    assert snapshots[1].state == State({
        u"username": u"new username",
    })


def test_snapshot_compact():
    builder = create_simple_snapshot_builder(configuration, compact=True)
    snapshots = builder(None, events)
    assert len(snapshots) == 1

    assert snapshots[0].state == State({
        u"username": u"new username",
    })
