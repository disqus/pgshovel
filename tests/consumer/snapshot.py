import cPickle as pickle
import json

from pgshovel.consumer.snapshot import create_simple_snapshot_builder
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


def payload(table, event, states, version, transaction):
    return '0:%s' % pickle.dumps((
        table,
        event,
        states,
        version,
        transaction,
    ))


def test_snapshot_insert():
    event = Event(1, payload(
        'user',
        'INSERT',
        (
            None,
            {
                "id": 1,
                "username": "username",
                "email": "user@example.org",
            },
        ),
        'version',
        (1, 1),
    ))

    snapshots = list(builder(None, (event,)))
    assert snapshots == [
        Snapshot(
            key=1,
            state=State({  # note the lack of other keys not present in configuration
                "username": "username",
            }),
            transaction=Transaction(id=1, timestamp=1),
        )
    ]


def test_snapshot_update_in_place():
    event = Event(1, payload(
        'user',
        'UPDATE',
        (
            {
                "id": 1,
                "username": "username",
            },
            {
                "id": 1,
                "username": "new username",
            },
        ),
        'version',
        (1, 1),
    ))

    snapshots = list(builder(None, (event,)))
    assert snapshots == [
        Snapshot(
            key=1,
            state=State({
                "username": "new username",
            }),
            transaction=Transaction(id=1, timestamp=1),
        ),
    ]


def test_snapshot_update_moved():
    event = Event(1, payload(
        'user',
        'UPDATE',
        (
            {
                "id": 1,
                "username": "username",
            },
            {
                "id": 2,
                "username": "new username",
            },
        ),
        'version',
        (1, 1),
    ))

    snapshots = list(builder(None, (event,)))
    assert sorted(snapshots) == [
        Snapshot(
            key=1,
            state=None,
            transaction=Transaction(id=1, timestamp=1),
        ),
        Snapshot(
            key=2,
            state=State({
                "username": "new username",
            }),
            transaction=Transaction(id=1, timestamp=1),
        ),
    ]


def test_snapshot_delete():
    event = Event(1, payload(
        'user',
        'DELETE',
        (
            {
                "id": 1,
                "username": "username",
            },
            None,
        ),
        'version',
        (1, 1),
    ))

    snapshots = list(builder(None, (event,)))
    assert snapshots == [
        Snapshot(
            key=1,
            state=None,
            transaction=Transaction(id=1, timestamp=1),
        ),
    ]


events = (
    Event(1, payload(
        'user',
        'INSERT',
        (
            None,
            {
                "id": 1,
                "username": "username",
            },
        ),
        'version',
        (1, 1),
    )),
    Event(2, payload(
        'user',
        'UPDATE',
        (
            {
                "id": 1,
                "username": "username",
            },
            {
                "id": 1,
                "username": "new username",
            },
        ),
        'version',
        (1, 1),
    )),
)


def test_snapshot_no_compact():
    builder = create_simple_snapshot_builder(configuration, compact=False)
    snapshots = list(builder(None, events))
    assert len(snapshots) == 2

    assert snapshots[0].state == State({
        "username": "username",
    })

    assert snapshots[1].state == State({
        "username": "new username",
    })


def test_snapshot_compact():
    builder = create_simple_snapshot_builder(configuration, compact=True)
    snapshots = list(builder(None, events))
    assert len(snapshots) == 1

    assert snapshots[0].state == State({
        "username": "new username",
    })
