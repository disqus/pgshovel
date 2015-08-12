import os
import signal
import uuid
from Queue import Queue
from contextlib import closing

import psycopg2
import pytest

from pgshovel.administration import create_set
from pgshovel.interfaces.common_pb2 import (
    Column,
    Row,
)
from pgshovel.interfaces.configurations_pb2 import ReplicationSetConfiguration
from pgshovel.interfaces.streams_pb2 import (
    BeginOperation,
    CommitOperation,
    MutationOperation,
    RollbackOperation,
)
from pgshovel.relay.relay import (
    Relay,
    Worker,
)
from pgshovel.streams.batches import get_operation
from tests.pgshovel.fixtures import (
    cluster,
    create_temporary_database,
)
from tests.pgshovel.streams.fixtures import reserialize


def configure_tick_frequency(dsn):
    with closing(psycopg2.connect(dsn)) as connection, connection.cursor() as cursor:
        cursor.execute("UPDATE pgq.queue SET queue_ticker_max_lag = %s, queue_ticker_idle_period = %s", ('0', '0'))
        connection.commit()


def create_set_configuration(dsn):
    replication_set = ReplicationSetConfiguration()
    replication_set.database.dsn = dsn

    replication_set.tables.add(
        name='auth_user',
        columns=['id', 'username'],
        primary_keys=['id'],
    )
    replication_set.tables.add(
        name='accounts_userprofile',
        primary_keys=['id'],
    )

    return replication_set


def force_tick(connection, queue):
    with connection.cursor() as cursor:
        cursor.execute('SELECT * FROM pgq.ticker(%s)', (queue,))
        connection.commit()


class QueueHandler(object):
    def __init__(self, queue):
        self.queue = queue

    def push(self, items):
        for item in items:
            self.queue.put(reserialize(item))


def get_events(queue, n):
    events = []
    for _ in xrange(n):
        events.append(queue.get(True, 1))
    return events


def assert_same_batch(events):
    # Protocol buffer messages aren't hashable, so we have to test against the
    # serialized immutable form.
    assert len(set([get_operation(event).batch_identifier.SerializeToString() for event in events])) == 1


def unwrap_transaction(events):
    operations = map(lambda event: get_operation(get_operation(event)), events)
    assert isinstance(operations[0], BeginOperation)
    assert isinstance(operations[-1], (CommitOperation, RollbackOperation))
    assert events[0].batch_operation.batch_identifier == events[-1].batch_operation.batch_identifier
    return operations[1:-1]



def test_worker(cluster):
    dsn = create_temporary_database()

    create_set(cluster, 'example', create_set_configuration(dsn))
    configure_tick_frequency(dsn)

    queue = Queue()
    worker = Worker(cluster, dsn, 'example', 'consumer', QueueHandler(queue))
    worker.start()

    with closing(psycopg2.connect(dsn)) as connection, connection.cursor() as cursor:
        cursor.execute('INSERT INTO auth_user (username) VALUES (%s)', ('example',))
        connection.commit()
        force_tick(connection, cluster.get_queue_name('example'))

    events = get_events(queue, 3)
    assert_same_batch(events)
    (mutation,) = unwrap_transaction(events)

    assert mutation.table == 'auth_user'
    assert mutation.schema == 'public'
    assert mutation.operation == MutationOperation.INSERT
    assert not mutation.HasField('old')
    assert sorted(mutation.new.columns, key=lambda c: c.name) == [
        Column(name='id', integer64=1),
        Column(name='username', string='example'),
    ]

    # also make sure tables without column whitelist defined replicate the entire row state
    with closing(psycopg2.connect(dsn)) as connection, connection.cursor() as cursor:
        cursor.execute('INSERT INTO accounts_userprofile (user_id, display_name) VALUES (%s, %s)', (1, 'example',))
        connection.commit()
        force_tick(connection, cluster.get_queue_name('example'))

    events = get_events(queue, 3)
    assert_same_batch(events)
    (mutation,) = unwrap_transaction(events)

    assert mutation.table == 'accounts_userprofile'
    assert mutation.schema == 'public'
    assert mutation.operation == MutationOperation.INSERT
    assert not mutation.HasField('old')
    assert sorted(mutation.new.columns, key=lambda c: c.name) == [
        Column(name='display_name', string='example'),
        Column(name='id', integer64=1),
        Column(name='user_id', integer64=1),
    ]

    worker.stop_async()
    worker.result(1)


def test_relay(cluster):
    primary_dsn = create_temporary_database()
    secondary_dsn = create_temporary_database()

    create_set(cluster, 'example', create_set_configuration(primary_dsn))
    configure_tick_frequency(primary_dsn)

    queue = Queue()
    relay = Relay(cluster, 'example', 'consumer', QueueHandler(queue), throttle=0.1)
    relay.start()

    with closing(psycopg2.connect(primary_dsn)) as connection, connection.cursor() as cursor:
        cursor.execute('INSERT INTO auth_user (username) VALUES (%s)', ('example',))
        connection.commit()
        force_tick(connection, cluster.get_queue_name('example'))

    events = get_events(queue, 3)
    assert_same_batch(events)
    (mutation,) = unwrap_transaction(events)

    assert mutation.table == 'auth_user'
    assert mutation.schema == 'public'
    assert mutation.operation == MutationOperation.INSERT
    assert not mutation.HasField('old')
    assert sorted(mutation.new.columns, key=lambda c: c.name) == [
        Column(name='id', integer64=1),
        Column(name='username', string='example'),
    ]

    # ensure the connection recovers after being killed
    with closing(psycopg2.connect(primary_dsn)) as connection, connection.cursor() as cursor:
        connection.autocommit = True
        cursor.execute('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid != pg_backend_pid()')

    with closing(psycopg2.connect(primary_dsn)) as connection, connection.cursor() as cursor:
        cursor.execute('INSERT INTO auth_user (username) VALUES (%s)', ('example',))
        connection.commit()
        force_tick(connection, cluster.get_queue_name('example'))

    events = get_events(queue, 3)
    assert_same_batch(events)
    (mutation,) = unwrap_transaction(events)

    assert mutation.table == 'auth_user'
    assert mutation.schema == 'public'
    assert mutation.operation == MutationOperation.INSERT
    assert not mutation.HasField('old')
    assert sorted(mutation.new.columns, key=lambda c: c.name) == [
        Column(name='id', integer64=2),
        Column(name='username', string='example'),
    ]

    relay.stop_async()
    relay.result(1)

    """
    # also test it's ability to handle zookeeper disconnection
    relay = Relay(cluster, 'example', 'consumer', QueueHandler(queue), throttle=0.1)
    relay.start()

    zookeeper_server, _ = zookeeper

    zookeeper_server.stop()
    relay.result(10)

    # XXX: have to restart for services rn, need to fix
    zookeeper_server.start()
    """
