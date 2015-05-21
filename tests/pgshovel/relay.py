import os
import signal
import uuid
from ConfigParser import SafeConfigParser
from Queue import Queue
from contextlib import (
    closing,
    contextmanager,
)

import psycopg2
import pytest

from pgshovel.administration import (
    create_set,
    initialize_cluster,
)
from pgshovel.cluster import Cluster
from pgshovel.interfaces.configurations_pb2 import ReplicationSetConfiguration
from pgshovel.relay import (
    Relay,
    Worker,
)
from tests.pgshovel.fixtures import (
    create_temporary_database,
    postgres,
    zookeeper,
)


zookeeper = pytest.yield_fixture(scope='module')(zookeeper)
postgres = contextmanager(postgres)


@pytest.yield_fixture
def cluster(zookeeper):
    zookeeper_server, _ = zookeeper

    configuration = SafeConfigParser()
    configuration.add_section('zookeeper')
    configuration.set('zookeeper', 'hosts', '%s:%s' % (zookeeper_server.host, zookeeper_server.port))
    cluster = Cluster('test_%s' % (uuid.uuid1().hex,),configuration,)

    with cluster:
        initialize_cluster(cluster)
        yield cluster


def setup_cluster(cluster, dsns):
    replication_set = ReplicationSetConfiguration()
    for dsn in dsns:
        replication_set.databases.add(dsn=dsn)
    replication_set.tables.add(name='auth_user', columns=['id', 'username'])
    replication_set.tables.add(name='accounts_userprofile', columns=['id', 'user_id', 'display_name'])
    create_set(cluster, 'example', replication_set)

    for dsn in dsns:
        with closing(psycopg2.connect(dsn)) as connection, connection.cursor() as cursor:
            cursor.execute("UPDATE pgq.queue SET queue_ticker_max_lag = %s, queue_ticker_idle_period = %s", ('0', '0'))
            connection.commit()


def force_tick(connection, queue):
    with connection.cursor() as cursor:
        cursor.execute('SELECT * FROM pgq.ticker(%s)', (queue,))
        connection.commit()


class QueueHandler(object):
    def __init__(self, queue):
        self.queue = queue

    def push(self, batch):
        self.queue.put(batch)


def test_worker(cluster):
    with postgres(create_temporary_database) as (_, dsn):
        setup_cluster(cluster, (dsn,))

        queue = Queue()
        worker = Worker(cluster, dsn, 'example', 'consumer', QueueHandler(queue))
        worker.start()

        with closing(psycopg2.connect(dsn)) as connection, connection.cursor() as cursor:
            cursor.execute('INSERT INTO auth_user (username) VALUES (%s)', ('example',))
            connection.commit()
            force_tick(connection, cluster.get_queue_name('example'))

        (event,) = queue.get(True, 1).events

        assert event.table == 'auth_user'
        assert event.operation == 'INSERT'
        assert event.states == (None, {
            'id': 1,
            'username': 'example',
        })

        worker.stop_async()
        worker.result(1)


def test_relay(zookeeper, cluster):
    with postgres(create_temporary_database) as (primary_database, primary_dsn), \
            postgres(create_temporary_database) as (_, secondary_dsn):
        setup_cluster(cluster, (primary_dsn, secondary_dsn))

        queue = Queue()
        relay = Relay(cluster, 'example', 'consumer', QueueHandler(queue), throttle=0.1)
        relay.start()

        for dsn in (primary_dsn, secondary_dsn):
            with closing(psycopg2.connect(dsn)) as connection, connection.cursor() as cursor:
                cursor.execute('INSERT INTO auth_user (username) VALUES (%s)', ('example',))
                connection.commit()
                force_tick(connection, cluster.get_queue_name('example'))

        batches = []
        for _ in (primary_dsn, secondary_dsn):
            batches.append(queue.get(True, 1))

        for batch in batches:
            (event,) = batch.events
            assert event.table == 'auth_user'
            assert event.operation == 'INSERT'
            assert event.states == (None, {
                'id': 1,
                'username': 'example',
            })

        # ensure the connection recovers after being killed
        with closing(psycopg2.connect(primary_dsn)) as connection, connection.cursor() as cursor:
            connection.autocommit = True
            cursor.execute('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid != pg_backend_pid()')

        with closing(psycopg2.connect(primary_dsn)) as connection, connection.cursor() as cursor:
            cursor.execute('INSERT INTO auth_user (username) VALUES (%s)', ('example',))
            connection.commit()
            force_tick(connection, cluster.get_queue_name('example'))

        batch = queue.get(True, 1)
        (event,) = batch.events
        assert event.table == 'auth_user'
        assert event.operation == 'INSERT'
        assert event.states == (None, {
            'id': 2,
            'username': 'example',
        })

        # XXX: (don't try this at home, need to refactor services to expose pid directly)
        with open(os.path.join(primary_database.tmp_dir, 'postmaster.pid')) as f:
            os.kill(int(f.readline()), signal.SIGINT)

        with pytest.raises(psycopg2.OperationalError):
            psycopg2.connect(primary_dsn)

        # ensure that the second database is still operational
        with closing(psycopg2.connect(secondary_dsn)) as connection, connection.cursor() as cursor:
            cursor.execute('INSERT INTO auth_user (username) VALUES (%s)', ('example',))
            connection.commit()
            force_tick(connection, cluster.get_queue_name('example'))

        batch = queue.get(True, 1)
        (event,) = batch.events
        assert event.table == 'auth_user'
        assert event.operation == 'INSERT'
        assert event.states == (None, {
            'id': 2,
            'username': 'example',
        })

        relay.stop_async()
        relay.result(1)

        # also test it's ability to handle zookeeper disconnection
        relay = Relay(cluster, 'example', 'consumer', QueueHandler(queue), throttle=0.1)
        relay.start()

        zookeeper_server, _ = zookeeper

        zookeeper_server.stop()
        relay.result(10)

        # XXX: have to restart for services rn, need to fix
        zookeeper_server.start()
