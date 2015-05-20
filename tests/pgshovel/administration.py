import os
import uuid
from ConfigParser import SafeConfigParser
from contextlib import (
    closing,
    contextmanager,
)

import psycopg2
import pytest

from pgshovel.administration import (
    create_set,
    drop_set,
    initialize_cluster,
    update_set,
)
from pgshovel.cluster import Cluster
from pgshovel.interfaces.configurations_pb2 import ReplicationSetConfiguration
from services import (
    Postgres,
    ZooKeeper,
    get_open_port,
)


@pytest.yield_fixture(scope='module')
def zookeeper():
    server = ZooKeeper(
        os.environ['ZOOKEEPER_PATH'],
        host='localhost',
        port=get_open_port(),
    )
    server.setup()
    server.start()
    yield server
    server.stop()
    server.teardown()


@contextmanager
def database():
    server = Postgres(
        os.environ['POSTGRES_PATH'],
        host='localhost',
        port=get_open_port(),
        max_prepared_transactions=10,  # XXX
    )
    server.setup()
    server.start()
    try:
        yield server
    finally:
        server.stop()
        server.teardown()


schema = """\
CREATE TABLE auth_user (
    id bigint PRIMARY KEY NOT NULL,
    username varchar(250) NOT NULL
);
CREATE TABLE accounts_userprofile (
    id bigint PRIMARY KEY NOT NULL,
    user_id bigint REFERENCES "auth_user" ("id"),
    display_name varchar(250)
);
"""


def test_workflows(zookeeper):
    configuration = SafeConfigParser()
    configuration.add_section('zookeeper')
    configuration.set('zookeeper', 'hosts', '%s:%s' % (zookeeper.host, zookeeper.port))
    cluster = Cluster(
        'test_%s' % (uuid.uuid1().hex,),
        configuration,
    )

    with cluster:
        initialize_cluster(cluster)

    def setup_database(database):
        base = 'postgresql://%s:%s' % (database.host, database.port)
        name = 'test_%s' % (uuid.uuid1().hex,)
        with closing(psycopg2.connect(base + '/postgres')) as connection, \
                connection.cursor() as cursor:
            connection.autocommit = True
            cursor.execute('CREATE DATABASE %s' % (name,))

        dsn = base + '/' + name
        with closing(psycopg2.connect(dsn)) as connection, \
                connection.cursor() as cursor:
            cursor.execute(schema)
            connection.commit()

        return dsn

    with database() as primary, database() as replica:
        databases = map(setup_database, [primary, replica])

        replication_set = ReplicationSetConfiguration()
        replication_set.databases.add(dsn=databases[0])
        replication_set.tables.add(
            name='auth_user',
            columns=['id', 'username'],
        )

        with cluster:
            create_set(cluster, 'example', replication_set)

            replication_set.databases.add(dsn=databases[1])
            replication_set.tables.add(
                name='accounts_userprofile',
                columns=['id', 'user_id', 'display_name']
            )
            update_set(cluster, 'example', replication_set)

            del replication_set.tables[0]
            del replication_set.databases[0]
            update_set(cluster, 'example', replication_set)

            drop_set(cluster, 'example')
