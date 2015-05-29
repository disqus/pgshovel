import os
import random
import string
import uuid
from contextlib import closing

import psycopg2

from pgshovel.testing import (
    BatchBuilder,
    EventBuilder,
)
from services import (
    Kafka,
    Postgres,
    ZooKeeper,
    get_open_port,
)


DEFAULT_SCHEMA = """\
CREATE TABLE auth_user (
    id bigserial PRIMARY KEY NOT NULL,
    username varchar(250) NOT NULL
);
CREATE TABLE accounts_userprofile (
    id bigserial PRIMARY KEY NOT NULL,
    user_id bigint REFERENCES "auth_user" ("id"),
    display_name varchar(250)
);
"""

def noop(*args, **kwargs):
    pass


def zookeeper(setup=noop):
    server = ZooKeeper(
        os.environ['ZOOKEEPER_PATH'],
        host='localhost',
        port=get_open_port(),
    )
    server.setup()
    server.start()
    try:
        yield server, setup(server)
    finally:
        server.stop()  # XXX: needs to be failure tolerant
        server.teardown()


def postgres(setup=noop):
    server = Postgres(
        os.environ['POSTGRES_PATH'],
        host='localhost',
        port=get_open_port(),
        max_prepared_transactions=10,  # XXX
    )
    server.setup()
    server.start()
    try:
        yield server, setup(server)
    finally:
        server.stop()  # XXX: needs to be failure tolerant
        server.teardown()


def kafka(zookeeper, broker_id=1, setup=noop):
    # TODO: probably would make sense to chroot the zookeeper path
    zookeeper_server, _ = zookeeper
    broker = Kafka(
        os.environ['KAFKA_PATH'],
        host='localhost',
        port=get_open_port(),
        configuration={
            'broker.id': broker_id,
            'zookeeper.connect': '%s:%s' % (zookeeper_server.host, zookeeper_server.port),
        },
    )
    broker.setup()
    broker.start()
    try:
        yield broker, setup(broker)
    finally:
        broker.stop()  # XXX: needs to be failure tolerant
        broker.teardown()


def create_temporary_database(server, prefix='test', schema=DEFAULT_SCHEMA):
    base = 'postgresql://%s:%s' % (server.host, server.port)

    name = '%s_%s' % (prefix,  uuid.uuid1().hex,)
    dsn = base + '/postgres'
    with closing(psycopg2.connect(dsn)) as connection, connection.cursor() as cursor:
        connection.autocommit = True
        cursor.execute('CREATE DATABASE %s' % (name,))

    dsn = base + '/' + name
    with closing(psycopg2.connect(dsn)) as connection, connection.cursor() as cursor:
        cursor.execute(schema)
        connection.commit()

    return dsn


def generate_random_string(length, characters=string.letters + string.digits):
    return ''.join(random.choice(characters) for _ in xrange(length))


batch_builder = BatchBuilder((
    EventBuilder(
        'auth_user',
        lambda: {
            'id': random.randint(0, 1e7),
            'username': generate_random_string(10),
        },
    ),
))