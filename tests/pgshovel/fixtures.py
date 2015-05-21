import os
import uuid
from contextlib import closing

import psycopg2

from services import (
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
