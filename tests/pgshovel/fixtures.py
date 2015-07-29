import random
import string
import uuid
from contextlib import closing

import psycopg2
import pytest
from kazoo.client import KazooClient

from pgshovel.administration import initialize_cluster
from pgshovel.cluster import Cluster
from pgshovel.testing import (
    BatchBuilder,
    EventBuilder,
)


DEFAULT_SCHEMA = """\
CREATE TABLE auth_user (
    id bigserial PRIMARY KEY NOT NULL,
    username varchar(250) NOT NULL
);
CREATE TABLE accounts_userprofile (
    id bigserial PRIMARY KEY NOT NULL,
    user_id bigint UNIQUE REFERENCES "auth_user" ("id"),
    display_name varchar(250)
);
"""


@pytest.yield_fixture
def cluster():
    cluster = Cluster(
        'test_%s' % (uuid.uuid1().hex,),
        KazooClient('zookeeper'),
    )

    with cluster:
        initialize_cluster(cluster)
        yield cluster


def create_temporary_database(prefix='test', schema=DEFAULT_SCHEMA):
    base = 'postgresql://postgres@postgres'

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
