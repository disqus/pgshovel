import logging
import os
import subprocess
import uuid
from contextlib import contextmanager

import pytest
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from pgshovel.administration import (
    create_group,
    drop_groups,
    fetch_groups,
    get_connection_for_database,
    get_version,
    initialize_cluster,
    move_groups,
    update_group,
)
from pgshovel.application import (
    Application,
    Environment,
)
from pgshovel.interfaces.application_pb2 import (
    ApplicationConfiguration,
    EnvironmentConfiguration,
)
from pgshovel.interfaces.groups_pb2 import (
    DatabaseConfiguration,
    GroupConfiguration,
    TableConfiguration,
)
from pgshovel.utilities.protobuf import TextCodec


logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s - %(processName)s:%(process)d - %(threadName)s:%(thread)d - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG,
)  # TODO: Something more intelligent.


@contextmanager
def TemporaryDatabase(alias):
    name = 'pgshovel_test_%s' % (uuid.uuid1().hex,)
    subprocess.check_call(('createdb', name))
    dsn = 'postgresql:///%s?application_name=pgshovel-test-%s' % (name, os.getpid())
    try:
        yield DatabaseConfiguration(
            name=alias,
            connection=DatabaseConfiguration.ConnectionConfiguration(dsn=dsn)
        )
    finally:
        try:
            subprocess.check_call(('dropdb', name))
        except Exception:
            logger.exception('Caught exception trying to delete temporary database: %r', name)


@contextmanager
def TemporaryZooKeeper():
    host = 'localhost:2181'
    path = 'pgshovel-test-%s' % (uuid.uuid1().hex,)

    zookeeper = KazooClient(host)
    zookeeper.start()
    zookeeper.create(path)
    zookeeper.stop()

    yield EnvironmentConfiguration.ZooKeeperConfiguration(hosts='/'.join((host, path)))


@contextmanager
def TemporaryEnvironment():
    with TemporaryZooKeeper() as zookeeper:
        configuration = EnvironmentConfiguration(zookeeper=zookeeper)
        yield Environment(configuration)


@contextmanager
def TemporaryApplication():
    name = 'test_%s' % (uuid.uuid1().hex,)
    with TemporaryEnvironment() as environment:
        configuration = ApplicationConfiguration(name=name)
        yield Application(environment, configuration)


RESOURCE_PATH = os.path.join(os.path.dirname(__file__), 'resources')

def resource(*parts):
    return open(os.path.join(RESOURCE_PATH, *parts))


def test_simple():
    def configure(database, schema):
        connection = get_connection_for_database(database)
        with connection.cursor() as cursor:
            cursor.execute(schema)
            connection.commit()
        connection.close()

    with TemporaryDatabase('primary') as database_primary, \
            TemporaryDatabase('replica') as database_replica, \
            TemporaryApplication() as application, \
            application:

        with resource('sql', 'forums.sql') as f:
            schema = f.read()
            configure(database_primary, schema)
            configure(database_replica, schema)

        initialize_cluster(application)

        with resource('tables', 'users') as f:
            configuration = GroupConfiguration(
                database=database_primary,
                table=TextCodec(TableConfiguration).decode(f.read()),
            )

        create_group(application, 'users', configuration)
        group = dict(fetch_groups(application, ('users',)))['users']
        assert group[0] == configuration
        assert group[1].version is 0

        update_group(application, 'users@%s' % get_version(configuration), configuration)
        group = dict(fetch_groups(application, ('users',)))['users']
        assert group[0] == configuration
        assert group[1].version is 1

        move_groups(application, ('users',), database_replica)
        group = dict(fetch_groups(application, ('users',)))['users']
        assert str(group[0].database) == str(database_replica)
        assert group[1].version is 2

        update_group(application, 'users', configuration)
        group = dict(fetch_groups(application, ('users',)))['users']
        assert group[0] == configuration
        assert group[1].version is 3

        drop_groups(application, ('users',))
        with pytest.raises(NoNodeError):
            dict(fetch_groups(application, ('users',)))
