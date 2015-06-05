import uuid
from contextlib import (
    contextmanager,
)

import pytest
from kazoo.client import KazooClient

from pgshovel.administration import (
    create_set,
    drop_set,
    initialize_cluster,
    update_set,
    upgrade_cluster,
)
from pgshovel.cluster import Cluster
from pgshovel.interfaces.configurations_pb2 import ReplicationSetConfiguration
from tests.pgshovel.fixtures import (
    create_temporary_database,
    postgres,
    zookeeper,
)


zookeeper = pytest.yield_fixture(scope='module')(zookeeper)
postgres = contextmanager(postgres)


def test_workflows(zookeeper):
    zookeeper_server, _ = zookeeper
    cluster = Cluster(
        'test_%s' % (uuid.uuid1().hex,),
        KazooClient('%s:%s' % (zookeeper_server.host, zookeeper_server.port)),
    )

    with cluster:
        initialize_cluster(cluster)

    with postgres() as (database_server, _):
        primary_dsn = create_temporary_database(database_server, 'primary')
        replica_dsn = create_temporary_database(database_server, 'replica')

        replication_set = ReplicationSetConfiguration()
        replication_set.databases.add(dsn=primary_dsn)
        replication_set.tables.add(
            name='auth_user',
            primary_keys=['id'],
        )

        with cluster:
            create_set(cluster, 'example', replication_set)

            replication_set.databases.add(dsn=replica_dsn)
            replication_set.tables.add(
                name='accounts_userprofile',
                primary_keys=['id'],
                columns=['id', 'user_id', 'display_name']
            )
            update_set(cluster, 'example', replication_set)

            upgrade_cluster(cluster, force=True)

            del replication_set.tables[0]
            del replication_set.databases[0]
            update_set(cluster, 'example', replication_set)

            drop_set(cluster, 'example')
