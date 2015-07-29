import uuid

from pgshovel.administration import (
    create_set,
    drop_set,
    update_set,
    upgrade_cluster,
)
from pgshovel.interfaces.configurations_pb2 import ReplicationSetConfiguration
from tests.pgshovel.fixtures import (
    cluster,
    create_temporary_database
)


def test_workflows(cluster):
    primary_dsn = create_temporary_database('primary')
    replica_dsn = create_temporary_database('replica')

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
