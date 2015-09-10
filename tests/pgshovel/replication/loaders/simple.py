from contextlib import closing

import psycopg2
from tests.pgshovel.fixtures import (
    cluster,
    create_temporary_database
)

from pgshovel.administration import (
    create_set,
    drop_set,
    update_set,
    upgrade_cluster,
)
from pgshovel.database import get_node_identifier
from pgshovel.interfaces.configurations_pb2 import ReplicationSetConfiguration
from pgshovel.replication.loaders.simple import SimpleLoader


def test_yields_a_bootstrap_state_containing_node_and_snapshot_value(cluster):
    dsn = create_temporary_database('primary')
    connection = psycopg2.connect(dsn)

    with connection as conn, conn.cursor() as cursor:
        cursor.executemany(
            'INSERT INTO auth_user (username) VALUES (%s)',
            (('example',), ('example2',))
        )

    with connection as conn, conn.cursor() as cursor:
        replication_set = ReplicationSetConfiguration()
        replication_set.database.dsn = dsn
        replication_set.tables.add(name='auth_user', primary_keys=['id'])

        with cluster:
            create_set(cluster, 'example', replication_set)
            with SimpleLoader(cluster, 'example').fetch() as (state, loaders):
                assert state.node == get_node_identifier(cluster, cursor).bytes

                ((table_config, stream),) = loaders
                assert table_config.name == 'auth_user'
                assert table_config.primary_keys == ['id']

                values = [
                    (c.name, c.string)
                    for row in stream
                    for c in row.columns
                ]
                assert values == [
                    (u'id', u''),
                    (u'username', u'example'),
                    (u'id', u''),
                    (u'username', u'example2')
                ]