import threading
from contextlib import contextmanager

from psycopg2.extras import NamedTupleCursor

from pgshovel.administration import fetch_sets
from pgshovel.database import ManagedDatabase
from pgshovel.interfaces.replication_pb2 import BootstrapState
from pgshovel.utilities.conversions import (
    row_converter,
    to_snapshot,
)
from pgshovel.utilities.postgresql import quote


def fetch_set(cluster, set):
    return dict(fetch_sets(cluster, (set,)))[set]


class SimpleLoader(object):
    def __init__(self, cluster, set):
        self.cluster = cluster
        self.set = set

    @contextmanager
    def fetch(self):
        # TODO: Might need to use the znode version here just to be safe
        # to avoid any race conditions? What happens if the set configuraton
        # is updated while this is starting?
        configuration, _ = fetch_set(self.cluster, self.set)
        database = ManagedDatabase(self.cluster, configuration.database.dsn)

        connection_lock = threading.Lock()

        with database.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SELECT txid_current_snapshot();')
                row = cursor.fetchone()
                snapshot = to_snapshot(row[0])

            def loader(table):
                with connection_lock, connection.cursor('records', cursor_factory=NamedTupleCursor) as cursor:
                    if table.columns:
                        columns = ', '.join(map(quote, table.columns))
                    else:
                        columns = '*'

                    statement = 'SELECT {columns} FROM {schema}.{name}'.format(
                        columns=columns,
                        schema=quote(table.schema),
                        name=quote(table.name),
                    )

                    cursor.execute(statement)
                    for row in cursor:
                        converted = row_converter.to_protobuf(row._asdict())
                        # XXX: This is necessary because of a bug in protocol buffer oneof.
                        yield type(converted).FromString(converted.SerializeToString())

            loaders = []
            for table in configuration.tables:
                loaders.append((table, loader(table)))

            state = BootstrapState(
                node=database.id.bytes,
                snapshot=snapshot,
            )

            yield state, loaders

            connection.commit()

    @classmethod
    def configure(cls, configuration, cluster, set):
        return cls(cluster, set)
