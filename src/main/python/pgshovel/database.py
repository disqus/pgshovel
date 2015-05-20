import logging
import uuid
from contextlib import contextmanager

from pgshovel.utilities.postgresql import (
    ManagedConnection,
    quote,
)


logger = logging.getLogger(__name__)


def get_configuration_value(cluster, cursor, key, default=None):
    statement = 'SELECT value FROM {schema}.configuration WHERE key = %s'.format(schema=quote(cluster.schema))
    cursor.execute(statement, (key,))
    results = cursor.fetchall()
    assert len(results) <= 1
    if results:
        return results[0][0]
    else:
        return default


def set_configuration_value(cluster, cursor, key, value):
    statement = 'INSERT INTO {schema}.configuration (key, value) VALUES (%s, %s)'.format(schema=quote(cluster.schema))
    cursor.execute(statement, (key, value))


def update_configuration_value(cluster, cursor, key, value):
    statement = 'UPDATE {schema}.configuration SET value = %s WHERE key = %s'.format(schema=quote(cluster.schema))
    cursor.execute(statement, (value, key))


NODE_ID_KEY = 'node_id'

def get_node_identifier(cluster, cursor):
    node_id_raw = get_configuration_value(cluster, cursor, NODE_ID_KEY)
    if node_id_raw is not None:
        return uuid.UUID(str(node_id_raw))
    else:
        return None


def set_node_identifier(cluster, cursor):
    node_id = uuid.uuid1()
    set_configuration_value(cluster, cursor, NODE_ID_KEY, node_id.hex)
    return node_id


def get_or_set_node_identifier(cluster, cursor):
    node_id = get_node_identifier(cluster, cursor)
    if node_id:
        logger.info('Found node ID: %s', node_id)
    else:
        node_id = set_node_identifier(cluster, cursor)
        logger.info('Registered new node ID: %s', node_id)
    return node_id


class ManagedDatabase(object):
    def __init__(self, cluster, dsn, options=None):
        self.cluster = cluster
        self.dsn = dsn

        self.__id = None

        self.__connection = ManagedConnection(dsn)

    def __str__(self):
        return '%s' % (self.dsn,)

    def __repr__(self):
        # TODO: maybe add backend PID for debugging, or that could go on managed connection
        return '<%s[%s]: %s (%s)>' % (
            type(self).__name__,
            'CONNECTED' if not self.__connection.closed else 'CLOSED',
            self.dsn,
            self.__id or 'UNKNOWN',
        )

    def __discover_node_id(self, cursor):
        logger.debug('Retrieving node configuration...')
        node_id = get_node_identifier(self.cluster, cursor)
        assert node_id is not None
        logger.debug('Connected to %s as %s.', self.dsn, node_id)
        if self.__id is None:
            self.__id = node_id
        elif self.__id != node_id:
            raise Exception('Identifier mismatch: %s and %s' % (node_id, self.__id))

    @property
    def id(self):
        """
        The unique ID for the database.

        This will cause a connection to be established, if one is not already.
        """
        if self.__id is None:
            with self.connection():
                # Force the connection to be established, so that the node ID
                # will be discovered.
                pass

        return self.__id

    @contextmanager
    def connection(self):
        """
        Yields a ``psycopg2.connection`` object.

        Successive calls to this method may not necessarily connect to the
        exact same database due to DNS changes, proxy configurations, etc! If
        this is important (due to snapshot synchronization, etc.), it is the
        reponsibility of the caller to ensure that the ``id`` attribute is the
        same between accesses.

        This will cause a connection to be established, if one is not already.
        """
        was_closed = self.__connection.closed
        with self.__connection() as connection:
            # When the connection is established -- either on a new connection,
            # or a reconnection if the connection was previously closed -- we
            # need to retrieve the node ID so that we know what database we're
            # actually connected to by UUID. (Some clients may actually need to
            # ensure that this doesn't change after a disconnect -- like when a
            # DNS entry changes, for instance.)
            if was_closed:
                with connection.cursor() as cursor:
                    self.__discover_node_id(cursor)
                    connection.commit()

            yield connection
