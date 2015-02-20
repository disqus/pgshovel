import collections
import functools
import hashlib
import logging
import operator
from contextlib import contextmanager
from string import Template

import psycopg2

from pgshovel.interfaces.groups_pb2 import GroupConfiguration
from pgshovel.utilities.postgresql import (
    Transaction,
    managed,
)
from pgshovel.utilities.protobuf import BinaryCodec
from pgshovel.utilities.templates import resource_string
from pgshovel.utilities.zookeeper import commit


logger = logging.getLogger(__name__)


def get_connection_for_database(database):
    """
    Returns a ``psycopg2.connection`` for a ``DatabaseConfiguration``.
    """
    return psycopg2.connect(database.connection.dsn)


def get_version(configuration):
    """
    Returns a MD5 hash (version identifier) for a configuration object.
    """
    return hashlib.md5(configuration.SerializeToString()).hexdigest()


def configure_database(application, cursor):
    """
    Configures a database (the provided cursor) for use with pgshovel.

    This function can also be used to repair a broken installation, or update
    an existing installation's capture function.
    """
    # Install PGQ if it doesn't already exist.
    logger.info('Creating PGQ extension...')
    cursor.execute('CREATE EXTENSION IF NOT EXISTS pgq')

    # Install pypythonu if it doesn't already exist.
    logger.info('Creating pypythonu langauge...')
    cursor.execute('CREATE OR REPLACE LANGUAGE plpythonu')

    # Create the schema if it doesn't already exist.
    logger.info('Creating schema...')
    cursor.execute('CREATE SCHEMA IF NOT EXISTS %s' % (application.schema,))

    # Install the capture function if it doesn't already exist.
    logger.info('Creating capture function...')
    cursor.execute(
        Template(resource_string('sql/create-capture-function.sql')).substitute({
            'schema': application.schema,
        })
    )


def trigger_name(application, name):
    return '_pgshovel_%s_%s_capture' % (application.configuration.name, name)


def collect_tables(table):
    """
    Returns a dictionary, keyed by table name, containing all columns that
    are watched in the associated table.
    """
    tables = collections.defaultdict(set)

    def __collect(table):
        columns = tables[table.name]
        columns.add(table.primary_key)
        columns.update(table.columns)

        for join in table.joins:
            tables[join.table.name].add(join.foreign_key)
            __collect(join.table)

    __collect(table)

    return tables


def configure_group(application, cursor, name, configuration):
    """
    Configures a capture group using the provided name and configuration data.
    """
    # Create the transaction queue if it doesn't already exist.
    logger.info('Creating transaction queue...')
    cursor.execute("SELECT pgq.create_queue(%s)", (application.get_queue_name(name),))

    tables = collect_tables(configuration.table)

    trigger = trigger_name(application, name)

    def create_trigger(table, columns):
        logger.info('Installing capture trigger on %s...', table)

        statement = """
            CREATE TRIGGER %(name)s
            AFTER INSERT OR UPDATE OF %(columns)s OR DELETE
            ON %(table)s
            FOR EACH ROW EXECUTE PROCEDURE %(schema)s.capture(%%s, %%s)
        """ % {
            'name': trigger,
            'columns': ', '.join(columns),
            'table': table,
            'schema': application.schema,
        }

        cursor.execute("DROP TRIGGER IF EXISTS %s ON %s" % (trigger, table))
        cursor.execute(statement, (
            application.get_queue_name(name),
            get_version(configuration)),
        )

    for table, columns in tables.items():
        create_trigger(table, columns)


def drop_trigger(application, cursor, name, table):
    """
    Drops a capture trigger on the provided table for the specified group.
    """
    logger.info('Dropping capture trigger on %s...', table)
    cursor.execute('DROP TRIGGER %s ON %s' % (trigger_name(application, name), table))


def unconfigure_group(application, cursor, name, configuration):
    """
    Removes all triggers and capture queue for the provided group name and
    configuration.
    """
    # Drop the transaction queue if it exists.
    logger.info('Dropping transaction queue...')
    cursor.execute("SELECT pgq.drop_queue(%s)", (application.get_queue_name(name),))

    # Drop the triggers on the destination table.
    def uninstall_triggers(table):
        drop_trigger(application, cursor, name, table.name)
        for join in table.joins:
            uninstall_triggers(join.table)

    uninstall_triggers(configuration.table)


def initialize_cluster(application):
    """
    Initialize a pgshovel cluster in ZooKeeper.
    """
    logger.info('Creating a new cluster for %s...', application)

    ztransaction = application.environment.zookeeper.transaction()
    ztransaction.create(application.path)
    ztransaction.create(application.get_group_path())
    commit(ztransaction)


def create_group(application, name, configuration):
    """
    Create a capture group for the provided configuration.
    """
    connection = get_connection_for_database(configuration.database)

    transaction = Transaction(connection, 'create-group:%s' % (name,))
    with connection.cursor() as cursor:
        configure_database(application, cursor)
        configure_group(application, cursor, name, configuration)

    with transaction:
        application.environment.zookeeper.create(
            application.get_group_path(name),
            BinaryCodec(GroupConfiguration).encode(configuration),
        )


class VersionedGroup(collections.namedtuple('VersionedGroup', 'name version')):
    @classmethod
    def expand(cls, value):
        bits = value.split('@', 1)
        if len(bits) == 1:
            return cls(bits[0], None)
        elif len(bits) == 2:
            return cls(bits[0], bits[1])
        else:
            raise AssertionError('Invalid group identifier: %r' % (value,))


def fetch_groups(application, names):
    groups = map(VersionedGroup.expand, names)
    paths = map(
        application.get_group_path,
        map(operator.attrgetter('name'), groups),
    )
    futures = map(application.environment.zookeeper.get_async, paths)

    results = []
    decode = BinaryCodec(GroupConfiguration).decode
    for group, future in zip(groups, futures):
        data, stat = future.get()
        configuration = decode(data)
        assert group.version is None or group.version == get_version(configuration), \
            'versions do not match (%s and %s)' % (group.version, get_version(configuration))
        results.append((group.name, (configuration, stat)))

    return results


def update_group(application, name, updated_configuration):
    (name, (current_configuration, stat)) = fetch_groups(application, (name,))[0]

    transactions = []

    current_tables = set(collect_tables(current_configuration.table))
    updated_tables = set(collect_tables(updated_configuration.table))

    source_connection = get_connection_for_database(current_configuration.database)
    transactions.append(Transaction(source_connection, 'update-group:%s' % (name,)))
    if current_configuration.database != updated_configuration.database:
        # If the database configuration has changed, we need to ensure that the
        # new database has the basic installation, and that all triggers are
        # dropped from the source database for this group.
        destination_connection = get_connection_for_database(updated_configuration.database)
        transactions.append(Transaction(destination_connection, 'update-group:%s' % (name,)))
        with destination_connection.cursor() as cursor:
            configure_database(application, cursor)

        with source_connection.cursor() as cursor:
            unconfigure_group(application, cursor, name, current_configuration)
    else:
        # If the database configuration has *not* changed, we need to ensure
        # that any tables that are no longer monitored have their triggers
        # removed before continuing.
        destination_connection = source_connection

        with source_connection.cursor() as cursor:
            dropped_tables = current_tables - updated_tables
            for table in dropped_tables:
                drop_trigger(application, cursor, name, table)

    with destination_connection.cursor() as cursor:
        configure_group(application, cursor, name, updated_configuration)

    with managed(transactions):
        application.environment.zookeeper.set(
            application.get_group_path(name),
            BinaryCodec(GroupConfiguration).encode(updated_configuration),
            version=stat.version,
        )


def collect_groups_by_database(groups):
    # TODO: Add a docstring here, this is kind of weird.
    # TODO: Eventually make use DatabaseConfiguration as keys, but they aren't hashable.
    sources = collections.defaultdict(dict)
    for name, (configuration, stat) in groups.items():
        sources[configuration.database.connection.dsn][name] = (configuration, stat)
    return sources



@contextmanager
def noop():
    yield


def move_groups(application, names, database, force=False):
    """
    Moves a collection of groups (provided by name) to the database
    configuration provided.
    """
    assert names, 'at least one group must be provided'

    original = dict(fetch_groups(application, names))

    transactions = []

    # TODO: Rather than make this totally skip removal, it probably makes sense
    # to allow a "best effort" cleanup that doesn't fail the task if the
    # database can't be accessed.
    if not force:
        # Remove all of the triggers from the previous database.
        # TODO: Figure out how this behavior works with consumers that are already
        # connected -- this should fail, as it is today.
        for dsn, groups in collect_groups_by_database(original).items():
            source_connection = psycopg2.connect(dsn)
            transactions.append(Transaction(source_connection, 'move-groups:source'))
            with source_connection.cursor() as cursor:
                for name, (configuration, stat) in groups.items():
                    unconfigure_group(application, cursor, name, configuration)

    updated = {}
    for name, (configuration, stat) in original.items():
        c = GroupConfiguration()
        c.CopyFrom(configuration)
        c.database.CopyFrom(database)
        updated[name] = (c, stat)

    # Add the triggers to the destination.
    destination_connection = get_connection_for_database(database)
    transactions.append(Transaction(destination_connection, 'move-groups:destination'))
    with destination_connection.cursor() as cursor:
        configure_database(application, cursor)
        for name, (configuration, stat) in updated.items():
            configure_group(application, cursor, name, configuration)

    with managed(transactions):
        ztransaction = application.environment.zookeeper.transaction()
        for name, (configuration, stat) in updated.items():
            ztransaction.set_data(
                application.get_group_path(name),
                BinaryCodec(GroupConfiguration).encode(configuration),
                version=stat.version,
            )
        commit(ztransaction)


def drop_groups(application, names, force=False):
    assert names, 'at least one group must be provided'

    results = dict(fetch_groups(application, names))

    if not force:
        transactions = []
        for dsn, groups in collect_groups_by_database(results).items():
            connection = psycopg2.connect(dsn)
            transactions.append(Transaction(connection, 'drop-groups'))
            with connection.cursor() as cursor:
                for name, (configuration, stat) in groups.items():
                    unconfigure_group(application, cursor, name, configuration)
        manager = managed(transactions)
    else:
        manager = noop()

    with manager:
        ztransaction = application.environment.zookeeper.transaction()
        for name, (configuration, stat) in results.iteritems():
            ztransaction.delete(
                application.get_group_path(name),
                version=stat.version,
            )
        commit(ztransaction)
