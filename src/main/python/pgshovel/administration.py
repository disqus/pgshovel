import code
import collections
import logging
import sys
from contextlib import contextmanager
from string import Template

from pgshovel.commands import (
    FormatOption,
    Option,
    command,
    formatters,
)
from pgshovel.groups import Group
from pgshovel.interfaces.groups_pb2 import (
    DatabaseConfiguration,
    GroupConfiguration,
)
from pgshovel.utilities.postgresql import (
    Transaction,
    managed,
)
from pgshovel.utilities.protobuf import TextCodec
from pgshovel.utilities.templates import resource_string
from pgshovel.utilities.zookeeper import commit


logger = logging.getLogger(__name__)


def __get_connection_for_dsn(dsn):
    import psycopg2
    return psycopg2.connect(dsn)


def __get_database_connection(group):
    return __get_connection_for_dsn(group.configuration.database.connection.dsn)


def __configure_database(application, cursor):
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


def __trigger_name(application, group):
    return '_pgshovel_%s_%s_capture' % (application.name, group.name)


def __queue_name(application, group):
    return '%s.%s' % (application.schema, group.name)


def __configure_group(application, cursor, group):
    # Create the transaction queue if it doesn't already exist.
    logger.info('Creating transaction queue...')
    cursor.execute("SELECT pgq.create_queue(%s)", (__queue_name(application, group),))

    tables = collections.defaultdict(set)

    def __collect_columns(table):
        columns = tables[table.name]
        columns.add(table.primary_key)
        columns.update(table.columns)

        for join in table.joins:
            tables[join.table.name].add(join.foreign_key)
            __collect_columns(join.table)

    __collect_columns(group.configuration.table)

    # Install the trigger(s) on the destination table (always replace.)
    name = __trigger_name(application, group)
    for table, columns in tables.items():
        statement = """
            CREATE TRIGGER %(name)s
            AFTER INSERT OR UPDATE OF %(columns)s OR DELETE
            ON %(table)s
            FOR EACH ROW EXECUTE PROCEDURE %(schema)s.capture(%%s, %%s)
        """ % {
            'name': name,
            'columns': ', '.join(columns),
            'table': table,
            'schema': application.schema,
        }

        logger.info('Installing capture trigger on %s...', table)
        cursor.execute("DROP TRIGGER IF EXISTS %s ON %s" % (name, table))
        cursor.execute(statement, (__queue_name(application, group), group.version))


def __drop_trigger(application, cursor, group, table):
    logger.info('Dropping capture trigger on %s...', table)
    cursor.execute('DROP TRIGGER %s ON %s' % (__trigger_name(application, group), table))


def __unconfigure_group(application, cursor, group):
    # Drop the transaction queue if it exists.
    logger.info('Dropping transaction queue...')
    cursor.execute("SELECT pgq.drop_queue(%s)", (__queue_name(application, group),))

    # Drop the triggers on the destination table.
    def __uninstall_triggers(table):
        __drop_trigger(application, cursor, group, table.name)
        for join in table.joins:
            __uninstall_triggers(join.table)

    __uninstall_triggers(group.configuration.table)


@command
def shell(options, application):
    exports = {
        'application': application,
        'environment': application.environment,
    }
    return code.interact(local=exports)


@command(description="Initializes a new cluster in ZooKeeper.", start=False)
def initialize_cluster(options, application):
    logger.info('Creating a new cluster for %s...', application)
    ztransaction = application.environment.zookeeper.transaction()
    ztransaction.create(application.path)
    ztransaction.create(application.groups.path)
    commit(ztransaction)


@command(
    description="Lists the registered capture groups.",
    options=(FormatOption,),
)
def list_groups(options, application):
    # TODO: support validation
    rows = []
    for group in list(application.groups.all()):
        rows.append((
            group.name,
            group.configuration.database.name,
            group.configuration.table.name,
            group.version,
        ))

    print formatters[options.format](rows, headers=('name', 'database', 'table', 'version'))


def __get_codec(options, cls):
    # TODO: allow switching between text and binary codecs as an option
    return TextCodec(cls)


@command(description="Creates a new capture group.")
def create_group(options, application, name):
    configuration = __get_codec(options, GroupConfiguration).decode(sys.stdin.read())

    group = Group(name, configuration)

    connection = __get_database_connection(group)
    transaction = Transaction(connection, 'create-group:%s' % (group.name,))
    with connection.cursor() as cursor:
        __configure_database(application, cursor)
        __configure_group(application, cursor, group)

    with transaction:
        application.groups.set(group)


@command(description="Provides details about a capture group.")
def inspect_group(options, application, name):
    group = application.groups.get(name)
    sys.stdout.write(__get_codec(options, GroupConfiguration).encode(group.configuration))
    sys.stderr.write(group.version)


def collect_tables(table):
    tables = set()

    def _collect(table):
        tables.add(table.name)
        for join in table.joins:
            _collect(join.table)

    _collect(table)

    return tables


@command(description="Updates a capture group.")
def update_group(options, application, name):
    # TODO: Implement version checking.
    configuration = __get_codec(options, GroupConfiguration).decode(sys.stdin.read())

    group = application.groups.get(name)

    transactions = []

    current_tables = collect_tables(group.configuration.table)
    updated_tables = collect_tables(configuration.table)

    source = __get_database_connection(group)
    transactions.append(Transaction(source, 'update-group:%s' % (group.name,)))

    if group.configuration.database != configuration.database:
        # If the database configuration has changed, we need to ensure that the
        # new database has the basic installation, and that all triggers were
        # dropped from the source database -- basically, that the group has
        # been entirely removed from the destination database.
        destination = __get_connection_for_dsn(configuration.database.connection.dsn)
        transactions.append(Transaction(destination, 'update-group:%s' % (group.name,)))
        with destination.cursor() as cursor:
            __configure_database(application, cursor)

        with source.cursor() as cursor:
            __unconfigure_group(application, cursor, group)
    else:
        # If the database configuration has *not* changed, we need to ensure
        # that any tables that are no longer monitored have their triggers
        # removed before continuining.
        destination = source

        with source.cursor() as cursor:
            dropped_tables = current_tables - updated_tables
            for table in dropped_tables:
                __drop_trigger(application, cursor, group, table)

    group.configuration = configuration

    with destination.cursor() as cursor:
        __configure_group(application, cursor, group)

    with managed(transactions):
        application.groups.set(group)


def __collect_by_dsn(groups):
    sources = collections.defaultdict(set)
    for group in groups:
        sources[group.configuration.database.connection.dsn].add(group)
    return sources


@contextmanager
def noop():
    yield


@command(
    options=(
        Option('--force', action='store_true', help='Skip database transactions on source databases.'),
    ),
)
def move_groups(options, application, *names):
    # TODO: Implement version checking.
    assert names, 'at least one group must be provided'

    database = __get_codec(options, DatabaseConfiguration).decode(sys.stdin.read())

    groups = map(application.groups.get, names)

    transactions = []

    if not options.force:
        # Remove all of the triggers from the previous database.
        # TODO: Figure out how this behavior works with consumers that are already
        # connected -- this should fail, as it is today.
        for dsn, groups_for_source in __collect_by_dsn(groups).items():
            connection = __get_connection_for_dsn(dsn)
            transactions.append(Transaction(connection, 'move-groups:source'))
            with connection.cursor() as cursor:
                for group in groups_for_source:
                    __unconfigure_group(application, cursor, group)

    # Overwrite the group's database with the updated values.
    for group in groups:
        group.configuration.database.CopyFrom(database)

    # Add the triggers to the destination.
    connection = __get_connection_for_dsn(database.connection.dsn)
    transactions.append(Transaction(connection, 'move-groups:destination'))
    with connection.cursor() as cursor:
        __configure_database(application, cursor)
        for group in groups:
            __configure_group(application, cursor, group)

    with managed(transactions):
        ztransaction = application.environment.zookeeper.transaction()
        for group in groups:
            application.groups.set(group, ztransaction)
        commit(ztransaction)


@command(
    description="Drops capture group(s).",
    options=(
        Option('--force', action='store_true', help='Skip database transactions.'),
    ),
)
def drop_groups(options, application, *names):
    # TODO: Implement version checking.
    assert names, 'at least one group must be provided'

    groups = map(application.groups.get, names)

    if not options.force:
        transactions = []
        for dsn, groups_for_source in __collect_by_dsn(groups).items():
            connection = __get_connection_for_dsn(dsn)
            transactions.append(Transaction(connection, 'drop-groups'))
            with connection.cursor() as cursor:
                for group in groups_for_source:
                    __unconfigure_group(application, cursor, group)
        manager = managed(transactions)
    else:
        manager = noop()

    with manager:
        ztransaction = application.environment.zookeeper.transaction()
        for group in groups:
            application.groups.delete(group, ztransaction)
        commit(ztransaction)
