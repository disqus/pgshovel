import functools
import logging
import textwrap

from pgshovel.commands import (
    FormatOption,
    Option,
    command,
    formatters,
)
from pgshovel.groups import Group
from pgshovel.utilities.contextmanagers import managed
from pgshovel.utilities.postgresql import Transaction
from pgshovel.utilities.templates import template
from pgshovel.utilities.zookeeper import commit


logger = logging.getLogger(__name__)


def _install_trigger(application, cursor, group, table, alias):
    logger.debug('Adding trigger to %s for %s...', table, group)
    cursor.execute(template(application, 'sql/install-trigger.sql')({
        'group': group.name,
        'table': table,
        'alias': alias,
    }))


def _uninstall_trigger(application, cursor, group, table, alias):
    logger.debug('Removing trigger for %s from %s...', group, table)
    cursor.execute(template(application, 'sql/uninstall-trigger.sql')({
        'group': group.name,
        'table': table,
        'alias': alias,
    }))


def explode_table_aliases(values, tables=None, separator=':'):
    tables = tables.copy() if tables is not None else {}
    aliases = set(tables.values())

    for value in values:
        bits = value.split(separator)
        if len(bits) == 1:
            table = alias = bits[0]
        elif len(bits) == 2:
            table, alias = bits
        else:
            raise ValueError('Invalid table specification: %s' % (value,))

        if alias in aliases:
            raise ValueError('%r has already been used as a table alias.' % (alias,))

        if table in tables:
            raise ValueError('%r has already been registered.' % (table,))

        tables[table] = alias
        aliases.add(alias)

    return tables


@command(description="Creates a new capture group.")
def create_group(options, application, group, database, *tables):
    tables = explode_table_aliases(tables)

    zookeeper = application.environment.zookeeper
    ztransaction = zookeeper.transaction()

    logger.info('Creating %s...', group)

    group = Group(group, {
        'database': database,
        'tables': tables,
    })

    application.groups.create(group, ztransaction)

    database = application.databases.get(database, ztransaction)
    connection = database.connect()
    with connection.cursor() as cursor:
        ptransaction = Transaction(connection, 'create-group:%s' % (group.name,))

        logger.info('Adding triggers for %s...', group)
        for table, alias in tables.items():
            _install_trigger(application, cursor, group, table, alias)

        with ptransaction:
            commit(ztransaction)


def maybe_implode((table, alias), separator=':'):
    return '%s:%s' % (table, alias) if table != alias else table


class TableMapping(dict):
    def __str__(self):
        return ', '.join(map(maybe_implode, sorted(self.items())))


@command(
    description="Lists the registered capture groups.",
    options=(
        FormatOption,
    )
)
def list_groups(options, application):
    rows = []

    for name, group in list(application.groups.all()):
        row = [name, group.configuration['database'], TableMapping(group.configuration['tables'])]
        rows.append(row)

    rows.sort()

    headers = ['name', 'database', 'tables']
    print formatters[options.format](rows, headers=headers)


@command(
    description="Moves a capture group from it's current database to the destination.",
    options=(
        Option('--failover', action='store_true', help=textwrap.dedent("""\
            Do not attempt to drop triggers from the origin database. (USE WITH
            CAUTION: This can result in data corruption if the origin database
            is recovered with a transaction queue that still contains items!)
        """)),
    ),
)
def move_group(options, application, group, destination):
    zookeeper = application.environment.zookeeper
    ztransaction = zookeeper.transaction()

    group = application.groups.get(group)  # does not do transaction check due to version passed during update

    destination_db = application.databases.get(destination, ztransaction)
    destination_connection = destination_db.connect()
    with destination_connection.cursor() as destination_cursor:
        logger.info('Moving %s to %s...', group, destination_db)
        transactions = []

        destination_transaction = Transaction(destination_connection, 'move-group:%s:destination' % (group.name,))
        transactions.append(destination_transaction)
        for table, alias in group.configuration['tables'].items():
            _install_trigger(application, destination_cursor, group, table, alias)

        if not options.failover:
            origin_db = application.databases.get(group.configuration['database'], ztransaction)
            origin_connection = origin_db.connect()
            with origin_connection.cursor() as origin_cursor:
                origin_transaction = Transaction(origin_connection, 'move-group:%s:origin' % (group.name,))
                transactions.append(origin_transaction)
                logger.info('Removing triggers from %s...', origin_db)
                for table, alias in group.configuration['tables'].items():
                    _uninstall_trigger(application, origin_cursor, group, table, alias)
        else:
            logger.warning('Skipped removing triggers from origin.')

        group.configuration['database'] = destination
        application.groups.update(group, ztransaction)

        managed(transactions, functools.partial(commit, ztransaction))


@command(description="Drop the capture group.")
def drop_group(options, application, group):
    zookeeper = application.environment.zookeeper
    ztransaction = zookeeper.transaction()

    group = application.groups.get(group)  # does not do transaction check due to version passed during delete
    logger.info('Removing %s...', group)

    database = application.databases.get(group.configuration['database'], ztransaction)
    connection = database.connect()
    with connection.cursor() as cursor:
        ptransaction = Transaction(connection, 'drop-group:%s' % (group.name,))
        logger.info('Removing triggers for %s...', group)
        for table, alias in group.configuration['tables'].items():
            _uninstall_trigger(application, cursor, group, table, alias)

        application.groups.delete(group, ztransaction)

        with ptransaction:
            commit(ztransaction)
