import logging
import operator
import sys

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from pgshovel.commands import (
    FormatOption,
    command,
    formatters,
)
from pgshovel.databases import Database
from pgshovel.utilities.postgresql import Transaction
from pgshovel.utilities.templates import template
from pgshovel.utilities.zookeeper import commit


logger = logging.getLogger(__name__)


@command(description="Initializes a new cluster in ZooKeeper.")
def initialize_cluster(options, application):
    logger.info('Creating new cluster for %s...', application)
    ztransaction = application.environment.zookeeper.transaction()
    ztransaction.create(application.path)
    ztransaction.create(application.databases.path)
    ztransaction.create(application.groups.path)
    commit(ztransaction)


@command(description="Adds a new database to the cluster.")
def add_database(options, application, name):
    ztransaction = application.environment.zookeeper.transaction()

    database = Database(name, application.codec.load(sys.stdin))
    application.databases.create(database, ztransaction)

    logger.info('Adding %s to %s...', database, application)

    connection = database.connect()
    with connection.cursor() as cursor:
        ptransaction = Transaction(connection, 'add-database')

        # Create the extension.
        logger.info('Installing PGQ extension...')
        cursor.execute('CREATE EXTENSION IF NOT EXISTS pgq;')

        # Create the language.
        logger.info('Installing pypythonu langauge...')
        cursor.execute('CREATE OR REPLACE LANGUAGE plpythonu')

        # Create the schema.
        logger.info('Installing schema...')
        cursor.execute(template(application, 'sql/install-schema.sql')())

        # Install the queue.
        logger.info('Creating transaction queue...')
        cursor.execute(template(application, 'sql/create-transaction-queue.sql')())

        # Create the trigger functions.
        # TODO: Maybe provide versions here?
        logger.info('Installing capture function...')
        cursor.execute(template(application, 'sql/install-capture-function.sql')())

        with ptransaction:
            commit(ztransaction)


class ConsumerRecordList(list):
    def __str__(self):
        return ', '.join(sorted(map(operator.attrgetter('consumer_name'), self)))


@command(
    description="Lists the registered databases.",
    options=(
        FormatOption,
    )
)
def list_databases(options, application):
    rows = []

    for name, database in list(application.databases.all()):
        row = [name, database.configuration['connection']['database']]

        try:
            connection = database.connect(isolation_level=ISOLATION_LEVEL_AUTOCOMMIT)
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM pgq.get_queue_info(%s);", (application.queue,))
                result = cursor.fetchone()
                row += [result.ev_per_sec, result.ticker_lag]
                # TODO: Make the consumer report more useful for JSON format.
                cursor.execute("SELECT * FROM pgq.get_consumer_info(%s);", (application.queue,))
                row += [ConsumerRecordList(cursor.fetchall())]
        except psycopg2.Error as e:
            row += ['error: %s' % (e,)]

        rows.append(row)

    rows.sort()

    headers = ['name', 'database', 'events/sec', 'ticker lag', 'consumers']
    print formatters[options.format](rows, headers=headers)
