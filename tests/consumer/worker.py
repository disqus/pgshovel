import logging
import threading
from contextlib import contextmanager
from Queue import Empty

import psycopg2
import pytest
from kazoo.recipe.lock import Lock

from pgshovel.administration import (
    create_group,
    initialize_cluster,
)
from pgshovel.consumer.worker import (
    Consumer,
    Coordinator,
)
from pgshovel.interfaces.groups_pb2 import (
    GroupConfiguration,
    TableConfiguration,
)
from pgshovel.utilities.postgresql import ManagedConnection
from pgshovel.utilities.protobuf import TextCodec
from tests.integration import (
    TemporaryApplication,
    TemporaryDatabase,
    resource,
)


logger = logging.getLogger(__name__)

consumer_identifier = 'consumer'
consumer_group_identifier = 'consumer-group'


class Explosion(Exception):
    pass


@pytest.yield_fixture
def application():
    with TemporaryApplication() as application:
        application.environment.zookeeper.start()
        try:
            yield application
        finally:
            application.environment.zookeeper.stop()


def database_generator():
    """Creates a database, preloaded with our testing schema."""
    with TemporaryDatabase() as database:
        logger.debug('Configuring database %s...', database.connection.dsn)

        connection = psycopg2.connect(database.connection.dsn)

        # Create the schema.
        with resource('sql', 'forums.sql') as f, connection.cursor() as cursor:
            schema = f.read()
            cursor.execute(schema)
            connection.commit()

        connection.close()

        yield database


database = pytest.yield_fixture(database_generator)
temporary_database = contextmanager(database_generator)


def setup_application(application, database):
    """Performs basic application setup (creates cluster, installs groups, etc.)"""
    initialize_cluster(application)

    groups = {}

    for group in ('users', 'forums'):
        with resource('tables', group) as f:
            configuration = groups[group] = GroupConfiguration(
                database=database,
                table=TextCodec(TableConfiguration).decode(f.read()),
            )

        create_group(application, group, configuration)

    connection = psycopg2.connect(database.connection.dsn)

    # Never try to be clever with windowing pgq ticks, always allow them to happen when requested.
    with connection.cursor() as cursor:
        cursor.execute("update pgq.queue set queue_ticker_max_lag = '0', queue_ticker_idle_period = '0'")
        connection.commit()

    connection.close()

    return groups


def force_tick(connection, application, group):
    with connection.cursor() as cursor:
        # Manually force a tick to happen, so that the events are added to a batch for processing.
        cursor.execute('SELECT * FROM pgq.ticker(%s)', (application.get_queue_name(group),))
        connection.commit()


def test_consumer_lifecycle(application, database):
    groups = setup_application(application, database)

    group = 'users'

    # Build the consumer.
    consumer = Consumer(
        application,
        ManagedConnection(database.connection.dsn),
        consumer_identifier,
        consumer_group_identifier,
        group,
        groups[group],
    )

    # Manually take out the ownership lock -- this will prevent the consumer
    # from fully initializing (which is what we want, for testing purposes.)
    lock = Lock(
        application.environment.zookeeper,
        application.get_ownership_lock_path(consumer_group_identifier)(group),
    )

    assert lock.acquire(blocking=False), 'could not acquire ownership lock for testing!'

    # Start the consumer -- this is where the meat of the test begins.
    consumer.start()

    connection = psycopg2.connect(database.connection.dsn)

    # Check to make sure that a pgq consumer registration hasn't been created yet.
    with connection.cursor() as cursor:
        statement = "SELECT count(*) FROM pgq.get_consumer_info(%s, %s)"
        cursor.execute(statement, (application.get_queue_name(group), consumer_group_identifier))
        (result,) = cursor.fetchone()
        assert result == 0, 'registration should not already exist'

    # Register the cursor anyway, so we can make sure consuming without the
    # ownership lock held doesn't do anything.
    with connection.cursor() as cursor:
        statement = "SELECT * FROM pgq.register_consumer(%s, %s)"
        cursor.execute(statement, ((application.get_queue_name(group), consumer_group_identifier)))
        (created,) = cursor.fetchone()
        assert created, '(manual) registration should succeed'

    # Do some shit to the database so that there will be rows to consume.
    # TODO: Build a generator to spam for us instead.
    n = 25
    with connection.cursor() as cursor:
        statement = 'INSERT INTO %s (username) VALUES (%%s)' % (groups[group].table.name,)
        for i in xrange(n):
            username = 'user_%s' % (i,)
            cursor.execute(statement, (username,))
        connection.commit()

        force_tick(connection, application, group)

    assert not consumer.ready.is_set(), 'consumer should not be ready'

    # Check to make sure that calling `consume` doesn't return any results.
    with pytest.raises(Empty):
        assert not consumer.batches.get(False)

    # Delete the ownership lock -- now the consumer should start running in
    # earnest.
    lock.release()

    # Wait for the consumer to be ready.
    assert consumer.ready.wait(3), 'consumer not ready before timeout'

    # Ensure that the ownership lock is acquired.
    assert consumer._ownership_lock.is_acquired, 'ownership lock should be acquired'

    # Check to make sure events can be consumed.
    with connection.cursor() as cursor:
        events, finish = consumer.batches.get(timeout=1)
        assert len(events) == n, 'consumer should recieve all records'
        finish(connection)

        with pytest.raises(Empty):  # There should be no more events.
            consumer.batches.get(timeout=1)

    # Stop the consumer.
    consumer.stop()

    # Check to make sure that the lock is released.
    consumer.result()  # wait for the consumer to stop, and report any errors
    assert not consumer._ownership_lock.is_acquired, 'consumer should release lock after stopping'

    connection.close()


def test_coordinator_lifecycle(application, database):
    groups = setup_application(application, database)

    group = 'users'
    configuration = groups[group]

    coordinator = Coordinator(
        application,
        ManagedConnection(database.connection.dsn),
        consumer_group_identifier,
        consumer_identifier,
    )

    # Start the coordinator.
    coordinator.start()

    # Subscribe to a group.
    consumer = coordinator.subscribe(group, configuration).result(1)
    assert consumer.ready.wait(3), 'consumer not ready before timeout'

    # Check consumer ownership.
    assert consumer._ownership_lock.is_acquired, 'ownership lock should be acquired'

    # Update the group configuration.
    updated_configuration = configuration.table.columns.append('lol')
    coordinator.subscribe(group, updated_configuration).result(1)

    assert consumer.configuration == updated_configuration, 'configuration should be updated'

    # Unsubscribe from the group.
    consumer = coordinator.unsubscribe(group).result(1)
    consumer.join(3)
    assert not consumer.running(), 'consumer should stop after unsubscribe'
    assert not consumer._ownership_lock.is_acquired, 'ownership lock should be released after unsubscribe'

    consumer = coordinator.subscribe(group, configuration).result(1)
    assert consumer.ready.wait(3), 'consumer not ready before timeout'

    # Stop the consumer.
    coordinator.stop()
    coordinator.result()  # ensure it exited gracefully (did not raise)

    assert not consumer.running(), 'consumer should be stopped'

def test_coordinator_consumer_failure_handling(application, database):
    groups = setup_application(application, database)

    die = threading.Event()

    class ExplodingConsumer(Consumer):
        def run(self):
            self.ready.set()
            die.wait()
            raise Explosion("I'm dead")

    class ExplodingCoordinator(Coordinator):
        Consumer = ExplodingConsumer

    coordinator = ExplodingCoordinator(
        application,
        ManagedConnection(database.connection.dsn),
        consumer_group_identifier,
        consumer_identifier,
    )

    coordinator.start()

    # Subscribe to a group.
    group = 'users'
    configuration = groups[group]
    consumer = coordinator.subscribe(group, configuration).result()

    die.set()

    # Ensure the coordinator dies.
    with pytest.raises(Explosion):
        consumer.result(3)

    with pytest.raises(Explosion):
        coordinator.result(3)


@pytest.mark.xfail
def test_zookeeper_disconnection(application):
    # Kill ZooKeeper session.
    # ???
    raise NotImplementedError
