import pytest
import psycopg2
import uuid
from copy import deepcopy
from itertools import islice
from kafka import (
    KafkaClient,
    SimpleProducer,
)
from kazoo.client import KazooClient

from kafka import KafkaClient
from tests.pgshovel.fixtures import (
    cluster,
    create_temporary_database
)
from tests.pgshovel.streams.fixtures import (
    batch_identifier,
    make_batch_messages,
    transaction,
)

from pgshovel.administration import (
    create_set,
    get_managed_databases,
    initialize_cluster,
)
from pgshovel.cluster import Cluster
from pgshovel.interfaces.common_pb2 import (
    Column,
    Row,
    Snapshot,
    Tick,
    Timestamp,
)
from pgshovel.interfaces.configurations_pb2 import ReplicationSetConfiguration
from pgshovel.interfaces.streams_pb2 import (
    BeginOperation,
    CommitOperation,
    MutationOperation,
    RollbackOperation,
)
from pgshovel.relay.streams.kafka import KafkaWriter
from pgshovel.replication.loaders.simple import SimpleLoader
from pgshovel.replication.streams.kafka import KafkaStream
from pgshovel.replication.targets.postgresql import PostgreSQLTarget
from pgshovel.utilities.conversions import to_snapshot


class LimitedKafkaStream(KafkaStream):

    def __init__(self, limit, *args, **kwargs):
        self.limit = limit
        super(LimitedKafkaStream, self).__init__(*args, **kwargs)

    def limited_to(self, limit):
        self.limit = limit
        return self

    def consume(self, *args, **kwargs):
        iterator = super(LimitedKafkaStream, self).consume(*args, **kwargs)
        return islice(iterator, self.limit)


def get_mutation(transaction_id=1, user_1_name='kevin bacon'):
    return MutationOperation(
        id=1,
        schema='public',
        table='auth_user',
        operation=MutationOperation.UPDATE,
        identity_columns=['id'],
        new=Row(
            columns=[
                Column(name='id', integer64=1),
                Column(name='username', string=user_1_name),
            ],
        ),
        old=Row(
            columns=[
                Column(name='id', integer64=1),
                Column(name='username', string='example2'),
            ],
        ),
        timestamp=Timestamp(seconds=0, nanos=0),
        transaction=transaction_id,
    )


@pytest.yield_fixture
def source_connection():
    dsn = create_temporary_database('source')
    yield psycopg2.connect(dsn)


@pytest.yield_fixture
def target_connection(target_cluster):
    dsn = create_temporary_database('target')
    _, connection = get_managed_databases(target_cluster, (dsn,)).items()[0]
    yield connection


@pytest.yield_fixture
def target_cluster():
    cluster = Cluster(
        'test_%s' % (uuid.uuid1().hex,),
        KazooClient('zookeeper'),
    )

    with cluster:
        initialize_cluster(cluster)
        yield cluster


@pytest.yield_fixture
def set(cluster, source_connection):
    name = str(uuid.uuid1())
    replication_set = ReplicationSetConfiguration()
    replication_set.database.dsn = source_connection.dsn
    replication_set.tables.add(name='auth_user', primary_keys=['id'])
    create_set(cluster, name, replication_set)
    yield name


@pytest.yield_fixture
def loader(cluster, set):
    yield SimpleLoader(cluster, set)


@pytest.yield_fixture
def stream(cluster, set, client):
    topic = str(uuid.uuid4())
    client.ensure_topic_exists(topic)
    yield LimitedKafkaStream(2, cluster, set, ('kafka:9092',), topic)


@pytest.yield_fixture
def target(target_cluster, set, target_connection):
    yield PostgreSQLTarget(target_cluster, set, target_connection)


@pytest.yield_fixture
def client():
    yield KafkaClient('kafka:9092')


@pytest.yield_fixture
def writer(client, stream):
    producer = SimpleProducer(client)
    yield KafkaWriter(producer, stream.topic)


@pytest.yield_fixture
def source_transaction_snapshot(source_connection):
    with source_connection as conn, conn.cursor() as cursor:
        cursor.execute('SELECT txid_current_snapshot();')
        row = cursor.fetchone()
        yield to_snapshot(row[0])


@pytest.yield_fixture
def invisible_transaction(source_transaction_snapshot):
    # TODO: Figure out if we need to set the min, max here.
    begin = BeginOperation(
        start=Tick(
            id=1,
            snapshot=Snapshot(min=100, max=200),
            timestamp=Timestamp(seconds=0, nanos=0),
        ),
        end=Tick(
            id=2,
            snapshot=Snapshot(min=150, max=250),
            timestamp=Timestamp(seconds=10, nanos=0),
        ),
    )

    mutation = get_mutation(source_transaction_snapshot.max + 1000)

    commit = CommitOperation()

    generator = make_batch_messages(
        batch_identifier,
        (
            {'begin_operation': begin},
            {'mutation_operation': mutation},
            {'commit_operation': commit},
        )
    )

    yield list(generator)


@pytest.yield_fixture
def follow_up_invisible_transaction(invisible_transaction):
    operations = deepcopy(invisible_transaction)
    operations[1].batch_operation.mutation_operation.CopyFrom(
        get_mutation(
            transaction_id=operations[1].batch_operation.mutation_operation.transaction + 1000,
            user_1_name='follow up'
        )
    )
    for operation, i in zip(operations, range(3, 6)):
        operation.header.sequence = i
        operation.batch_operation.batch_identifier.id += 1

    yield operations


@pytest.yield_fixture
def visible_transaction(invisible_transaction):
    operations = deepcopy(invisible_transaction)
    operations[1].batch_operation.mutation_operation.CopyFrom(
        get_mutation(transaction_id=1)
    )
    yield operations


def test_loads_each_table_and_visible_transactions(
    target,
    loader,
    stream,
    source_connection,
    target_connection,
    writer,
    invisible_transaction
):
    with source_connection as conn, conn.cursor() as cursor:
        cursor.executemany(
            'INSERT INTO auth_user (username) VALUES (%s)',
            (('example',), ('example2',))
        )

    # Push a transaction through the relay that is invisible to the snapshot of
    # the source database.
    writer.push(invisible_transaction)

    # Run the target, with a finite stream of just the one invisible
    # transaction (of 3 messages), so at the end of this method call the
    # transaction should have been applied.
    target.run(loader, stream.limited_to(3))

    with target_connection as conn, conn.cursor() as cursor:
        cursor.execute('SELECT * FROM auth_user')
        assert sorted(cursor.fetchmany(3)) == [(1L, 'kevin bacon'), (2L, 'example2')]


def test_any_transactions_visible_to_snapshot_are_ignored(
    target,
    loader,
    stream,
    source_connection,
    target_connection,
    writer,
    visible_transaction
):
    with source_connection as conn, conn.cursor() as cursor:
        cursor.executemany(
            'INSERT INTO auth_user (username) VALUES (%s)',
            (('example',), ('example2',))
        )

    # Push a transaction through the relay that is invisible to the snapshot of
    # the source database.
    writer.push(visible_transaction)

    # Run the target, with a finite stream of just the one invisible
    # transaction (of 3 messages), so at the end of this method call the
    # transaction should have been applied.
    target.run(loader, stream.limited_to(3))

    with target_connection as conn, conn.cursor() as cursor:
        cursor.execute('SELECT * FROM auth_user')
        assert sorted(cursor.fetchmany(3)) == [(1L, 'example'), (2L, 'example2')]


def test_saved_state_is_used_when_run_is_called_multiple_times(
    target,
    loader,
    stream,
    source_connection,
    target_connection,
    writer,
    invisible_transaction,
    follow_up_invisible_transaction
):
    with source_connection as conn, conn.cursor() as cursor:
        cursor.executemany(
            'INSERT INTO auth_user (username) VALUES (%s)',
            (('example',), ('example2',))
        )

    # Push a transaction through the relay that is invisible to the snapshot of
    # the source database.
    writer.push(invisible_transaction)

    # Run the target, with a finite stream of just the one invisible
    # transaction (of 3 messages), so at the end of this method call the
    # transaction should have been applied.
    target.run(loader, stream.limited_to(3))

    with target_connection as conn, conn.cursor() as cursor:
        cursor.execute('SELECT * FROM auth_user')
        assert sorted(cursor.fetchmany(3)) == [(1L, 'kevin bacon'), (2L, 'example2')]

    writer.push(follow_up_invisible_transaction)
    target.run(loader, stream.limited_to(3))

    with target_connection as conn, conn.cursor() as cursor:
        cursor.execute('SELECT * FROM auth_user')
        assert sorted(cursor.fetchmany(3)) == [(1L, 'follow up'), (2L, 'example2')]