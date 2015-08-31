from __future__ import absolute_import

import pytest
from itertools import islice

from kafka import (
    KafkaClient,
    SimpleProducer,
)
from tests.pgshovel.fixtures import (
    cluster,
    create_temporary_database,
)
from tests.pgshovel.streams.fixtures import (
    DEFAULT_PUBLISHER,
    begin,
    transaction,
    transactions,
)

from pgshovel.interfaces.common_pb2 import Snapshot
from pgshovel.interfaces.configurations_pb2 import ReplicationSetConfiguration
from pgshovel.interfaces.replication_pb2 import (
    ConsumerState,
    State,
    BootstrapState,
    TransactionState,
)
from pgshovel.interfaces.streams_pb2 import (
    Header,
    Message,
)
from pgshovel.replication.streams.kafka import KafkaStream
from pgshovel.replication.validation.consumers import SequencingError
from pgshovel.replication.validation.transactions import InvalidEventError
from pgshovel.relay.streams.kafka import KafkaWriter
from pgshovel.streams.utilities import UnableToPrimeError


@pytest.yield_fixture
def configuration():
    yield {'hosts': 'kafka:9092'}


@pytest.yield_fixture
def stream(configuration, cluster, client):
    stream = KafkaStream.configure(configuration, cluster, 'default')
    client.ensure_topic_exists(stream.topic)
    yield stream


@pytest.yield_fixture
def client(configuration):
    yield KafkaClient(configuration['hosts'])


@pytest.yield_fixture
def writer(client, stream):
    producer = SimpleProducer(client)
    yield KafkaWriter(producer, stream.topic)


@pytest.yield_fixture
def state():
    bootstrap_state = BootstrapState(
        node='1234',
        snapshot=Snapshot(min=1, max=2),
    )
    yield State(bootstrap_state=bootstrap_state)


@pytest.yield_fixture
def sliced_transaction():
    two_transactions = list(islice(transactions(), 6))

    head, remainder = two_transactions[0], two_transactions[1:]
    assert head.batch_operation.begin_operation == begin
    yield remainder



def test_starts_at_beginning_of_stream_for_bootstrapped_state(writer, stream, state):
    writer.push(transaction)
    consumed = list(islice(stream.consume(state), 3))
    assert [message for _, _, message in consumed] == transaction

def test_yields_new_update_state_after_each_message(writer, stream, state):
    expected_states = {
        0: 'in_transaction',
        1: 'in_transaction',
        2: 'committed'
    }

    writer.push(transaction)

    for state, offset, message in islice(stream.consume(state), 3):
        assert state.stream_state.consumer_state.offset == offset
        assert state.stream_state.consumer_state.header == message.header
        assert state.stream_state.transaction_state.WhichOneof('state') == expected_states[offset]

def test_uses_existing_stream_state_if_it_exists(writer, stream, state):
    writer.push(islice(transactions(), 6))

    iterator = stream.consume(state)

    next(iterator)
    next(iterator)
    (new_state, offset, message) = next(iterator)

    new_iterator = stream.consume(new_state)

    (_, new_offset, _) = next(new_iterator)
    assert new_offset == 3


def test_crashes_on_no_state(stream):
    with pytest.raises(AttributeError):
        next(stream.consume(None))


def test_validates_stream_and_crashes_when_invalid(writer, stream, state):
    messages = list(islice(transactions(), 3))
    messages[1] = messages[0]

    writer.push(messages)

    with pytest.raises(SequencingError):
        list(stream.consume(state))


def test_discards_messages_until_start_of_transaction(writer, stream, state, sliced_transaction):
    writer.push(sliced_transaction)

    consumed = list(islice(stream.consume(state), 3))
    assert [message for _, _, message in consumed] == sliced_transaction[-3:]


def test_discarded_messages_is_configurable(configuration, cluster, client, state, writer, sliced_transaction):
    writer.push(sliced_transaction)

    configuration['prime_threshold'] = 1
    antsy_stream = KafkaStream.configure(configuration, cluster, 'default')

    less_antsy_config = configuration.copy()
    less_antsy_config['prime_threshold'] = 3
    less_antsy_stream = KafkaStream.configure(less_antsy_config, cluster, 'default')

    client.ensure_topic_exists(antsy_stream.topic)

    with pytest.raises(UnableToPrimeError):
        list(islice(antsy_stream.consume(state), 3))

    consumed = list(islice(less_antsy_stream.consume(state), 3))
    assert [message for _, _, message in consumed] == sliced_transaction[-3:]
