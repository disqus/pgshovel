import itertools
import uuid
from collections import namedtuple

import pytest

from pgshovel.interfaces.stream_pb2 import (
    Batch,
    Begin,
    Column,
    Commit,
    Message,
    Mutation,
    Rollback,
    Row,
    Snapshot,
    Tick,
    Timestamp,
)
from pgshovel.stream import (
    Committed,
    InTransaction,
    InvalidBatch,
    InvalidOperation,
    InvalidPublisher,
    InvalidSequenceStartError,
    Publisher,
    RepeatedSequenceError,
    RolledBack,
    RowConverter,
    SequencingError,
    StateTransitionError,
    StatefulStreamValidator,
    TransactionAborted,
    TransactionCancelled,
    batched,
    get_operation,
    require_batch_id_advanced_if_same_node,
    require_batch_id_not_advanced_if_same_node,
    require_different_publisher,
    require_same_batch,
    require_same_publisher,
    to_snapshot,
    to_timestamp,
    validate_events,
    validate_sequences,
)


def build_header(sequence, publisher=uuid.uuid1().bytes, timestamp=Timestamp(seconds=0, nanos=0)):
    return Message.Header(
        publisher=publisher,
        sequence=sequence,
        timestamp=timestamp,
    )


def test_simple_sequence():
    messages = [
        Message(header=build_header(0)),
        Message(header=build_header(1)),
        Message(header=build_header(2)),
    ]

    stream = validate_sequences(messages)
    assert list(stream) == messages


def test_incorrect_sequence_start():
    messages = [
        Message(header=build_header(1)),
    ]

    stream = validate_sequences(messages)
    with pytest.raises(InvalidSequenceStartError):
        next(stream)


def test_invalid_multiplexed_sequence():
    messages = [
        Message(header=build_header(0, publisher='a')),
        Message(header=build_header(1, publisher='a')),
        Message(header=build_header(0, publisher='b')),
        Message(header=build_header(2, publisher='a')),
    ]

    stream = validate_sequences(messages)
    assert next(stream) is messages[0]
    assert next(stream) is messages[1]
    assert next(stream) is messages[2]
    with pytest.raises(InvalidPublisher):
        next(stream)


def test_missing_message():
    messages = [
        Message(header=build_header(0)),
        Message(header=build_header(2)),
    ]

    stream = validate_sequences(messages)

    assert next(stream) is messages[0]
    with pytest.raises(SequencingError):
        next(stream)


def test_out_of_order_message():
    messages = [
        Message(header=build_header(0)),
        Message(header=build_header(1)),
        Message(header=build_header(2)),
        Message(header=build_header(1)),
    ]

    stream = validate_sequences(messages)

    assert next(stream) is messages[0]
    assert next(stream) is messages[1]
    assert next(stream) is messages[2]
    with pytest.raises(SequencingError):
        next(stream)


def test_duplicate_message():
    messages = [
        Message(header=build_header(0)),
        Message(header=build_header(1)),
        Message(header=build_header(1)),
        Message(header=build_header(2)),
    ]

    stream = validate_sequences(messages)
    assert list(stream) == [messages[0], messages[1], messages[3]]


def test_repeated_sequence():
    messages = [
        Message(header=build_header(0, timestamp=Timestamp(seconds=0, nanos=0))),
        Message(header=build_header(0, timestamp=Timestamp(seconds=1, nanos=0))),
    ]

    stream = validate_sequences(messages)
    assert next(stream) is messages[0]

    with pytest.raises(RepeatedSequenceError):
        next(stream)


def test_stateful_validator():
    Locked = namedtuple('Locked', '')
    Unlocked = namedtuple('Unlocked', '')

    validator = StatefulStreamValidator({
        Unlocked: {
            'coin': lambda state, event: Unlocked(),
            'push': lambda state, event: Locked(),
        },
        Locked: {
            'coin': lambda state, event: Unlocked(),
            'push': lambda state, event: Locked(),
        },
    }, start=Locked())

    assertions = (
        ('push', Locked()),
        ('coin', Unlocked()),
        ('coin', Unlocked()),
        ('push', Locked()),
        ('push', Locked()),
    )

    inputs = (i[0] for i in assertions)
    validated = validator(i[0] for i in assertions)
    expected = (i[1] for i in assertions)
    for input, (state, event), expected in itertools.izip(inputs, validated, expected):
        assert input == event
        assert state == expected

    with pytest.raises(StateTransitionError):
        next(validator(('kick',)))


def test_stateful_validator_unhandled_starting_state():
    events = range(5)
    validator = StatefulStreamValidator({})
    validated = validator(events)

    with pytest.raises(StateTransitionError):
        next(validated)


DEFAULT_PUBLISHER = uuid.uuid1().bytes


def make_message(payload, sequence=1, publisher=DEFAULT_PUBLISHER):
    message = Message(
        header=Message.Header(
            publisher=publisher,
            sequence=sequence,
            timestamp=Timestamp(seconds=0, nanos=0),
        ),
        **payload
    )

    return Message.FromString(message.SerializeToString())


def make_messages(payloads, publisher=DEFAULT_PUBLISHER):
    sequence = itertools.count()

    for payload in payloads:
        yield make_message(payload, next(sequence), publisher)


batch = Batch(id=1, node=uuid.uuid1().bytes)

begin = Begin(
    batch=batch,
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

mutation = Mutation(
    id=1,
    batch=batch,
    schema='public',
    table='users',
    operation=Mutation.INSERT,
    identity_columns=['id'],
    new=Row(
        columns=[
            Column(name='id', integer64=1),
            Column(name='username', string='ted'),
        ],
    ),
    timestamp=Timestamp(seconds=0, nanos=0),
    transaction=1,
)

commit = Commit(batch=batch)

rollback = Rollback(batch=batch)


def test_successful_transaction():
    messages = list(make_messages([
        {'begin': begin},
        {'mutation': mutation},
        {'commit': commit},
    ]))

    validated = validate_events(messages)

    assert next(validated) == (InTransaction(messages[0].header.publisher, batch), messages[0])
    assert next(validated) == (InTransaction(messages[1].header.publisher, batch), messages[1])
    assert next(validated) == (Committed(messages[2].header.publisher, batch), messages[2])


def copy(message, **replacements):
    updated = type(message).FromString(message.SerializeToString())
    for key, value in replacements.items():
        setattr(updated, key, value)
    return updated


@pytest.yield_fixture
def message():
    yield make_message({'commit': commit})


def test_require_same_batch(message):
    batch = get_operation(message).batch

    require_same_batch(
        InTransaction(
            message.header.publisher,
            batch,
        ),
        message,
    )

    with pytest.raises(InvalidBatch):
        require_same_batch(
            InTransaction(
                message.header.publisher,
                copy(batch, id=batch.id + 1),
            ),
            message,
        )


def test_require_batch_advanced_if_same_node(message):
    batch = get_operation(message).batch

    require_batch_id_advanced_if_same_node(
        Committed(
            message.header.publisher,
            copy(batch, node=uuid.uuid1().bytes),
        ),
        message,
    )

    require_batch_id_advanced_if_same_node(
        Committed(
            message.header.publisher,
            copy(batch, id=batch.id - 1),
        ),
        message,
    )

    with pytest.raises(InvalidBatch):
        require_batch_id_advanced_if_same_node(
            Committed(
                message.header.publisher,
                batch,
            ),
            message,
        )


def test_require_batch_id_not_advanced_if_same_node(message):
    batch = get_operation(message).batch

    require_batch_id_not_advanced_if_same_node(
        RolledBack(
            message.header.publisher,
            batch,
        ),
        message,
    )

    require_batch_id_not_advanced_if_same_node(
        RolledBack(
            message.header.publisher,
            copy(batch, node=uuid.uuid1().bytes),
        ),
        message,
    )

    with pytest.raises(InvalidBatch):
        require_batch_id_not_advanced_if_same_node(
            RolledBack(
                message.header.publisher,
                copy(batch, id=batch.id + 1),
            ),
            message,
        )


def test_require_same_publisher(message):
    batch = get_operation(message).batch

    require_same_publisher(
        Committed(
            message.header.publisher,
            batch,
        ),
        message,
    )

    with pytest.raises(InvalidPublisher):
        require_same_publisher(
            Committed(
                uuid.uuid1().bytes,
                batch,
            ),
            message,
        )


def test_require_different_publisher(message):
    batch = get_operation(message).batch

    require_different_publisher(
        Committed(
            uuid.uuid1().bytes,  # change the publisher
            batch,
        ),
        message,
    )

    with pytest.raises(InvalidPublisher):
        require_different_publisher(
            Committed(
                message.header.publisher,
                batch,
            ),
            message,
        )


def test_batch_iterator():
    messages = make_messages([
        {'begin': begin},
        {'mutation': mutation},
        {'mutation': mutation},
        {'mutation': mutation},
        {'commit': commit},
    ])
    batches = batched(validate_events(messages))

    batch, mutations = next(batches)
    assert batch == begin.batch
    assert list(mutations) == [mutation] * 3


def test_batch_iterator_early_exit():
    messages = make_messages([
        {'begin': begin},
        {'mutation': mutation},
    ])
    batches = batched(validate_events(messages))

    batch, mutations = next(batches)
    assert batch == begin.batch
    assert next(mutations) == mutation
    with pytest.raises(TransactionAborted):
        next(mutations)


def test_batch_iterator_rolled_back():
    messages = make_messages([
        {'begin': begin},
        {'mutation': mutation},
        {'rollback': rollback},
    ])
    batches = batched(validate_events(messages))

    batch, mutations = next(batches)
    assert batch == begin.batch
    assert next(mutations) == mutation
    with pytest.raises(TransactionCancelled):
        next(mutations)


def test_batch_restarted():
    incomplete = make_messages([
        {'begin': begin},
        {'mutation': mutation},
    ], publisher=uuid.uuid1().bytes)
    complete = make_messages([
        {'begin': begin},
        {'mutation': mutation},
        {'commit': commit},
    ], publisher=uuid.uuid1().bytes)
    messages = itertools.chain(incomplete, complete)

    batches = batched(validate_events(messages))

    # The first batch should be aborted, since it didn't end with a
    # commit/rollback before switching publishers.
    batch, mutations = next(batches)
    assert batch == begin.batch
    assert next(mutations) == mutation
    with pytest.raises(TransactionAborted):
        next(mutations)

    batch, mutations = next(batches)
    assert batch == begin.batch
    assert next(mutations) == mutation
    with pytest.raises(StopIteration):
        next(mutations)


def test_batch_no_mutations():
    messages = make_messages([
        {'begin': begin},
        {'commit': commit},
    ])
    batches = batched(validate_events(messages))

    batch, mutations = next(batches)
    assert batch == begin.batch
    with pytest.raises(StopIteration):
        next(mutations)


def reserialize(message):
    # This is a hack to get around errors with oneof field initialization in a
    # message constructor:: https://github.com/google/protobuf/issues/147
    return type(message).FromString(message.SerializeToString())


def test_row_conversion():
    converter = RowConverter(sorted=True)  # maintain sort order for equality checks

    row = reserialize(
        Row(
            columns=[
                Column(name='active', boolean=True),
                Column(name='biography'),
                Column(name='id', integer64=9223372036854775807),
                Column(name='reputation', float=1.0),
                Column(name='username', string='bob'),
            ],
        ),
    )

    decoded = converter.to_python(row)
    assert decoded == {
        'id': 9223372036854775807,
        'username': 'bob',
        'active': True,
        'reputation': 1.0,
        'biography': None,
    }

    assert converter.to_protobuf(decoded) == row


def test_snapshot_conversion():
    assert to_snapshot('1:10:') == Snapshot(
        min=1,
        max=10,
    )


def test_snapshot_conversion_in_progress():
    assert to_snapshot('1:10:2,3,4') == Snapshot(
        min=1,
        max=10,
        active=[2, 3, 4],
    )


def test_timetamp_conversion():
    assert to_timestamp(1438814328.940597) == Timestamp(
        seconds=1438814328,
        nanos=940597057,  # this is different due to floating point arithmetic
    )


def test_publisher():
    messages = []
    publisher = Publisher(messages.append)

    with publisher.batch(begin.batch, start=begin.start, end=begin.end) as publish:
        publish(
            id=mutation.id,
            schema=mutation.schema,
            table=mutation.table,
            operation=mutation.operation,
            identity_columns=mutation.identity_columns,
            new=mutation.new,
            timestamp=mutation.timestamp,
            transaction=mutation.transaction,
        )

    published_messages = map(reserialize, messages)

    assert get_operation(published_messages[0]) == begin
    assert get_operation(published_messages[1]) == mutation
    assert get_operation(published_messages[2]) == commit

    for i, message in enumerate(published_messages):
        assert message.header.publisher == publisher.id
        assert message.header.sequence == i

    # Ensure it actually generates valid data.
    assert list(validate_sequences(published_messages))
    assert list(validate_events(published_messages))


def test_publisher_failure():
    messages = []
    publisher = Publisher(messages.append)

    with pytest.raises(NotImplementedError):
        with publisher.batch(begin.batch, start=begin.start, end=begin.end):
            raise NotImplementedError

    published_messages = map(reserialize, messages)

    assert get_operation(published_messages[0]) == begin
    assert get_operation(published_messages[1]) == rollback

    # Ensure it actually generates valid data.
    assert list(validate_sequences(published_messages))
    assert list(validate_events(published_messages))

    for i, message in enumerate(published_messages):
        assert message.header.publisher == publisher.id
        assert message.header.sequence == i

    # Write another message to ensure that the publisher can continue to be used.
    assert len(messages) == 2
    publisher.publish()
    assert len(messages) == 3
    assert messages[2].header.sequence == 2
