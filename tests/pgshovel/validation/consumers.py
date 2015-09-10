import uuid

import pytest

from pgshovel.interfaces.common_pb2 import Timestamp
from pgshovel.interfaces.streams_pb2 import (
    Header,
    Message,
)
from pgshovel.interfaces.replication_pb2 import ConsumerState
from pgshovel.replication.validation.consumers import (
    InvalidPublisher,
    InvalidSequenceStartError,
    SequencingError,
    validate_consumer_state,
)


timestamp = Timestamp(seconds=0, nanos=0)


def validate(stream, start=0, state=None):
    for offset, message in enumerate(stream, start):
        state = validate_consumer_state(state, offset, message)
        yield state


def build_header(sequence, publisher=uuid.uuid1().bytes, timestamp=timestamp):
    return Header(
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

    list(validate(messages))


def test_incorrect_sequence_start():
    messages = [
        Message(header=build_header(1)),
    ]

    state = ConsumerState()
    state.header.publisher = 'abcd'
    stream = validate(messages, start=1, state=state)
    with pytest.raises(InvalidSequenceStartError):
        next(stream)


def test_invalid_multiplexed_sequence():
    messages = [
        Message(header=build_header(0, publisher='a')),
        Message(header=build_header(1, publisher='a')),
        Message(header=build_header(0, publisher='b')),
        Message(header=build_header(2, publisher='a')),
    ]

    stream = validate(messages)
    assert next(stream)
    assert next(stream)
    assert next(stream)
    with pytest.raises(InvalidPublisher):
        next(stream)


def test_missing_message():
    messages = [
        Message(header=build_header(0)),
        Message(header=build_header(2)),
    ]

    stream = validate(messages)

    assert next(stream)
    with pytest.raises(SequencingError):
        next(stream)


def test_out_of_order_message():
    messages = [
        Message(header=build_header(0)),
        Message(header=build_header(1)),
        Message(header=build_header(2)),
        Message(header=build_header(1)),
    ]

    stream = validate(messages)

    assert next(stream)
    assert next(stream)
    assert next(stream)
    with pytest.raises(SequencingError):
        next(stream)


def test_duplicate_message():
    messages = [
        Message(header=build_header(0)),
        Message(header=build_header(1)),
        Message(header=build_header(1)),
        Message(header=build_header(2)),
    ]

    stream = validate(messages)
    assert next(stream)
    assert next(stream)
    with pytest.raises(SequencingError):
        next(stream)


def test_repeated_sequence():
    messages = [
        Message(header=build_header(0, timestamp=Timestamp(seconds=0, nanos=0))),
        Message(header=build_header(0, timestamp=Timestamp(seconds=1, nanos=0))),
    ]

    stream = validate(messages)
    assert next(stream)

    with pytest.raises(SequencingError):
        next(stream)
