import uuid

import pytest

from pgshovel.interfaces.common_pb2 import Timestamp
from pgshovel.interfaces.streams_pb2 import (
    Header,
    Message,
)
from pgshovel.streams.sequences import (
    InvalidPublisher,
    InvalidSequenceStartError,
    RepeatedSequenceError,
    SequencingError,
    validate,
)


timestamp = Timestamp(seconds=0, nanos=0)


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

    stream = validate(messages)
    assert list(stream) == messages


def test_incorrect_sequence_start():
    messages = [
        Message(header=build_header(1)),
    ]

    stream = validate(messages)
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

    stream = validate(messages)

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

    stream = validate(messages)

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

    stream = validate(messages)
    assert list(stream) == [messages[0], messages[1], messages[3]]


def test_repeated_sequence():
    messages = [
        Message(header=build_header(0, timestamp=Timestamp(seconds=0, nanos=0))),
        Message(header=build_header(0, timestamp=Timestamp(seconds=1, nanos=0))),
    ]

    stream = validate(messages)
    assert next(stream) is messages[0]

    with pytest.raises(RepeatedSequenceError):
        next(stream)
