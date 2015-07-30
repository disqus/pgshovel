import uuid

import pytest

from pgshovel.interfaces.stream_pb2 import (
    Message,
    Timestamp,
)
from pgshovel.stream import (
    RepeatedSequenceError,
    SequencingError,
    validate,
)


def build_header(sequence, publisher=uuid.uuid1().bytes, timestamp=Timestamp(seconds=0, nanos=0)):
    return Message.Header(
        publisher=publisher,
        sequence=sequence,
        timestamp=timestamp,
    )


def test_simple_sequence():
    messages = [
        Message(header=build_header(1)),
        Message(header=build_header(2)),
        Message(header=build_header(3)),
    ]

    stream = validate(messages)
    assert list(stream) == messages


def test_multiplexed_sequence():
    messages = [
        Message(header=build_header(100, publisher='a')),
        Message(header=build_header(200, publisher='b')),
        Message(header=build_header(101, publisher='a')),
        Message(header=build_header(201, publisher='b')),
        Message(header=build_header(300, publisher='c')),
        Message(header=build_header(102, publisher='a')),
    ]

    stream = validate(messages)
    assert list(stream) == messages


def test_missing_message():
    messages = [
        Message(header=build_header(1)),
        Message(header=build_header(4)),
    ]

    stream = validate(messages)

    assert next(stream) is messages[0]
    with pytest.raises(SequencingError):
        next(stream)


def test_out_of_order_message():
    messages = [
        Message(header=build_header(2)),
        Message(header=build_header(1)),
    ]

    stream = validate(messages)

    assert next(stream) is messages[0]
    with pytest.raises(SequencingError):
        next(stream)


def test_duplicate_message():
    messages = [
        Message(header=build_header(1)),
        Message(header=build_header(2)),
        Message(header=build_header(2)),
        Message(header=build_header(3)),
    ]

    stream = validate(messages)
    assert list(stream) == [messages[0], messages[1], messages[3]]


def test_repeated_sequence():
    messages = [
        Message(header=build_header(1, timestamp=Timestamp(seconds=0, nanos=0))),
        Message(header=build_header(1, timestamp=Timestamp(seconds=1, nanos=0))),
    ]

    stream = validate(messages)
    assert next(stream) is messages[0]

    with pytest.raises(RepeatedSequenceError):
        next(stream)
