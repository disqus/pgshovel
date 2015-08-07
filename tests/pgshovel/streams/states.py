import itertools
import pytest
from collections import namedtuple
import uuid
from pgshovel.streams.states import (
    Committed,
    InTransaction,
    InvalidBatch,
    InvalidEventError,
    InvalidPublisher,
    RolledBack,
    StatefulStreamValidator,
    get_operation,
    require_batch_id_advanced_if_same_node,
    require_batch_id_not_advanced_if_same_node,
    require_different_publisher,
    require_same_batch,
    require_same_publisher,
    validate,
)
from tests.pgshovel.streams.fixtures import (
    batch,
    begin,
    commit,
    copy,
    make_messages,
    message,
    mutation,
)


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

    with pytest.raises(InvalidEventError):
        next(validator(('kick',)))


def test_stateful_validator_unhandled_starting_state():
    events = range(5)
    validator = StatefulStreamValidator({})
    validated = validator(events)

    with pytest.raises(InvalidEventError):
        next(validated)


def test_successful_transaction():
    messages = list(make_messages([
        {'begin': begin},
        {'mutation': mutation},
        {'commit': commit},
    ]))

    validated = validate(messages)

    assert next(validated) == (InTransaction(messages[0].header.publisher, batch), messages[0])
    assert next(validated) == (InTransaction(messages[1].header.publisher, batch), messages[1])
    assert next(validated) == (Committed(messages[2].header.publisher, batch), messages[2])
