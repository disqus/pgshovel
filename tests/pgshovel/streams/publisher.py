import pytest

from pgshovel.streams import (
    sequences,
    states,
)
from pgshovel.streams.batches import get_operation
from pgshovel.streams.publisher import Publisher
from tests.pgshovel.streams.fixtures import (
    batch_identifier,
    begin,
    commit,
    mutation,
    reserialize,
    rollback,
)


def test_publisher():
    messages = []
    publisher = Publisher(messages.append)

    with publisher.batch(batch_identifier, begin) as publish:
        publish(mutation)

    published_messages = map(reserialize, messages)

    assert get_operation(get_operation(published_messages[0])) == begin
    assert get_operation(get_operation(published_messages[1])) == mutation
    assert get_operation(get_operation(published_messages[2])) == commit

    for i, message in enumerate(published_messages):
        assert message.header.publisher == publisher.id
        assert message.header.sequence == i

    # Ensure it actually generates valid data.
    assert list(states.validate(published_messages))
    assert list(sequences.validate(published_messages))


def test_publisher_failure():
    messages = []
    publisher = Publisher(messages.append)

    with pytest.raises(NotImplementedError):
        with publisher.batch(batch_identifier, begin):
            raise NotImplementedError

    published_messages = map(reserialize, messages)

    assert get_operation(get_operation(published_messages[0])) == begin
    assert get_operation(get_operation(published_messages[1])) == rollback

    # Ensure it actually generates valid data.
    assert list(states.validate(published_messages))
    assert list(sequences.validate(published_messages))

    for i, message in enumerate(published_messages):
        assert message.header.publisher == publisher.id
        assert message.header.sequence == i

    # Write another message to ensure that the publisher can continue to be used.
    assert len(messages) == 2
    publisher.publish()
    assert len(messages) == 3
    assert messages[2].header.sequence == 2
