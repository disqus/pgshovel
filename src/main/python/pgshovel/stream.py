import functools
import logging
import uuid
from collections import namedtuple

from pgshovel.interfaces.stream_pb2 import (
    Begin,
    Commit,
    Mutation,
    Rollback,
)


logger = logging.getLogger(__name__)


class SequencingError(Exception):
    """
    Error raised when messages are read out of sequence (either out of order,
    or with missing messages in between.)
    """
    template = 'Invalid sequence: {0} to {1}'

    def __init__(self, previous, current):
        self.previous = previous
        self.current = current

        message = self.template.format(
            previous.header.sequence,
            current.header.sequence,
        )
        super(SequencingError, self).__init__(message)


class InvalidPublisher(Exception):
    pass


class RepeatedSequenceError(SequencingError):
    template = 'Repeated sequence: {0} and {1}'


class InvalidSequenceStartError(Exception):
    pass


def validate_sequences(messages):
    """
    Validates a stream of Message instances, ensuring that the correct
    sequencing order is maintained, all messages are present, and only a single
    publisher is communicating on the stream.

    Duplicate messages are dropped if they have already been yielded.
    """
    previous = None

    # All of the publishers that have been previously seen during the execution
    # of this validator. (Does not include the currently active publisher.)
    dead = set()

    for message in messages:
        if message.header.publisher in dead:
            raise InvalidPublisher('Received message from previously used publisher.')

        if previous is not None:
            if previous.header.publisher == message.header.publisher:
                # If the message we just received is exactly the same as the
                # previous message, we can safely ignore it. (This could happen
                # if the publisher is retrying a message that was not fully
                # acknowledged before being partitioned from the recipient, but
                # was actually written.)
                if previous.header.sequence == message.header.sequence:
                    if previous == message:
                        logger.debug('Skipping duplicate message.')
                        continue
                    else:
                        raise RepeatedSequenceError(previous, message)
                elif previous.header.sequence + 1 != message.header.sequence:
                    raise SequencingError(previous, message)
            else:
                logger.info(
                    'Publisher of %r has changed from %r to %r.',
                    messages,
                    previous.header.publisher,
                    message.header.publisher,
                )
                dead.add(previous.header.publisher)
                previous = None

        if previous is None and message.header.sequence != 0:
            raise InvalidSequenceStartError('Invalid sequence start point: {0}'.format(message.header.sequence))

        yield message

        previous = message


def get_operation(message):
    if message is None:
        return None

    return getattr(message, message.WhichOneof('operation'))


class StateTransitionError(Exception):
    pass


class StatefulStreamValidator(object):
    """
    Responds to a sequence of events, ensuring that the provided input has a
    valid receiver for the current state of the stream. If the event cannot
    be accepted, a ``StateTransitionError`` is raised.
    """
    # This implementation is heavily inspired by Erlang's ``gen_fsm``:
    # http://www.erlang.org/doc/design_principles/fsm.html

    def __init__(self, receivers, start=None, key_function=lambda event: event):
        #: A Map[StateType][EventKey] = (current state, event) -> State
        #: The return value of the receiver will become the new state of the
        #: stream validator, and will also be yielded for each item along with
        #: the original input.
        self.receivers = receivers

        #: The starting state.
        self.start = start

        #: A function that when applied to incoming events returns a key used
        #: look up the event receiver for the current sate.
        self.key_function = key_function

    def __call__(self, events):
        """
        Accepts a stream of events, yielding a two-tuple of ``(new state,
        event)`` for each input.
        """
        state = self.start

        for event in events:
            try:
                receivers = self.receivers[type(state) if state is not None else None]
            except KeyError:
                raise StateTransitionError('Cannot receive events in state: {0!r}'.format(state))

            key = self.key_function(event)
            try:
                receiver = receivers[key]
            except KeyError:
                raise StateTransitionError('Cannot receive {0!r} while in state: {1!r}'.format(event, state))

            state = receiver(state, event)
            yield state, event


class InvalidOperation(Exception):
    pass


class InvalidBatch(InvalidOperation):
    pass


class InvalidPublisher(InvalidOperation):
    pass


def validate_event(validators, receiver):
    """
    Wraps an event receiver, applying each test function before calling the
    receiver function.
    """
    @functools.wraps(receiver)
    def __validate__(state, event):
        for validator in validators:
            validator(state, event)
        return receiver(state, event)
    return __validate__


def require_same_batch(state, event):
    # TODO: This should probably validate the tick contents as well.
    if state.batch != get_operation(event).batch:
        raise InvalidBatch('Event batch ID must be the same as the current state.')


def require_batch_id_advanced_if_same_node(state, event):
    # TODO: This should probably validate the tick contents as well.
    operation = get_operation(event)
    if state.batch.node == operation.batch.node and state.batch.id >= operation.batch.id:
        raise InvalidBatch('Event batch ID must be advanced from the current state.')


def require_batch_id_not_advanced_if_same_node(state, event):
    operation = get_operation(event)
    if state.batch.node == operation.batch.node and state.batch.id != operation.batch.id:
        raise InvalidBatch('Event batch ID must not be advanced from the current state.')


def require_same_publisher(state, event):
    if state.publisher != event.header.publisher:
        raise InvalidPublisher('Event publisher ID must be the same as the the current state.')


def require_different_publisher(state, event):
    if state.publisher == event.header.publisher:
        raise InvalidPublisher('Event publisher ID cannot be the same as the current state.')


InTransaction = namedtuple('InTransaction', 'publisher batch')
Committed = namedtuple('Committed', 'publisher batch')
RolledBack = namedtuple('RolledBack', 'publisher batch')


validate_events = StatefulStreamValidator({
    None: {
        Begin: lambda state, event: InTransaction(event.header.publisher, event.begin.batch),
    },
    InTransaction: {
        Mutation: validate_event(
            (require_same_publisher, require_same_batch),
            lambda state, event: InTransaction(event.header.publisher, event.mutation.batch),
        ),
        Commit: validate_event(
            (require_same_publisher, require_same_batch),
            lambda state, event: Committed(event.header.publisher, event.commit.batch),
        ),
        Rollback: validate_event(
            (require_same_publisher, require_same_batch),
            lambda state, event: RolledBack(event.header.publisher, event.rollback.batch),
        ),
        Begin: validate_event(
            (require_different_publisher, require_batch_id_not_advanced_if_same_node),
            lambda state, event: InTransaction(event.header.publisher, event.begin.batch),
        ),
    },
    Committed: {
        Begin: validate_event(
            (require_same_publisher, require_batch_id_advanced_if_same_node),
            lambda state, event: InTransaction(event.header.publisher, event.begin.batch),
        ),
    },
    RolledBack: {
        Begin: validate_event(
            (require_same_publisher, require_batch_id_not_advanced_if_same_node),
            lambda state, event: InTransaction(event.header.publisher, event.begin.batch),
        ),
    }
}, key_function=lambda event: type(get_operation(event)))
