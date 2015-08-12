"""
Tools for validating input streams.
"""
import functools
from collections import namedtuple

from pgshovel.interfaces.streams_pb2 import (
    BatchOperation,
    BeginOperation,
    CommitOperation,
    MutationOperation,
    RollbackOperation,
)


def get_operation(message):
    if message is None:
        return None

    return getattr(message, message.WhichOneof('operation'))


class InvalidEventError(Exception):
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
                raise InvalidEventError('Cannot receive events in state: {0!r}'.format(state))

            key = self.key_function(event)
            try:
                receiver = receivers[key]
            except KeyError:
                raise InvalidEventError('Cannot receive {0!r} while in state: {1!r}'.format(event, state))

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
    if state.batch_identifier != get_operation(event).batch_identifier:
        raise InvalidBatch('Event batch ID must be the same as the current state.')


def require_batch_id_advanced_if_same_node(state, event):
    # TODO: This should probably validate the tick contents as well.
    operation = get_operation(event)
    if state.batch_identifier.node == operation.batch_identifier.node and \
            state.batch_identifier.id >= operation.batch_identifier.id:
        raise InvalidBatch('Event batch ID must be advanced from the current state.')


def require_batch_id_not_advanced_if_same_node(state, event):
    operation = get_operation(event)
    if state.batch_identifier.node == operation.batch_identifier.node and \
            state.batch_identifier.id != operation.batch_identifier.id:
        raise InvalidBatch('Event batch ID must not be advanced from the current state.')


def require_same_publisher(state, event):
    if state.publisher != event.header.publisher:
        raise InvalidPublisher('Event publisher ID must be the same as the the current state.')


def require_different_publisher(state, event):
    if state.publisher == event.header.publisher:
        raise InvalidPublisher('Event publisher ID cannot be the same as the current state.')


# States

InTransaction = namedtuple('InTransaction', 'publisher batch_identifier')
Committed = namedtuple('Committed', 'publisher batch_identifier')
RolledBack = namedtuple('RolledBack', 'publisher batch_identifier')


def get_batch_operation_type(event):
    operation = get_operation(event)
    assert isinstance(operation, BatchOperation)
    return type(get_operation(operation))


validate = StatefulStreamValidator({
    None: {
        BeginOperation: lambda state, event: InTransaction(event.header.publisher, event.batch_operation.batch_identifier),
    },
    InTransaction: {
        MutationOperation: validate_event(
            (require_same_publisher, require_same_batch),
            lambda state, event: InTransaction(event.header.publisher, event.batch_operation.batch_identifier),
        ),
        CommitOperation: validate_event(
            (require_same_publisher, require_same_batch),
            lambda state, event: Committed(event.header.publisher, event.batch_operation.batch_identifier),
        ),
        RollbackOperation: validate_event(
            (require_same_publisher, require_same_batch),
            lambda state, event: RolledBack(event.header.publisher, event.batch_operation.batch_identifier),
        ),
        BeginOperation: validate_event(
            (require_different_publisher, require_batch_id_not_advanced_if_same_node),
            lambda state, event: InTransaction(event.header.publisher, event.batch_operation.batch_identifier),
        ),
    },
    Committed: {
        BeginOperation: validate_event(
            (require_batch_id_advanced_if_same_node,),
            lambda state, event: InTransaction(event.header.publisher, event.batch_operation.batch_identifier),
        ),
    },
    RolledBack: {
        BeginOperation: validate_event(
            (require_batch_id_not_advanced_if_same_node,),
            lambda state, event: InTransaction(event.header.publisher, event.batch_operation.batch_identifier),
        ),
    }
}, key_function=get_batch_operation_type)
