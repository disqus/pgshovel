"""
Tools for validating input streams.
"""
import functools

from pgshovel.interfaces.replication_pb2 import (
    Committed,
    InTransaction,
    RolledBack,
    TransactionState,
)
from pgshovel.interfaces.streams_pb2 import (
    BeginOperation,
    CommitOperation,
    MutationOperation,
    RollbackOperation,
)
from pgshovel.utilities.protobuf import get_oneof_value


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

    def __init__(self, message, receivers):
        self.message = message

        #: A Map[StateType][EventKey] = (current state, event) -> State
        #: The return value of the receiver will become the new state of the
        #: stream validator, and will also be yielded for each item along with
        #: the original input.
        self.receivers = receivers

    def __call__(self, state, offset, message):
        if state is not None:
            state = get_oneof_value(state, 'state')

        operation = get_oneof_value(get_oneof_value(message, 'operation'), 'operation')

        try:
            receivers = self.receivers[type(state) if state is not None else None]
        except KeyError:
            raise InvalidEventError('Cannot receive events in state: {0!r}'.format(state))

        try:
            receiver = receivers[type(operation)]
        except KeyError:
            raise InvalidEventError('Cannot receive {0!r} while in state: {1!r}'.format(operation, state))

        return self.message(**receiver(state, offset, message))


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
    def __validate__(*args, **kwargs):
        for validator in validators:
            validator(*args, **kwargs)
        return receiver(*args, **kwargs)
    return __validate__


def require_same_batch(state, offset, message):
    # TODO: This should probably validate the tick contents as well.
    if state.batch_identifier != message.batch_operation.batch_identifier:
        raise InvalidBatch('Event batch ID must be the same as the current state.')


def require_batch_id_advanced_if_same_node(state, offset, message):
    # TODO: This should probably validate the tick contents as well.
    operation = message.batch_operation
    if state.batch_identifier.node == operation.batch_identifier.node and \
            state.batch_identifier.id >= operation.batch_identifier.id:
        raise InvalidBatch('Event batch ID must be advanced from the current state.')


def require_batch_id_not_advanced_if_same_node(state, offset, message):
    operation = message.batch_operation
    if state.batch_identifier.node == operation.batch_identifier.node and \
            state.batch_identifier.id != operation.batch_identifier.id:
        raise InvalidBatch('Event batch ID must not be advanced from the current state.')


def require_same_publisher(state, offset, message):
    if state.publisher != message.header.publisher:
        raise InvalidPublisher('Event publisher ID must be the same as the the current state.')


def require_different_publisher(state, offset, message):
    if state.publisher == message.header.publisher:
        raise InvalidPublisher('Event publisher ID cannot be the same as the current state.')


def build_transition_function(label, cls):
    def transition(state, offset, message):
        return {
            label: cls(
                publisher=message.header.publisher,
                batch_identifier=message.batch_operation.batch_identifier,
            ),
        }
    return transition


transition_to_in_transaction = build_transition_function('in_transaction', InTransaction)
transition_to_committed = build_transition_function('committed', Committed)
transition_to_rolled_back = build_transition_function('rolled_back', RolledBack)

validate_transaction_state = StatefulStreamValidator(TransactionState, {
    None: {
        BeginOperation: transition_to_in_transaction,
    },
    InTransaction: {
        MutationOperation: validate_event(
            (require_same_publisher, require_same_batch),
            transition_to_in_transaction,
        ),
        CommitOperation: validate_event(
            (require_same_publisher, require_same_batch),
            transition_to_committed,
        ),
        RollbackOperation: validate_event(
            (require_same_publisher, require_same_batch),
            transition_to_rolled_back,
        ),
        BeginOperation: validate_event(
            (require_different_publisher, require_batch_id_not_advanced_if_same_node),
            transition_to_in_transaction,
        ),
    },
    Committed: {
        BeginOperation: validate_event(
            (require_batch_id_advanced_if_same_node,),
            transition_to_in_transaction,
        ),
    },
    RolledBack: {
        BeginOperation: validate_event(
            (require_batch_id_not_advanced_if_same_node,),
            transition_to_in_transaction,
        ),
    }
})
