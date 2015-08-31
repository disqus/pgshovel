from pgshovel.interfaces.replication_pb2 import (
    State,
    StreamState,
)
from pgshovel.replication.validation.bootstrap import validate_bootstrap_state
from pgshovel.replication.validation.consumers import validate_consumer_state
from pgshovel.replication.validation.transactions import validate_transaction_state


class MultipleStateValidator(object):
    def __init__(self, message, validators):
        self.message = message
        self.validators = validators

    def __call__(self, state, *args, **kwargs):
        states = {}
        for name, validator in self.validators.items():
            if state is not None and state.HasField(name):
                value = getattr(state, name)
            else:
                value = None
            result = validator(value, *args, **kwargs)
            if result is not None:
                states[name] = result
        return self.message(**states)


validate_state = MultipleStateValidator(State, {
    'bootstrap_state': validate_bootstrap_state,
    'stream_state': MultipleStateValidator(StreamState, {
        'consumer_state': validate_consumer_state,
        'transaction_state': validate_transaction_state,
    })
})


#: The expected types of event for a stream of transactions when there is no
#: existing ``TransactionState``.
TRANSACTION_START_EVENT_TYPES = validate_state.validators['stream_state'].validators['transaction_state'].receivers[None].keys()  # noqa
