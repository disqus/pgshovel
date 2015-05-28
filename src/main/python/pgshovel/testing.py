import itertools
import random
import time
import uuid

from pgshovel.events import (
    MutationBatch,
    MutationEvent,
)


def advance(generator, steps):
    """
    Advances a generator the provided number of steps, discarding
    intermediate values.
    """
    for _ in xrange(steps):
        next(generator)


operations = {
    'INSERT': lambda generator: (None, generator()),
    'UPDATE': lambda generator: (generator(), generator()),
    'DELETE': lambda generator: (generator(), None),
}


class EventBuilder(object):
    def __init__(self, table, state_generator, primary_key_columns=['id'], schema='public'):
        self.schema = schema
        self.table = table
        self.state_generator = state_generator
        self.primary_key_columns = primary_key_columns

    def __call__(self, id, transaction_id):
        operation, state_constructor = random.choice(operations.items())

        return MutationEvent(
            id,
            self.schema,
            self.table,
            operation,
            self.primary_key_columns,
            state_constructor(self.state_generator),
            transaction_id,
            time.time(),
        )


class BatchBuilder(object):
    DEFAULT_NODE_ID = uuid.UUID('00000000-0000-1000-8080-808080808080')

    def __init__(self, builders):
        self.builders = builders

        self.__batch_id_sequence = itertools.count(1)
        self.__event_id_sequence = itertools.count(1)
        self.__tx_sequence = itertools.count(1)

    def __call__(self, size, node=DEFAULT_NODE_ID):
        events = []
        for event_id in itertools.islice(self.__event_id_sequence, size):
            advance(self.__tx_sequence, random.randint(0, 2))
            transaction_id = next(self.__tx_sequence)
            event_builder = random.choice(self.builders)
            events.append(event_builder(event_id, transaction_id))

        batch = MutationBatch(
            next(self.__batch_id_sequence),
            events[0].transaction_id,
            events[-1].transaction_id,
            node,
            events,
        )

        # force non-overlapping transaction ids between batches
        advance(self.__tx_sequence, 1)

        return batch
