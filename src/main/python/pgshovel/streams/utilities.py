import logging
import uuid
from functools import partial
from itertools import (
    count,
    dropwhile,
)

from pgshovel.replication.validation import TRANSACTION_START_EVENT_TYPES
from pgshovel.utilities.protobuf import get_oneof_value


logger = logging.getLogger(__name__)


class FormattedBatchIdentifier(object):
    def __init__(self, batch_identifier):
        self.batch_identifier = batch_identifier

    def __str__(self):
        return '{node.hex}/{id}'.format(
            node=uuid.UUID(bytes=self.batch_identifier.node),
            id=self.batch_identifier.id,
        )


class FormattedSnapshot(object):
    def __init__(self, snapshot, max=3):
        self.snapshot = snapshot
        self.max = max

    def __str__(self):
        active = self.snapshot.active
        return '{snapshot.min}:{snapshot.max}:[{active}{truncated}]'.format(
            snapshot=self.snapshot,
            active=','.join(map(str, active[:self.max])),
            truncated='' if len(active) <= self.max else ',+{0}...'.format(len(active) - self.max),
        )


class UnableToPrimeError(Exception):
    """
    Raised when an attempt to prime is made, but the priming failed after the
    max number of messages.
    """
    pass


def is_start_batch_operation(message):
    """
    Is the ``message`` the start operation of a batch of mutations.
    """
    value = get_oneof_value(message.batch_operation, 'operation')
    return type(value) in TRANSACTION_START_EVENT_TYPES


def prime_until(check, max_messages, stream):
    """
    Attempts to "prime" the ``stream`` by dropping any messages for which
    ``check`` evaluates to ``False``, up until ``max_messages``.
    """

    attempts = count(start=1)

    def still_checking((offset, message)):
        attempt = next(attempts)

        if check(message):
            return False  # Stop checking
        elif attempt > max_messages:
            raise UnableToPrimeError  # We've dropped too many messages
        elif attempt % 25 == 0:
            logging.info('Still priming %r.' % stream)
        else:
            return True  # Still checking

    return dropwhile(still_checking, stream)


prime_for_batch_start = partial(prime_until, is_start_batch_operation)
