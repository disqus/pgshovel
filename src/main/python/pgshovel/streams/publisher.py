"""
Tools for publishing batches.
"""
import itertools
import logging
import time
import uuid
from contextlib import contextmanager

from pgshovel.streams.interfaces_pb2 import (
    Begin,
    Commit,
    Message,
    Mutation,
    Rollback,
)
from pgshovel.streams.utilities import to_timestamp


logger = logging.getLogger(__name__)


class Publisher(object):
    """
    Handles publishing messages to a receiver, ensuring that the messages are
    sequenced correctly, and transactional semantics are preserved during batch
    publishing.

    This class is *not* designed to be thread safe.
    """
    def __init__(self, receiver):
        #: A function or callable for writing to an output stream. This is
        #: assumed to be synchronous, and that the receiver function will block
        #: until the messages have been acknowledged by the destination. If the
        #: message cannot be accepted by the receiver for any reason, the
        #: receiver should raise an exception.
        self.receiver = receiver

        self.id = uuid.uuid1().bytes
        self.sequence = itertools.count(0)

    def publish(self, **kwargs):
        self.receiver(
            Message(
                header=Message.Header(
                    publisher=self.id,
                    sequence=next(self.sequence),
                    timestamp=to_timestamp(time.time()),
                ),
                **kwargs
            ),
        )

    @contextmanager
    def batch(self, batch, **kwargs):
        """
        Wraps a batch, ensuring the Begin and appropriate Commit/Rollback
        messages are sent. The context manager provides a function that can be
        used to publish mutation events that are part of the batch.

        Extra keyword arguments provided to the constructor are forwarded to
        the ``Begin`` constructor.
        """
        logger.debug('Starting transaction...')
        self.publish(begin=Begin(batch=batch, **kwargs))

        def mutation(**kwargs):
            return self.publish(mutation=Mutation(batch=batch, **kwargs))

        try:
            yield mutation
        except Exception:
            logger.debug('Attempting to publish rollback of in progress transaction...')
            self.publish(rollback=Rollback(batch=batch))  # TODO: Handle *this* failing.
            logger.debug('Published rollback.')
            raise
        else:
            logger.debug('Attempting to publish commit of in progress transaction...')
            self.publish(commit=Commit(batch=batch))
            logger.debug('Published commit.')
