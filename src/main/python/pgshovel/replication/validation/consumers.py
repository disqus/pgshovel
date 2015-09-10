"""
Tools for validating input streams.
"""
import logging

from pgshovel.interfaces.replication_pb2 import ConsumerState


logger = logging.getLogger(__name__)


class SequencingError(Exception):
    """
    Error raised when a message contains an invalid sequence value.
    """


class InvalidPublisher(SequencingError):
    """
    Error raised when a message is received from an unexpected publisher.
    """


class InvalidSequenceStartError(Exception):
    """
    Error raised when a message is recieved from a new publisher that starts
    at an incorrect sequence value.
    """


def validate_consumer_state(state, offset, message):
    """
    Validates a stream of Message instances, ensuring that the correct
    sequencing order is maintained, all messages are present, and only a single
    publisher is communicating on the stream.

    Duplicate messages are dropped if they have already been yielded.
    """
    dead = set()

    if state is not None:
        dead.update(state.dead_publishers)

        if message.header.publisher in dead:
            raise InvalidPublisher('Received message from previously used publisher.')

        if state.header.publisher == message.header.publisher:
            # XXX: Can't do this -- might need to checksum it?
            # If the message we just received is exactly the same as the
            # previous message, we can safely ignore it. (This could happen
            # if the publisher is retrying a message that was not fully
            # acknowledged before being partitioned from the recipient, but
            # was actually written.)
            #if state.header.sequence == message.header.sequence:
            #    if previous == message:
            #        logger.debug('Skipping duplicate message.')
            #        continue
            #    else:
            #        raise RepeatedSequenceError(previous, message)
            if state.header.sequence + 1 != message.header.sequence:
                raise SequencingError(
                    'Invalid sequence: {0} to {1}'.format(
                        state.header.sequence,
                        message.header.sequence,
                    )
                )
        else:
            logger.info(
                'Publisher has changed from %r to %r.',
                state.header.publisher,
                message.header.publisher,
            )
            dead.add(state.header.publisher)

            # TODO: This needs to handle starting consumption in the middle of
            # the stream somehow.
            if message.header.sequence != 0:
                raise InvalidSequenceStartError(
                    'Invalid sequence start point: {0}'.format(
                        message.header.sequence,
                    )
                )

    return ConsumerState(
        offset=offset,
        header=message.header,
        dead_publishers=sorted(dead),
    )
