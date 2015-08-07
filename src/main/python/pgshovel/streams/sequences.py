"""
Tools for validating input streams.
"""
import logging


logger = logging.getLogger(__name__)


class SequencingError(Exception):
    """
    Error raised when a message contains an invalid sequence value.
    """


class InvalidPublisher(SequencingError):
    """
    Error raised when a message is received from an unexpected publisher.
    """


class RepeatedSequenceError(SequencingError):
    """
    Error raised when a sequence value is reused for a different message from
    the same publisher.
    """


class InvalidSequenceStartError(Exception):
    """
    Error raised when a message is recieved from a new publisher that starts
    at an incorrect sequence value.
    """


def validate(messages):
    """
    Validates a stream of Message instances, ensuring that the correct
    sequencing order is maintained, all messages are present, and only a single
    publisher is communicating on the stream.

    Duplicate messages are dropped if they have already been yielded.
    """
    # TODO: Also warn on non-monotonic timestamp advancement.

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
                    raise SequencingError(
                        'Invalid sequence: {0} to {1}'.format(
                            previous.header.sequence,
                            message.header.sequence,
                        )
                    )
            else:
                logger.info(
                    'Publisher of %r has changed from %r to %r.',
                    messages,
                    previous.header.publisher,
                    message.header.publisher,
                )
                dead.add(previous.header.publisher)
                previous = None

        # TODO: This needs to handle starting consumption in the middle of the
        # stream somehow.
        if previous is None and message.header.sequence != 0:
            raise InvalidSequenceStartError(
                'Invalid sequence start point: {0}'.format(
                    message.header.sequence,
                )
            )

        yield message

        previous = message
