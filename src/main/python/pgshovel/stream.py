import logging


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


def validate(messages):
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
