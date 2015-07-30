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


class RepeatedSequenceError(SequencingError):
    template = 'Repeated sequence: {0} and {1}'


def validate(messages):
    """
    Validates a stream of Message instances, ensuring that the correct
    sequencing order is maintained and all messages are present.

    Duplicate messages are dropped if they have already been yielded.
    """
    # Keep a map of all of the last seen messages by publisher.
    publishers = {}

    for message in messages:
        # TODO: This could just store the previous sequence ID and content hash
        # if memory usage is a concern here, instead of the full message.
        key = message.header.publisher
        previous = publishers.get(key)
        if previous is not None:
            # If this the last message we just saw, ignore it if the contents
            # are the same. This could happen if the publisher is retrying a
            # message that was not acknowledged, but still written.
            if previous.header.sequence == message.header.sequence:
                if previous == message:
                    logger.debug('Skipping duplicate message.')
                    continue
                else:
                    raise RepeatedSequenceError(previous, message)
            elif previous.header.sequence + 1 != message.header.sequence:
                raise SequencingError(previous, message)

        yield message

        publishers[key] = message
