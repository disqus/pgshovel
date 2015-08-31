from __future__ import absolute_import

import logging
from itertools import imap

from kafka.consumer.simple import SimpleConsumer
from kafka.client import KafkaClient

from pgshovel.replication.validation import validate_state
from pgshovel.interfaces.streams_pb2 import Message
from pgshovel.streams.utilities import prime_for_batch_start
from pgshovel.utilities.protobuf import BinaryCodec


logger = logging.getLogger(__name__)


class KafkaStream(object):
    def __init__(self, cluster, set, hosts, topic, prime_threshold):
        self.cluster = cluster
        self.set = set
        self.hosts = hosts
        self.topic = topic
        self.codec = BinaryCodec(Message)
        self.prime_threshold = prime_threshold

    def consume(self, state):
        """
        Starts consuming from the configured Kafka topic given a possible
        existing ``pgshovel.interfaces.replication_pb2:State``.

        If the provided ``state`` does not contain a
        ``stream_state.consumer_state`` value, the ``KafaStream`` attempts to
        start reading from the Kafka topic after first "priming" the stream.
        Priming involves consuming messages from the topic looking for a
        ``BeginOperation``. Any message that is not a ``BeginOperation`` is
        dropped, until a ``BeginOperation`` is seen or the ``prime_threshold``
        is reached. The latter of which raises a
        ``pgshovel.streams.utilities:UnableToPrimeError`` error.

        In general, it makes sense to set the ``prime_threshold`` to high enough
        value that exceeds the max transaction size you expect to see in your
        data.  Generally speaking a  ``prime_threshold`` can effectively be
        infinite (and you could construct the stream with ``float('inf')``,
        however the lack of a ``BeginOperation`` in the stream would cause the
        stream to hang, possibly forever, so the ``prime_threshold`` config
        parameter is provided to raise an exception if this unexpected behavior
        occurs.
        """
        consumer = SimpleConsumer(KafkaClient(self.hosts), None, self.topic)

        # You can only update one offset at a time with kafka-python, plus
        # dealing with reconstituting global order from a partitioned stream is
        # hard we don't really need to deal with it right now.
        assert len(consumer.offsets) is 1

        decoded = imap(
            lambda (offset, msg): (offset, self.codec.decode(msg.value)),
            consumer
        )

        if state.stream_state.HasField('consumer_state'):
            # Seeking to a direct offset was not in the PyPI release of
            # kafka-python when this was implemented:
            # https://github.com/mumrah/kafka-python/pull/412
            current = consumer.offsets[0]
            offset = state.stream_state.consumer_state.offset + 1
            delta = offset - current
            logger.debug('Moving to previous replication log offset: %s (current position: %s)...', offset, current)
            consumer.seek(delta, 1)
            assert consumer.offsets[0] == offset
        else:
            logger.info('No consumer state provided, will attempt to prime to begin BeginOperation')
            # The call to ``prime_for_batch_start`` "primes" the stream by
            # dropping messages until it sees a message that is an intance of
            # one of the types in
            # ``pgshovel.replication.validation.TRANSACTION_START_EVENT_TYPES``
            decoded = prime_for_batch_start(
                max_messages=self.prime_threshold,
                stream=decoded
            )

        for offset, message in decoded:
            state = validate_state(state, offset, message)
            # XXX: This is necessary because of a bug in protocol buffer oneof.
            state = type(state).FromString(state.SerializeToString())
            yield state, offset, message


    @classmethod
    def configure(cls, configuration, cluster, set):
        topic = '{cluster}.{set}.mutations'.format(cluster=cluster.name, set=set)
        return cls(
            cluster,
            set,
            configuration['hosts'],
            topic,
            configuration.get('prime_threshold', 1000)
        )
