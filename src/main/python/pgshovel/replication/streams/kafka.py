from __future__ import absolute_import

import logging

from kafka.consumer.simple import SimpleConsumer
from kafka.client import KafkaClient

from pgshovel.replication.validation import validate_state
from pgshovel.interfaces.streams_pb2 import Message
from pgshovel.utilities.protobuf import BinaryCodec


logger = logging.getLogger(__name__)


class KafkaStream(object):
    def __init__(self, cluster, set, hosts, topic):
        self.cluster = cluster
        self.set = set
        self.hosts = hosts
        self.topic = topic
        self.codec = BinaryCodec(Message)

    def consume(self, state):
        consumer = SimpleConsumer(KafkaClient(self.hosts), None, self.topic)

        # You can only update one offset at a time with kafka-python, plus
        # dealing with reconstituting global order from a partitioned stream is
        # hard we don't really need to deal with it right now.
        assert len(consumer.offsets) is 1

        if state.HasField('stream_state'):
            # Seeking to a direct offset was not in the PyPI release of
            # kafka-python when this was implemented:
            # https://github.com/mumrah/kafka-python/pull/412
            current = consumer.offsets[0]
            offset = state.stream_state.consumer_state.offset + 1
            delta = offset - current
            logger.debug('Moving to previous replication log offset: %s (current position: %s)...', offset, current)
            consumer.seek(delta, 1)
            assert consumer.offsets[0] == offset

        for offset, message in consumer:
            decoded = self.codec.decode(message.value)
            state = validate_state(state, offset, decoded)
            # XXX: This is necessary because of a bug in protocol buffer oneof.
            state = type(state).FromString(state.SerializeToString())
            yield state, offset, decoded

    @classmethod
    def configure(cls, configuration, cluster, set):
        topic = '{cluster}.{set}.mutations'.format(cluster=cluster.name, set=set)
        return cls(cluster, set, configuration['hosts'], topic)
