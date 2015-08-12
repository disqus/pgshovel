from __future__ import absolute_import

import functools
import threading

import click

from pgshovel.interfaces.streams_pb2 import Message
from pgshovel.utilities import import_extras
from pgshovel.utilities.protobuf import BinaryCodec

with import_extras('kafka'):
    from kafka.client import KafkaClient
    from kafka.producer.simple import SimpleProducer


class KafkaWriter(object):
    def __init__(self, producer, topic):
        self.producer = producer
        self.topic = topic
        self.codec = BinaryCodec(Message)

        # TODO: Might not need to be thread safe any more?
        self.__lock = threading.Lock()

        self.producer.client.ensure_topic_exists(topic)

    def __str__(self):
        return 'Kafka writer (topic: %s, codec: %s)' % (self.topic, type(self.codec).__name__)

    def __repr__(self):
        return '<%s: %s on %r>' % (
            type(self).__name__,
            self.topic,
            [':'.join(map(str, h)) for h in self.producer.client.hosts]
        )

    def push(self, messages):
        with self.__lock:  # TODO: ensure this is required, better safe than sorry
            self.producer.send_messages(self.topic, *map(self.codec.encode, messages))

    @classmethod
    def configure(cls, configuration, cluster, set):
        return cls(
            SimpleProducer(KafkaClient(configuration['hosts'])),
            '{cluster}.{set}.mutations'.format(
                cluster=cluster.name,
                set=set,
            )
        )
