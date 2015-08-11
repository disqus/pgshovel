from __future__ import absolute_import

import functools
import threading

import click

from pgshovel.relay.entrypoint import entrypoint
from pgshovel.streams.interfaces_pb2 import Message
from pgshovel.utilities import import_extras
from pgshovel.utilities.protobuf import BinaryCodec

with import_extras('kafka'):
    from kafka.client import KafkaClient
    from kafka.producer.simple import SimpleProducer


class KafkaWriter(object):
    def __init__(self, producer, topic, codec):
        self.producer = producer
        self.topic = topic
        self.codec = codec

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


@click.command(
    help="Publishes mutation batches to the specified Kafka topic.",
)
@click.option(
    '--kafka-hosts',
    default='127.0.0.1:9092',
    help="Kafka broker connection string (as a comma separated list of hosts.)",
)
@click.option(
    '--kafka-topic',
    default='{cluster}.{set}.mutations',
    help="Destination Topic for mutation batch publishing.",
)
@entrypoint
def main(cluster, set, kafka_hosts, kafka_topic):
    client = KafkaClient(kafka_hosts)
    producer = SimpleProducer(client)
    topic = kafka_topic.format(cluster=cluster.name, set=set)
    return KafkaWriter(producer, topic, BinaryCodec(Message))


__main__ = functools.partial(main, auto_envvar_prefix='PGSHOVEL')

if __name__ == '__main__':
    __main__()
