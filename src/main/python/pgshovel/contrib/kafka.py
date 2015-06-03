"""
.. caution::

    The default Kafka broker configuration (for 0.8, at time of writing) has
    maximum message size of 1 MB (``message.max.bytes`` property).

    Having too low of a value for this setting can lead to rejected publishes
    (and halted replication) if or when the size of a ``MutationBatch`` exceeds
    the maximum message size accepted by the broker. This can be particularly
    problematic when dealing with very active workloads, or data sets that have
    very large rows.

    In addition to tuning the ``message.max.bytes`` size for your particular
    workload, it is beneficial to adjust the queue configuration located in the
    ``pgq.queue`` table -- specifically the ``queue_ticker_max_count`` and
    ``queue_ticker_max_lag`` keys -- to encourage smaller and more frequently
    generated batches.

"""
from __future__ import absolute_import

import functools
import threading

import click

from pgshovel.relay import relay_entrypoint
from pgshovel.utilities import import_extras
from pgshovel.utilities.commands import LoadPathType

with import_extras('kafka'):
    from kafka.client import KafkaClient
    from kafka.producer.simple import SimpleProducer


class KafkaWriter(object):
    def __init__(self, producer, topic, codec):
        self.producer = producer
        self.topic = topic
        self.codec = codec
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

    def push(self, batch):
        with self.__lock:  # TODO: ensure this is required, better safe than sorry
            self.producer.send_messages(self.topic, self.codec.encode(batch))


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
@click.option(
    '--codec',
    type=LoadPathType(),
    default='pgshovel.codecs:json',
    help="Codec used when encoding batches for publishing.",
)
@relay_entrypoint
def main(cluster, set, kafka_hosts, kafka_topic, codec):
    client = KafkaClient(kafka_hosts)
    producer = SimpleProducer(client)
    topic = kafka_topic.format(cluster=cluster.name, set=set)
    return KafkaWriter(producer, topic, codec)


__main__ = functools.partial(
    main,
    auto_envvar_prefix='PGSHOVEL',
)
