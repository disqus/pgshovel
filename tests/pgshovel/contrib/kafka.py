from __future__ import absolute_import

import uuid

from pgshovel.contrib.kafka import KafkaWriter
from pgshovel.contrib.msgpack import codec
from pgshovel.utilities import import_extras
from tests.pgshovel.fixtures import batch_builder

with import_extras('kafka'):
    from kafka.client import KafkaClient
    from kafka.consumer.simple import SimpleConsumer
    from kafka.producer.simple import SimpleProducer


def test_handler():
    topic = '%s-mutations' % (uuid.uuid1().hex,)

    client = KafkaClient('kafka')
    producer = SimpleProducer(client)
    writer = KafkaWriter(producer, topic, codec)

    batch = batch_builder(3)
    writer.push(batch)

    consumer = SimpleConsumer(client, 'test', topic)
    ((offset, message),) = consumer.get_messages()
    assert codec.decode(message.value) == batch
