from __future__ import absolute_import

import functools
import uuid

import pytest
from pgshovel.contrib.kafka import KafkaWriter
from pgshovel.contrib.msgpack import codec
from pgshovel.utilities import import_extras
from tests.pgshovel.fixtures import (
    batch_builder,
    kafka,
    zookeeper,
)

with import_extras('kafka'):
    from kafka.client import KafkaClient
    from kafka.consumer.simple import SimpleConsumer
    from kafka.producer.simple import SimpleProducer


zookeeper = pytest.yield_fixture(zookeeper)
kafka = pytest.yield_fixture(kafka)


def test_handler(kafka):
    kafka_broker, _ = kafka
    hosts = '%s:%s' % (kafka_broker.host, kafka_broker.port)
    topic = 'mutations'

    client = KafkaClient(hosts)
    producer = SimpleProducer(client, topic)
    writer = KafkaWriter(producer, topic, codec)

    batch = batch_builder(3)
    writer.push(batch)

    consumer = SimpleConsumer(client, 'test', topic)
    ((offset, message),) = consumer.get_messages()
    assert codec.decode(message.value) == batch
