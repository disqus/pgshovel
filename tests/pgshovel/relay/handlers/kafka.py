from __future__ import absolute_import

import operator
import uuid

from pgshovel.relay.handlers.kafka import KafkaWriter
from pgshovel.streams.interfaces_pb2 import Message
from pgshovel.utilities import import_extras
from pgshovel.utilities.protobuf import BinaryCodec
from tests.pgshovel.streams.fixtures import transaction

with import_extras('kafka'):
    from kafka.client import KafkaClient
    from kafka.consumer.simple import SimpleConsumer
    from kafka.producer.simple import SimpleProducer


def test_handler():
    topic = '%s-mutations' % (uuid.uuid1().hex,)

    codec = BinaryCodec(Message)

    client = KafkaClient('kafka')
    producer = SimpleProducer(client)
    writer = KafkaWriter(producer, topic, codec)

    inputs = list(transaction)
    writer.push(inputs)

    consumer = SimpleConsumer(client, 'test', topic, auto_offset_reset='smallest')

    outputs = map(
        codec.decode,
        map(
            operator.attrgetter('message.value'),
            list(consumer.get_messages(count=3)),
        ),
    )

    assert outputs == inputs
