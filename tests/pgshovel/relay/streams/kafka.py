from __future__ import absolute_import

import operator
import uuid

from pgshovel.relay.streams.kafka import KafkaWriter
from pgshovel.utilities import import_extras
from tests.pgshovel.streams.fixtures import transaction

with import_extras('kafka'):
    from kafka.client import KafkaClient
    from kafka.consumer.simple import SimpleConsumer
    from kafka.producer.simple import SimpleProducer


def test_writer():
    topic = '%s-mutations' % (uuid.uuid1().hex,)

    client = KafkaClient('kafka')
    producer = SimpleProducer(client)
    writer = KafkaWriter(producer, topic)

    inputs = list(transaction)
    writer.push(inputs)

    consumer = SimpleConsumer(client, 'test', topic, auto_offset_reset='smallest')

    outputs = map(
        writer.codec.decode,
        map(
            operator.attrgetter('message.value'),
            list(consumer.get_messages(count=3)),
        ),
    )

    assert outputs == inputs
