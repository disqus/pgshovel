import importlib
import json

from pgqueue import Consumer

from pgshovel.commands import (
    Option,
    command,
)


def load(path):
    module, name = path.split(':')
    return getattr(importlib.import_module(module), name)


class StreamWriter(object):
    def __init__(self, application, stream='sys:stdout', encoder='json:dump'):
        self.stream = load(stream)
        self.encoder = load(encoder)

    def __call__(self, payload):
        self.encoder(payload, self.stream)
        self.stream.write('\n')


@command(
    description="Consume captured transactions from the specified database.",
    options=(
        Option('--consumer-id', default='pgshovel', metavar='IDENTIFIER', help='PGQ Consumer ID (%default)'),
    ),
)
def consume(options, application, database, handler='pgshovel.consumer:StreamWriter', *arguments):
    handler = load(handler)(application, *arguments)

    database = application.databases.get(database)
    connection = database.connect(role='consumer')
    with connection.cursor() as cursor:
        consumer = Consumer(application.queue, options.consumer_id)
        consumer.register(cursor)
        connection.commit()

        while True:
            for event in consumer.next_events(cursor, commit=False):
                payload = json.loads(event.data)
                handler(payload)

            connection.commit()
