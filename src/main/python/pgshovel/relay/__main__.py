import functools
import logging
import signal

import click
import yaml

from pgshovel.relay.relay import Relay
from pgshovel.utilities.components import configure
from pgshovel.utilities.commands import entrypoint


@click.command()
@click.option(
    '--consumer-id',
    default='default',
    help="PgQ consumer registration identifier.",
)
@click.argument('configuration', type=click.File('rb'))  # TODO: YamlFile
@click.argument('set')
@entrypoint
def main(cluster, consumer_id, configuration, set):
    configuration = yaml.load(configuration)
    with cluster:
        stream = configure(configuration['stream'])(cluster, set)

        relay = Relay(cluster, set, consumer_id, stream)
        relay.start()

        def __request_exit(signal, frame):
            relay.stop_async()

        signal.signal(signal.SIGINT, __request_exit)
        signal.signal(signal.SIGTERM, __request_exit)

        while True:
            relay.join(0.1)
            if not relay.is_alive():
                relay.result()
                break


__main__ = functools.partial(
    main,
    auto_envvar_prefix='PGSHOVEL',
)


if __name__ == '__main__':
    __main__()
