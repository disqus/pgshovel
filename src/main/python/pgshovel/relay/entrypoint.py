import logging
import signal

import click

from pgshovel.relay.relay import Relay
from pgshovel.utilities import commands


logger = logging.getLogger(__name__)


def entrypoint(command):
    """
    Adds common command-line options, arguments, and signal handling to the
    provided relay constructor.

    This must be the last (innermost) decorator used.
    """
    @click.argument('set')
    @click.option(
        '--consumer-id',
        default='default',
        help="PgQ consumer registration identifier.",
    )
    @commands.entrypoint
    def decorated(cluster, set, consumer_id, *args, **kwargs):
        handler = command(cluster, set, *args, **kwargs)

        with cluster:
            relay = Relay(cluster, set, consumer_id, handler)
            relay.start()

            def __request_exit(signal, frame):
                logger.info('Caught signal %s, stopping...', signal)
                relay.stop_async()

            signal.signal(signal.SIGINT, __request_exit)
            signal.signal(signal.SIGTERM, __request_exit)

            while True:
                relay.join(0.1)
                if not relay.is_alive():
                    relay.result()
                    break

    return decorated
