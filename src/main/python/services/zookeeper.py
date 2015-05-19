import logging
import threading
import signal
import os
from services import ZooKeeper, get_open_port
import sys


logger = logging.getLogger(__name__)


if __name__ == '__main__':
    import optparse

    logging.basicConfig(level=logging.DEBUG)

    # TODO: allow proxying logs
    # TODO: support clustering

    parser = optparse.OptionParser()
    parser.add_option('--host', default='localhost')
    parser.add_option('--port', type=int, default=None)
    options, arguments = parser.parse_args()

    server = ZooKeeper(
        os.getcwd(),
        options.host,
        options.port if options.port is not None else get_open_port(),
    )

    server.setup()
    server.start()

    stopping = threading.Event()

    def __handle_stop(signal, frame):
        if server.child:
            logger.info('Stopping %r...', server)
            server.stop()
        else:
            logger.info('Starting %r...', server)
            server.start()

    def __handle_teardown(signal, frame):
        stopping.set()

    def __handle_info(signal, frame):
        if server.child:
            print >> sys.stderr, '%s:%s' % (server.host, server.port)
        else:
            print >> sys.stderr, '(not running)'

    signal.signal(signal.SIGINT, __handle_stop)
    signal.signal(signal.SIGQUIT, __handle_teardown)
    signal.signal(signal.SIGINFO, __handle_info)

    while True:
        if stopping.wait(0.1):
            break

    if server.child:
        server.stop()

    server.teardown()
