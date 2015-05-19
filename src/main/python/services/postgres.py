import logging
import signal
import os
from services import Postgres, get_open_port


if __name__ == '__main__':
    import optparse

    logging.basicConfig(level=logging.DEBUG)

    parser = optparse.OptionParser()
    parser.add_option('--host', default='localhost')
    parser.add_option('--port', type=int, default=None)
    options, arguments = parser.parse_args()

    # TODO: allow proxying logs

    server = Postgres(
        os.getcwd(),
        options.host,
        options.port if options.port is not None else get_open_port(),
        max_prepared_transactions=10,  # TODO: just write the config file, proxy over stdin
    )

    server.setup()
    server.start()
    print '%s:%s' % (server.host, server.port)
    try:
        # TODO: handle INT as stop, TERM as teardown
        signal.pause()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()
        server.teardown()
