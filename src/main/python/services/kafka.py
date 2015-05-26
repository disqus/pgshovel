import logging
import signal
import os
from services import Kafka, get_open_port


if __name__ == '__main__':
    import optparse

    logging.basicConfig(level=logging.DEBUG)

    parser = optparse.OptionParser()
    parser.add_option('--host', default='localhost')
    parser.add_option('--port', type=int, default=None)
    parser.add_option('--broker-id', type=int, default=1)
    parser.add_option('--zookeeper', default='127.0.0.1:2181')

    options, arguments = parser.parse_args()

    # TODO: allow proxying logs

    max_message_size = int(100 * 1e6)
    server = Kafka(
        os.getcwd(),
        options.host,
        options.port if options.port is not None else get_open_port(),
        {
            'broker.id': options.broker_id,
            'zookeeper.connect': options.zookeeper,
            'message.max.bytes': max_message_size,
            'replica.fetch.max.bytes': max_message_size,
        },
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
