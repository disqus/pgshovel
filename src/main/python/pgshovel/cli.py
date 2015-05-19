import code
import logging
import signal
import sys
import time
from datetime import timedelta

from pgshovel import administration
from pgshovel.interfaces.configurations_pb2 import ReplicationSetConfiguration
from pgshovel.relay import Relay, StreamWriter
from pgshovel.utilities.commands import (
    FormatOption,
    Option,
    command,
    formatters,
)
from pgshovel.utilities.datastructures import FormattedSequence
from pgshovel.utilities.protobuf import (
    BinaryCodec,
    TextCodec,
)


logger = logging.getLogger(__name__)


def __get_codec(options, cls):
    # TODO: allow switching between text and binary codecs as an option
    return TextCodec(cls)


@command
def shell(options, cluster):
    with cluster:
        exports = {
            'cluster': cluster,
            'environment': cluster.environment,
        }
        return code.interact(local=exports)


@command(description="Initializes a new cluster in ZooKeeper.")
def initialize_cluster(options, cluster):
    with cluster.environment:
        return administration.initialize_cluster(cluster)


@command(
    options=(
        Option('--force', action='store_true', default=False, help='skip version checks'),
    )
)
def upgrade_cluster(options, cluster):
    with cluster.environment:
        return administration.upgrade_cluster(cluster, force=options.force)


@command(
    description="Lists the registered replication sets.",
    options=(FormatOption,),
)
def list_sets(options, cluster):
    with cluster:
        rows = []
        for name, (configuration, stat) in administration.fetch_sets(cluster):
            rows.append((
                name,
                ', '.join(d.dsn for d in configuration.databases),
                FormattedSequence([t.name for t in configuration.tables]),
                administration.get_version(configuration),
            ))

        print formatters[options.format](sorted(rows), headers=('name', 'database', 'table', 'version'))


@command(description="Creates a new replication set.")
def create_set(options, cluster, name, path='/dev/stdin'):
    with open(path, 'r') as f:
        data = f.read()

    with cluster:
        configuration = __get_codec(options, ReplicationSetConfiguration).decode(data)
        return administration.create_set(cluster, name, configuration)


@command(description="Provides details about a replication set.")
def inspect_set(options, cluster, name):
    with cluster:
        data, stat = cluster.environment.zookeeper.get(cluster.get_set_path(name))
        configuration = BinaryCodec(ReplicationSetConfiguration).decode(data)
        sys.stdout.write(__get_codec(options, ReplicationSetConfiguration).encode(configuration))
        sys.stderr.write(administration.get_version(configuration) + '\n')


allow_forced_removal = Option(
    '--allow-forced-removal',
    action='store_true',
    default=False,
    help='allows removing databases from ZooKeeper, even if the database '
         'cannot be reached for the replication set to be unconfigured '
         '(triggers dropped, queue removed, etc.)',
)


@command(
    description="Updates a replication set.",
    options=(
        allow_forced_removal,
    ),
)
def update_set(options, cluster, name, path='/dev/stdin'):
    with open(path, 'r') as f:
        data = f.read()

    with cluster:
        configuration = __get_codec(options, ReplicationSetConfiguration).decode(data)
        return administration.update_set(
            cluster,
            name,
            configuration,
            allow_forced_removal=options.allow_forced_removal,
        )


@command(
    description="Drops replication set(s).",
    options=(
        allow_forced_removal,
    ),
)
def drop_set(options, cluster, name):
    with cluster:
        return administration.drop_set(
            cluster,
            name,
            allow_forced_removal=options.allow_forced_removal,
        )


@command
def relay(options, cluster, set, consumer):
    handler = StreamWriter(sys.stdout)

    with cluster:
        relay = Relay(cluster, set, consumer, handler)
        relay.start()

        def __request_exit(signal, frame):
            logger.info('Caught signal %s, stopping...', signal)
            relay.stop_async()

        def __request_info(signal, frame):
            for started, worker in relay.workers().values():
                logger.info('%s uptime: %s', worker, timedelta(seconds=time.time() - started))

        signal.signal(signal.SIGINT, __request_exit)
        signal.signal(signal.SIGTERM, __request_exit)
        signal.signal(signal.SIGINFO, __request_info)

        while True:
            relay.join(0.1)
            if not relay.is_alive():
                relay.result()
                break
