import code
import sys

from pgshovel import administration
from pgshovel.interfaces.groups_pb2 import (
    DatabaseConfiguration,
    GroupConfiguration,
)
from pgshovel.utilities.commands import (
    FormatOption,
    Option,
    command,
    formatters,
)
from pgshovel.utilities.protobuf import (
    BinaryCodec,
    TextCodec,
)


def __get_codec(options, cls):
    # TODO: allow switching between text and binary codecs as an option
    return TextCodec(cls)


@command
def shell(options, application):
    with application:
        exports = {
            'application': application,
            'environment': application.environment,
        }
        return code.interact(local=exports)


@command(description="Initializes a new cluster in ZooKeeper.")
def initialize_cluster(options, application):
    with application.environment:
        return administration.initialize_cluster(application)


@command(
    description="Lists the registered capture groups.",
    options=(FormatOption,),
)
def list_groups(options, application):
    with application:
        zookeeper = application.environment.zookeeper
        names = zookeeper.get_children(application.get_group_path())

        rows = []
        for name, (configuration, stat) in administration.fetch_groups(application, names):
            rows.append((
                name,
                configuration.database.name,
                configuration.table.name,
                administration.get_version(configuration),
            ))

        print formatters[options.format](rows, headers=('name', 'database', 'table', 'version'))


@command(description="Creates a new capture group.")
def create_group(options, application, name):
    with application:
        configuration = __get_codec(options, GroupConfiguration).decode(sys.stdin.read())
        return administration.create_group(application, name, configuration)


@command(description="Provides details about a capture group.")
def inspect_group(options, application, name):
    with application:
        data, stat = application.environment.zookeeper.get(administration.get_group_path(application, name))
        configuration = BinaryCodec(GroupConfiguration).decode(data)
        sys.stdout.write(__get_codec(options, GroupConfiguration).encode(configuration))
        sys.stderr.write(administration.get_version(configuration) + '\n')


@command(description="Updates a capture group.")
def update_group(options, application, name):
    with application:
        configuration = __get_codec(options, GroupConfiguration).decode(sys.stdin.read())
        return administration.update_group(application, name, configuration)


@command(
    options=(
        Option('--force', action='store_true', help='Skip database transactions on source databases.'),
    ),
)
def move_groups(options, application, *names):
    with application:
        database = __get_codec(options, DatabaseConfiguration).decode(sys.stdin.read())
        return administration.move_groups(application, names, database, force=options.force)


@command(
    description="Drops capture group(s).",
    options=(
        Option('--force', action='store_true', help='Skip database transactions.'),
    ),
)
def drop_groups(options, application, *names):
    with application:
        return administration.drop_groups(application, names, force=options.force)
