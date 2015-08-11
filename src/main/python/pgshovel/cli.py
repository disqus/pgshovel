import code
import functools
import logging

import click
from tabulate import tabulate

from pgshovel import administration
from pgshovel.interfaces.configurations_pb2 import ReplicationSetConfiguration
from pgshovel.utilities.commands import (
    entrypoint,
    pass_cluster,
)
from pgshovel.utilities.datastructures import FormattedSequence
from pgshovel.utilities.protobuf import (
    BinaryCodec,
    TextCodec,
)


logger = logging.getLogger(__name__)


@click.group()
@entrypoint
def main(cluster):
    pass


@main.group(short_help='Manage cluster configuration.')
def cluster():
    pass


@cluster.command(short_help='Initialize a new replication cluster.')
@pass_cluster
def initialize(cluster):
    with cluster:
        administration.initialize_cluster(cluster)


@cluster.command(short_help='Upgrade an existing replication cluster.')
@click.option('--force/--no-force', default=False)
@pass_cluster
def upgrade(cluster, force):
    with cluster:
        administration.upgrade_cluster(cluster, force=force)


@main.group(short_help='Manage replication set configuration.')
def set():
    pass


@set.command(short_help='List replication sets.')
@pass_cluster
def list(cluster):
    with cluster:
        rows = []
        for name, (configuration, stat) in administration.fetch_sets(cluster):
            rows.append((
                name,
                configuration.database.dsn,
                FormattedSequence([t.name for t in configuration.tables]),
                administration.get_version(configuration),
            ))

    # TODO: Bring back pluggable formatters.
    click.echo(tabulate(sorted(rows), headers=('name', 'database', 'table', 'version')))


@set.command(short_help='Retrieve replication set configuration.')
@click.argument('name', type=str)
@pass_cluster
def inspect(cluster, name):
    with cluster:
        data, stat = cluster.zookeeper.get(cluster.get_set_path(name))
        configuration = BinaryCodec(ReplicationSetConfiguration).decode(data)
        click.echo(TextCodec(ReplicationSetConfiguration).encode(configuration))
        click.echo('version: %s' % (administration.get_version(configuration)), err=True)


@set.command(short_help='Create new replication set.')
@click.argument('name', type=str)
@click.argument('configuration', type=click.File('r'), default='-')
@pass_cluster
def create(cluster, name, configuration):
    codec = TextCodec(ReplicationSetConfiguration)
    configuration = codec.decode(configuration.read())

    with cluster:
        return administration.create_set(cluster, name, configuration)


@set.command(short_help='Updating existing replication set.')
@click.argument('name', type=str)
@click.argument('configuration', type=click.File('r'), default='-')
@pass_cluster
def update(cluster, name, configuration):
    codec = TextCodec(ReplicationSetConfiguration)
    configuration = codec.decode(configuration.read())

    # TODO: Support forced removal again.
    with cluster:
        return administration.update_set(cluster, name, configuration)


@set.command(short_help='Drop existing replication set.')
@click.argument('name', type=str)
@pass_cluster
def drop(cluster, name):
    # TODO: Support forced removal again.
    with cluster:
        return administration.drop_set(cluster, name)


@main.command(short_help='Launch interactive shell.')
@pass_cluster
def shell(cluster):
    with cluster:
        return code.interact(local={'cluster': cluster})


__main__ = functools.partial(
    main,
    auto_envvar_prefix='PGSHOVEL',
)
