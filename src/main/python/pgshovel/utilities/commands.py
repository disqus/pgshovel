import functools
import logging.config

import click
from kazoo.client import KazooClient

from pgshovel.cluster import Cluster
from pgshovel.utilities import load
from pgshovel.utilities.templates import resource_filename


pass_cluster = click.make_pass_decorator(Cluster)


class LoadPathType(click.ParamType):
    name = 'path'

    def get_metavar(self, param):
        return 'IMPORT_PATH'

    def convert(self, value, param, ctx):
        try:
            return load(value)
        except AttributeError:
            self.fail('Could not load module attribute: %s' % value, param, ctx)
        except ImportError:
            self.fail('Could not import module: %s' % value, param, ctx)
        except ValueError:
            self.fail('Invalid module path: %s' % value, param, ctx)


def entrypoint(function):
    """
    Adds common command-line options to the provided click command or group
    implementation.

    This must be the last (innermost) decorator used.
    """
    @click.option(
        '--cluster',
        default='default',
        help="Unique identifier for this cluster.",
    )
    @click.option(
        '--logging-configuration',
        type=click.Path(dir_okay=False, exists=True),
        default=resource_filename('configuration/logging.conf'),
        help="Path to Python logging configuration file.",
    )
    @click.option(
        '--zookeeper-hosts',
        default='127.0.0.1:2181',
        help="ZooKeeper connection string (as a comma separated list of hosts, with optional chroot path.)",
    )
    @click.version_option()
    @click.pass_context
    @functools.wraps(function)  # preserve original name, etc.
    def decorated(context, cluster, zookeeper_hosts, logging_configuration, *args, **kwargs):
        logging.config.fileConfig(logging_configuration)

        zookeeper = KazooClient(zookeeper_hosts)
        context.obj = cluster = Cluster(cluster, zookeeper)

        return function(cluster, *args, **kwargs)

    # TODO: Also partial the default ``auto_envvar_prefix`` to ``main``.
    return decorated
