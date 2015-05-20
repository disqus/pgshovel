import functools
import logging
import posixpath

from kazoo.client import KazooClient

from pgshovel import __version__
from pgshovel.interfaces.configurations_pb2 import ClusterConfiguration
from pgshovel.utilities.protobuf import BinaryCodec


logger = logging.getLogger(__name__)


class VersionMismatchError(Exception):
    def __init__(self, cluster_version, local_version=__version__):
        message = 'Local version (%s) does not match cluster version (%s)' % (local_version, cluster_version)
        super(VersionMismatchError, self).__init__(message)

        self.cluster_version = cluster_version
        self.local_version = local_version


def check_version(cluster):
    zookeeper = cluster.zookeeper

    logger.debug('Checking cluster version...')
    data, stat = zookeeper.get(cluster.path)
    configuration = BinaryCodec(ClusterConfiguration).decode(data)
    if __version__ != configuration.version:
        raise VersionMismatchError(configuration.version)

    logger.debug('Remote version: %s', configuration.version)

    ztransaction = zookeeper.transaction()
    ztransaction.check(cluster.path, version=stat.version)
    return ztransaction


class Cluster(object):
    def __init__(self, name, configuration):
        self.name = name
        self.configuration = configuration

        self.zookeeper = KazooClient(configuration.get('zookeeper', 'hosts'))

    def __enter__(self):
        self.start()

    def __exit__(self, type, value, traceback):
        self.stop()

    def __str__(self):
        return self.name

    @property
    def path(self):
        return '/%s' % (self.name,)

    @property
    def schema(self):
        return 'pgshovel_%s' % (self.name,)

    def start(self):
        # TODO: Needs timeout
        self.zookeeper.start()

    def stop(self):
        self.zookeeper.stop()

    def get_trigger_name(self, set):
        return 'pgshovel_%s_%s_log' % (self.name, set)

    @property
    def get_set_path(self):
        return functools.partial(posixpath.join, self.path, 'sets')

    def get_queue_name(self, set):
        return 'pgshovel:%s:%s' % (self.name, set)
