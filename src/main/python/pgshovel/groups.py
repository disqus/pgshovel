import hashlib
import logging

import posixpath

from pgshovel.interfaces.groups_pb2 import GroupConfiguration
from pgshovel.utilities.protobuf import BinaryCodec
from pgshovel.utilities.zookeeper import SynchronizedMapping


logger = logging.getLogger(__name__)


class Group(object):
    def __init__(self, name, configuration, stat=None):
        self.name = name
        self.configuration = configuration
        self.stat = stat

    def __str__(self):
        return self.name

    @property
    def version(self):
        return hashlib.md5(self.configuration.SerializeToString()).hexdigest()


class GroupManager(object):
    codec = BinaryCodec(GroupConfiguration)

    def __init__(self, application):
        self.application = application
        self.__groups = SynchronizedMapping(
            application.environment.zookeeper,
            self.path,
            lambda name, data, stat: Group(name, self.codec.decode(data), stat),
        )

    @property
    def path(self):
        return posixpath.join(self.application.path, 'groups')

    def all(self):
        return self.__groups.itervalues()

    def get(self, name, transaction=None):
        group = self.__groups[name]
        if transaction:
            transaction.check(posixpath.join(self.path, name), version=group.stat.version)
        return group

    def set(self, group, transaction=None):
        if not group.stat:
            method = (transaction if transaction else self.application.environment.zookeeper).create
        else:
            method = transaction.set_data if transaction else self.application.environment.zookeeper.set

        method(
            posixpath.join(self.path, group.name),
            self.codec.encode(group.configuration),
        )

    def delete(self, group, transaction=None):
        method = (transaction if transaction else self.application.environment.zookeeper).delete
        return method(posixpath.join(self.path, group.name), version=group.stat.version)

    def start(self):
        self.__groups.start()

    def stop(self):
        self.__groups.stop()
