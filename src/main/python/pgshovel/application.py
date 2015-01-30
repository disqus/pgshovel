from kazoo.client import KazooClient

from pgshovel.groups import GroupManager


class Environment(object):
    def __init__(self, configuration):
        self.configuration = configuration

    @property
    def zookeeper(self):
        try:
            return self.__zookeeper
        except AttributeError:
            zookeeper = self.__zookeeper = KazooClient(self.configuration.zookeeper.hosts)
        return zookeeper


class Application(object):
    def __init__(self, environment, configuration):
        self.environment = environment
        self.configuration = configuration

        self.groups = GroupManager(self)

    def __str__(self):
        return self.configuration.name

    @property
    def path(self):
        return '/%s' % (self.configuration.name,)

    @property
    def schema(self):
        return '_pgshovel_%s' % (self.configuration.name,)

    def start(self):
        self.groups.start()

    def stop(self):
        self.groups.stop()
