from pgshovel.codec import JsonCodec
from pgshovel.databases import DatabaseManager
from pgshovel.groups import GroupManager


class Environment(object):
    def __init__(self, zookeeper):
        self.zookeeper = zookeeper


class Application(object):
    codec = JsonCodec()

    def __init__(self, name, environment):
        self.name = name
        self.environment = environment

        self.databases = DatabaseManager(self)
        self.groups = GroupManager(self)

    def __str__(self):
        return self.name

    @property
    def path(self):
        return '/%s' % (self.name,)

    @property
    def schema(self):
        return '_pgshovel_%s' % (self.name,)

    @property
    def queue(self):
        return '_pgshovel_%s.transactions' % (self.name,)
