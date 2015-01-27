from pgshovel.groups import GroupManager


class Environment(object):
    def __init__(self, zookeeper):
        self.zookeeper = zookeeper


class Application(object):
    def __init__(self, name, environment):
        self.name = name
        self.environment = environment

        self.groups = GroupManager(self)

    def __str__(self):
        return self.name

    @property
    def path(self):
        return '/%s' % (self.name,)

    @property
    def schema(self):
        return '_pgshovel_%s' % (self.name,)

    def start(self):
        self.groups.start()

    def stop(self):
        self.groups.stop()
