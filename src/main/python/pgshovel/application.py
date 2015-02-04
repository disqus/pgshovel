from kazoo.client import KazooClient



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

    def __enter__(self):
        self.start()

    def __exit__(self, type, value, traceback):
        self.stop()

    def stop(self):
        self.zookeeper.stop()

    def start(self):
        self.zookeeper.start()


class Application(object):
    def __init__(self, environment, configuration):
        self.environment = environment
        self.configuration = configuration

    def __enter__(self):
        self.start()

    def __exit__(self, type, value, traceback):
        self.stop()

    def __str__(self):
        return self.configuration.name

    @property
    def path(self):
        return '/%s' % (self.configuration.name,)

    @property
    def schema(self):
        return '_pgshovel_%s' % (self.configuration.name,)

    def start(self):
        self.environment.start()

    def stop(self):
        self.environment.stop()
