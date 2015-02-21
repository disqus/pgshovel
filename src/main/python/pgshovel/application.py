import functools
import posixpath

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

    @property
    def get_group_path(self):
        return functools.partial(posixpath.join, self.path, 'groups')

    @property
    def get_consumer_path(self):
        return functools.partial(posixpath.join, self.path, 'consumers')

    def get_consumer_group_membership_path(self, consumer_group):
        return functools.partial(self.get_consumer_path, consumer_group, 'members')

    def get_ownership_lock_path(self, consumer_group):
        return functools.partial(self.get_consumer_path ,consumer_group, 'ownership')

    def get_queue_name(self, group):
        return 'pgshovel:%s:%s' % (self.configuration.name, group)
