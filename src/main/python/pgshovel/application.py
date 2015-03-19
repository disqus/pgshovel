import functools
import posixpath

from kazoo.client import KazooClient
from cached_property import cached_property



class Environment(object):
    def __init__(self, zookeeper_hosts):
        self.zookeeper = KazooClient(zookeeper_hosts)

    def __enter__(self):
        self.start()

    def __exit__(self, type, value, traceback):
        self.stop()

    def stop(self):
        self.zookeeper.stop()

    def start(self):
        self.zookeeper.start()


class Application(object):
    def __init__(self, name, environment):
        self.name = name
        self.environment = environment

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
        self.environment.start()

    def stop(self):
        self.environment.stop()

    def get_trigger_name(self, group):
        return 'pgshovel_%s_%s_capture' % (self.name, group)

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
        return 'pgshovel:%s:%s' % (self.name, group)
