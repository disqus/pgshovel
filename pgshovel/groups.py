import collections
import posixpath

from kazoo.exceptions import NoNodeError

from pgshovel.utilities.zookeeper import children
from pgshovel.utilities.exceptions import chained


class GroupConfiguration(collections.MutableMapping):
    __valid_keys = __required_keys = frozenset(('database', 'tables'))

    def __init__(self, *args, **kwargs):
        self.__configuration = {}

        for arg in args:
            self.update(arg)

        if kwargs:
            self.update(kwargs)

    def __validate_key(self, key):
        if key not in self.__valid_keys:
            raise ValueError('Invalid key: %r' % (key,))

    def __getitem__(self, key):
        self.__validate_key(key)
        return self.__configuration[key]

    def __setitem__(self, key, value):
        self.__validate_key(key)
        self.__configuration[key] = value

    def __delitem__(self, key):
        self.__validate_key(key)
        del self.__configuration[key]

    def __iter__(self):
        return iter(self.__configuration)

    def __len__(self):
        return len(self.__configuration)

    def validate(self):
        missing = self.__required_keys - set(self)
        if missing:
            raise ValueError('Missing required keys: %r' % (missing,))


class Group(object):
    def __init__(self, name, configuration, stat=None):
        self.name = name
        self.configuration = GroupConfiguration(configuration)
        self.configuration.validate()
        self.stat = stat

    def __str__(self):
        return self.name


class GroupManager(object):
    def __init__(self, application):
        self.application = application

    @property
    def path(self):
        return posixpath.join(self.application.path, 'groups')

    def all(self):
        try:
            results = children(self.application.environment.zookeeper, self.path)
        except NoNodeError:
            message = 'Could not load groups for %r. (Has the application cluster been initialized in ZooKeeper?)' % (self.application,)
            raise chained(message, RuntimeError)

        for name, future in results:
            # TODO: Handle future race condition.
            data, stat = future.get()
            yield name, Group(name, self.application.codec.loads(data), stat)

    def create(self, group, transaction=None):
        zookeeper = transaction if transaction else self.application.environment.zookeeper
        zookeeper.create(
            posixpath.join(self.path, group.name),
            self.application.codec.dumps(group.configuration)
        )

    def get(self, name, transaction=None):
        path = posixpath.join(self.path, name)
        try:
            data, stat = self.application.environment.zookeeper.get(path)
        except NoNodeError:
            raise chained('Invalid group: %s' % (name,), RuntimeError)
        configuration = self.application.codec.loads(data)
        if transaction:
            transaction.check(path, version=stat.version)
        return Group(name, configuration, stat)

    def update(self, group, transaction=None):
        # TODO: Wrap transactions in something API compatible.
        method = transaction.set_data if transaction else self.application.environment.zookeeper.set
        # TODO: Figure out a way to handle updating the ZnodeStat when executed in a transaction.
        method(
            posixpath.join(self.path, group.name),
            self.application.codec.dumps(group.configuration),
            version=group.stat.version, # TODO: Maybe allow dirty writes?
        )

    def delete(self, group, transaction=None):
        zookeeper = transaction if transaction else self.application.environment.zookeeper
        zookeeper.delete(
            posixpath.join(self.path, group.name),
            version=group.stat.version,  # TODO: Allow dirty deletes?
        )
