import collections
import posixpath

import psycopg2
from kazoo.exceptions import NoNodeError
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED
from psycopg2.extras import NamedTupleCursor

from pgshovel.utilities.exceptions import chained
from pgshovel.utilities.zookeeper import children


class DatabaseConfiguration(collections.MutableMapping):
    __valid_keys = __required_keys = frozenset(('connection', 'roles'))

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


class Database(object):
    def __init__(self, name, configuration):
        self.name = name
        self.configuration = DatabaseConfiguration(configuration)
        self.configuration.validate()

    def __str__(self):
        return self.name

    def connect(self, role='administrator', isolation_level=ISOLATION_LEVEL_READ_COMMITTED):
        options = self.configuration['connection'].copy()
        options.update(self.configuration.get('roles', {}).get(role, {}))
        connection = psycopg2.connect(cursor_factory=NamedTupleCursor, **options)
        connection.set_isolation_level(isolation_level)
        return connection


class DatabaseManager(object):
    def __init__(self, application):
        self.application = application

    @property
    def path(self):
        return posixpath.join(self.application.path, 'databases')

    def all(self):
        try:
            results = children(self.application.environment.zookeeper, self.path)
        except NoNodeError:
            message = 'Could not load databases for %r. (Has the application cluster been initialized in ZooKeeper?)' % (self.application,)
            raise chained(message, RuntimeError)

        for name, future in results:
            # TODO: Handle future race condition.
            data, stat = future.get()
            yield name, Database(name, self.application.codec.loads(data))

    def create(self, database, transaction=None):
        zookeeper = transaction if transaction else self.application.environment.zookeeper
        zookeeper.create(
            posixpath.join(self.path, database.name),
            self.application.codec.dumps(database.configuration)
        )

    def get(self, name, transaction=None):
        path = posixpath.join(self.path, name)
        try:
            data, stat = self.application.environment.zookeeper.get(path)
        except NoNodeError:
            raise chained('Invalid database: %s' % (name,), RuntimeError)
        configuration = self.application.codec.loads(data)
        if transaction:
            transaction.check(path, version=stat.version)
        return Database(name, configuration)
