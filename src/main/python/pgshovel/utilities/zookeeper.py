import collections
import functools
import logging
import posixpath
import pprint

from kazoo.recipe.watchers import (
    ChildrenWatch,
    DataWatch,
)


logger = logging.getLogger(__name__)


class TransactionFailed(Exception):
    def __init__(self, results):
        super(TransactionFailed, self).__init__(pprint.pformat(results))
        self.results = results


def commit(transaction):
    results = zip(transaction.operations, transaction.commit())
    if any(isinstance(result[1], Exception) for result in results):
        raise TransactionFailed(results)
    return results


def children(zookeeper, path):
    children = zookeeper.get_children(path)
    return zip(children, map(
        zookeeper.get_async,
        map(
            functools.partial(posixpath.join, path),
            children,
        )
    ))


class SynchronizedMapping(collections.Mapping):
    class State:
        STOPPED = 0
        STARTING = 1
        ACTIVE = 2

    def __init__(self, zookeeper, path, transformer):
        self.__zookeeper = zookeeper
        self.__path = path
        self.__transformer = transformer
        self.__nodes = {}

        self.__state = self.State.STOPPED

    def __update_node(self, name, data, stat):
        logger.debug('Recieved node update for %s/%s; stat: %r', self.__path, name, stat)

        if not data and not stat:
            return False  # The node will be removed with a child watch.

        self.__nodes[name] = self.__transformer(name, data, stat)

    def __update_children(self, children):
        self.__state = self.State.ACTIVE

        current = set(self.__nodes)
        updated = set(children)

        created = updated - current
        deleted = current - updated
        logger.debug(
            'Recieved child updates for %s; created: %r, deleted: %r',
            self.__path,
            created,
            deleted,
        )

        for name in created:
            DataWatch(
                self.__zookeeper,
                posixpath.join(self.__path, name),
                functools.partial(self.__update_node, name),
            )

        for name in deleted:
            del self.__nodes[name]

    def __getitem__(self, name):
        if self.__state is not self.State.ACTIVE:
            raise RuntimeError  # TODO: Make this a better error.

        return self.__nodes[name]

    def __iter__(self):
        if self.__state is not self.State.ACTIVE:
            raise RuntimeError  # TODO: Make this a better error.

        return iter(self.__nodes)

    def __len__(self):
        if self.__state is not self.State.ACTIVE:
            raise RuntimeError  # TODO: Make this a better error.

        return len(self.__nodes)

    def start(self):
        if self.__state in (self.State.STARTING, self.State.ACTIVE):
            return

        ChildrenWatch(
            self.__zookeeper,
            self.__path,
            self.__update_children,
        )

    def stop(self):
        raise NotImplementedError  # TODO
