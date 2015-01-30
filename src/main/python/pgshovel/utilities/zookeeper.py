import collections
import functools
import logging
import posixpath
import pprint
import threading

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


class StatefulWatch(object):
    """
    Implements a cancellable watch for a znode.

    The watch callback should take two parameters -- the "old" and "new" state,
    which are both either ``State(data, stat)`` or ``None``.

    Valid state transitions include:

    * ``None`` to ``State``
        * a watch was just created on a node that exists, or
        * a watch that was previously registered was triggered on a node that was just created.
    * ``State`` to ``State``
        * a watch that was previously registered was triggered on a state change
    * ``State`` to ``None``
        * a watch that was previously registered had it's underlying node deleted, or
        * a watch that was previously registered was cancelled
    * ``None`` to ``None``
        * a watch that was previously registered on a node that did not exist (or was deleted) was cancelled.
    """
    State = collections.namedtuple('State', 'data stat')

    def __init__(self, zookeeper, path, callback, *args, **kwargs):
        self.path = path
        self.callback = callback

        self.__cancelled = False
        self.__lock = threading.Lock()
        self.__state = None

        DataWatch(zookeeper, path, self.__watch, *args, **kwargs)

    def __repr__(self):
        return '<%s: %r on %r>' % (type(self).__name__, self.callback, self.path)

    def __update_state(self, data, stat):
        # WARNING: This assumes that the lock has been acquired!

        # If the watch has been cancelled, remove it without calling the callback.
        if self.__cancelled:
            logger.debug('%r has been cancelled, removing watch...', self)
            return False

        old = self.__state
        new = self.__state = self.State(data, stat) if (data is not None and stat is not None) else None

        return self.callback(old, new)

    def __watch(self, data, stat):
        with self.__lock:
            return self.__update_state(data, stat)

    def cancel(self):
        # This needs to take out the lock to ensure any watches triggered
        # during execution of this method are also cancelled.
        with self.__lock:
            if not self.__cancelled:
                logger.debug('Cancelling watch on %r...', self.path)
                self.__update_state(None, None)
                self.__cancelled = True


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
