from __future__ import absolute_import

import collections
import functools
import logging
import posixpath
import pprint
import threading

from kazoo.recipe.watchers import DataWatch


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
