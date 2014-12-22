import functools
import posixpath
import pprint


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
