import threading
import uuid

from kazoo.client import KazooClient

from pgshovel.utilities.zookeeper import StatefulWatch


def test_stateful_watch():
    zookeeper = KazooClient()
    zookeeper.start()

    def callback(old, new):
        callback.state = (old, new)
        callback.event.set()

    callback.event = threading.Event()
    callback.state = None

    path = '/%s' % (uuid.uuid1().hex,)

    watch = StatefulWatch(zookeeper, path, callback)
    callback.event.wait()
    assert callback.state[0] is None
    assert callback.state[1] is None
    callback.event.clear()

    zookeeper.create(path, ephemeral=True)
    callback.event.wait()
    assert callback.state[0] is None
    assert callback.state[1] is not None
    callback.event.clear()

    zookeeper.set(path, 'foo')
    callback.event.wait()
    assert callback.state[0] is not None
    assert callback.state[1].data == 'foo'
    callback.event.clear()

    watch.cancel()
    callback.event.wait()
    assert callback.state[0].data == 'foo'
    assert callback.state[1] is None
    callback.event.clear()

    zookeeper.set(path, 'bar')
    assert not callback.event.is_set()

    zookeeper.delete(path)
    assert not callback.event.is_set()

    zookeeper.stop()
