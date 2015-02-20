import tempfile
import threading
import time

import pytest

from pgshovel.utilities.async import (
    Runnable,
    deferred,
    spawn,
)


class Explosion(Exception):
    pass


def test_spawn():
    value = object()
    event = threading.Event()

    def run():
        event.wait()
        return value

    future = spawn(run)
    time.sleep(0.01)  # try to ensure context switch occurs
    assert future.running(), 'future should be started'

    event.set()
    assert future.result(0.01) is value, 'result value should be value singleton'


def test_spawn_failure():
    def run():
        raise Explosion("I'm dead")

    future = spawn(run)
    with pytest.raises(Explosion):
        future.result(0.1)


def test_runnable():
    value = object()
    event = threading.Event()

    class Widget(Runnable):
        def run(self):
            event.wait()
            return value

    runnable = Widget()
    assert not runnable.running(), 'should not be running until started'

    runnable.start()
    time.sleep(0.01)  # try to ensure context switch
    assert runnable.running(), 'should be started'

    event.set()
    runnable.join()
    assert runnable.result(0) is value, 'result value should be value singleton'

    with pytest.raises(RuntimeError):
        runnable.start()


def test_runnable_failure():
    class Widget(Runnable):
        def run(self):
            raise Explosion("I'm dead")

    runnable = Widget()
    assert not runnable.running(), 'should not be running until started'

    runnable.start()
    with pytest.raises(Explosion):
        runnable.result(1)


def test_deferred():
    f = tempfile.TemporaryFile()

    with pytest.raises(Explosion):
        with deferred() as defer:
            assert not f.closed, 'file should be open'
            defer(f.close)
            raise Explosion("I'm dead")

    assert f.closed, 'file should be closed'
