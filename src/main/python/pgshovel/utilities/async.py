import logging
from contextlib import contextmanager
from threading import (
    Lock,
    Thread,
)

from concurrent.futures import Future


logger = logging.getLogger(__name__)


@contextmanager
def deferred():
    callbacks = []
    try:
        yield callbacks.append
    finally:
        for callback in reversed(callbacks):  # LIFO
            try:
                callback()
            except Exception as exception:
                logger.exception('Caught exception during deferred callback execution of %r: %s', callback, exception)


def invoke(function, future):
    """
    Invokes the provided callable, binding the result to the provided future.
    """
    try:
        result = function()
    except Exception as exception:
        logger.exception('Caught exception when invoking %r for future %r: %s', function, future, exception)
        future.set_exception(exception)
        raise

    future.set_result(result)

    return result


def spawn(function, daemon=None, name=None):
    """
    Runs a function in a new thread, returning a future that contains either
    the result of the function's successful execution, or an exception if it
    errored.
    """
    future = Future()

    def run():
        cancelled = not future.set_running_or_notify_cancel()
        if cancelled:
            return

        return invoke(function, future)

    thread = Thread(target=run)

    if daemon is not None:
        thread.daemon = daemon

    if name is not None:
        thread.name = name

    thread.start()

    return future


class Runnable(object):
    def __init__(self, *args, **kwargs):
        self.__future = None
        self.__state_transition_lock = Lock()

        self.__args = args
        self.__kwargs = kwargs

    def run(self):
        raise NotImplementedError

    def start(self):
        with self.__state_transition_lock:
            if self.started():
                raise RuntimeError('%s can only be run once.' % (self,))

            self.__future = spawn(self.run)

    def started(self):
        return self.__future is not None

    def running(self):
        return self.started() and self.__future.running()

    def join(self, timeout=None):
        if not self.started():
            raise RuntimeError('Cannot join %s that has not yet started.' % (self,))

        try:
            self.__future.result(timeout)
        except Exception as error:
            logger.debug('Caught exception while joining %r: %s', self, error, exc_info=True)

    def result(self, timeout=None):
        if not self.started():
            raise RuntimeError('Cannot retrieve the result of %s that has not yet started.' % (self,))

        return self.__future.result(timeout)
