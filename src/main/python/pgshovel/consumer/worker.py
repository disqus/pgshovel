import collections
import functools
import logging
import threading
from cPickle import loads
from Queue import (
    Empty,
    Queue,
)
from concurrent.futures import Future

from cached_property import cached_property
from kazoo.recipe.lock import Lock
from CloseableQueue import CloseableQueue

from pgshovel.utilities.async import (
    Runnable,
    deferred,
    invoke,
)


logger = logging.getLogger(__name__)


def use_defer(method):
    @functools.wraps(method)
    def wrapped(self, *args, **kwargs):
        with deferred() as defer:
            return method(self, defer, *args, **kwargs)
    return wrapped


def check_stop(runnable):
    runnable.stop()
    runnable.result()


States = collections.namedtuple('States', 'old new')
Data = collections.namedtuple('Data', 'table operation states version transaction')
Transaction = collections.namedtuple('Transaction', 'id time')


def parse_version_0(payload):
    table, operation, states, version, transaction = loads(payload)
    return Data(
        table,
        operation,
        States(*states),
        version,
        Transaction(*transaction),
    )


PAYLOAD_PARSERS = {
    0: parse_version_0,
}


class Event(object):
    def __init__(self, id, raw):
        self.id = id
        self.raw = raw

    @cached_property
    def data(self):
        version, payload = self.raw.split(':', 1)
        return PAYLOAD_PARSERS[int(version)](payload)


class Consumer(Runnable):
    """
    Manages consumption for an individual capture group.
    """
    def __init__(self, application, database, consumer_identifier, consumer_group_identifier, group, configuration):
        super(Consumer, self).__init__(name='consumer:%s' % (group,), daemon=True)

        self.application = application
        self.database = database
        self.consumer_identifier = consumer_identifier
        self.consumer_group_identifier = consumer_group_identifier
        self.group = group
        self.configuration = configuration

        self._ownership_lock = Lock(
            self.application.environment.zookeeper,
            self.application.get_ownership_lock_path(self.consumer_group_identifier)(self.group),
            self.consumer_identifier,
            # TODO: Manage state when disconnected from ZooKeeper, etc.
            # If the lock is lost, remove it from the consumers, and wait for
            # the connection to be re-established so that we can register it.
        )

        self.ready = threading.Event()

        # Right now, this only makes sense to have a buffer of one batch
        # because we share the connection and it doesn't make sense to call
        # `next_batch` across multiple transactions. If this was changed to
        # only fill the queue when it is **totally empty** (rather than has
        # open slots), this could be arbitarily large and represent a buffer
        # with a maximum fill.
        self.batches = Queue(1)

        self.__stop_requested = threading.Event()

    def __repr__(self):
        return '<%s: %r (%s)>' % (
            type(self).__name__,
            self.group,
            'running' if self.running() else 'stopped',
        )

    @use_defer
    def run(self, defer):
        logger.debug('Trying to acquire capture group ownership...')
        self._ownership_lock.acquire()  # TODO: Don't block stop request.

        def release():
            logger.debug('Releasing capture group ownership...')
            self._ownership_lock.release()
            logger.debug('Released ownership.')

        defer(release)

        logger.debug('Registering consumer...')
        with self.database() as database, database.cursor() as cursor:
            statement = "SELECT * FROM pgq.register_consumer(%s, %s)"
            cursor.execute(statement, (self.application.get_queue_name(self.group), self.consumer_group_identifier))
            (new,) = cursor.fetchone()
            logger.debug('Registered as queue consumer using %s registration.', 'new' if new else 'existing')
            database.commit()

        logger.debug('Ready to consume events.')
        self.ready.set()

        while True:
            if self.__stop_requested.wait(0.01):
                break

            if not self._ownership_lock.is_acquired:
                raise RuntimeError('Lost ownership of consumer lock!')

            # If the coordinator is ready, prepare another batch for consumption.
            if not self.batches.full():
                with self.database() as connection, connection.cursor() as cursor:
                    statement = "SELECT batch_id FROM pgq.next_batch_info(%s, %s)"
                    cursor.execute(statement, (self.application.get_queue_name(self.group), self.consumer_group_identifier,))
                    (batch_id,) = cursor.fetchone()
                    if batch_id is None:
                        connection.commit()
                        continue  #  There is nothing to consume.

                    statement = "SELECT ev_id, ev_data FROM pgq.get_batch_events(%s)"
                    cursor.execute(statement, (batch_id,))

                    events = map(lambda row: Event(*row), cursor.fetchall())

                    def finish(connection):
                        with connection.cursor() as cursor:
                            cursor.execute("SELECT * FROM pgq.finish_batch(%s)", (batch_id,))
                            (success,) = cursor.fetchone()
                            if not success:
                                raise RuntimeError('Could not close batch!')
                        connection.commit()

                    # This passes along the ``configuration`` value at the
                    # point of retrieval. Since ``configuration`` attribute on
                    # this worker is only replaced with a **new** configuration
                    # value (never mutated in-place) this doesn't require a
                    # lock or a deep copy to ensure it's validity later on.
                    self.batches.put((self.configuration, events, finish))
                    connection.commit()

    def stop_async(self):
        """
        Stop the consumer, releasing ownership of the capture group.
        """
        self.__stop_requested.set()

    def stop(self, timeout=None):
        self.stop_async()
        return self.join(timeout)


class Coordinator(Runnable):
    """
    Handles the coordination of consumers for capture groups that exist on the
    same database.
    """
    Consumer = Consumer

    def __init__(self, application, database, consumer_group_identifier, consumer_identifier, handler):
        super(Coordinator, self).__init__(name='coordinator:%s' % (database,), daemon=True)

        self.application = application
        self.database = database
        self.consumer_group_identifier = consumer_group_identifier
        self.consumer_identifier = consumer_identifier
        self.handler = handler

        self.__queue = CloseableQueue()
        self.__consumers = {}  # <group name> -> Consumer

        self.__stop_requested = threading.Event()

    def __repr__(self):
        return '<%s: %r/%r (%s)>' % (
            type(self).__name__,
            self.consumer_group_identifier,
            self.consumer_identifier,
            'running' if self.running() else 'stopped',
        )

    @use_defer
    def run(self, defer):
        def subscribe(name, configuration):
            consumer = self.__consumers.get(name)
            if consumer:
                consumer.configuration = configuration
            else:
                consumer = self.__consumers[name] = self.Consumer(
                    self.application,
                    self.database,
                    self.consumer_identifier,
                    self.consumer_group_identifier,
                    name,
                    configuration,
                )
                logger.debug('Starting %r...', consumer)
                consumer.start()
                defer(functools.partial(check_stop, consumer))  # TODO: Use non-blocking shutdown
            return consumer

        def unsubscribe(name):
            consumer = self.__consumers.pop(name)
            logger.debug('Stopping %r...', consumer)
            consumer.stop()
            logger.debug('Stopped %r.', consumer)
            return consumer

        operations = {
            'subscribe': subscribe,
            'unsubscribe': unsubscribe,
        }

        stopping = False
        while not stopping:
            if self.__stop_requested.wait(0.01):
                logger.debug('Stop requested, flushing queue and preparing to exit...')
                self.__queue.close()
                stopping = True

            # Perform all pending operations in the control queue.
            # TODO: Refactor the control logic into something that more closely represents a channel.
            while not self.__queue.empty():
                operation, arguments, response = self.__queue.get()
                invoke(functools.partial(operations[operation], *arguments), response)

            with self.database() as connection:
                for consumer in self.__consumers.values():
                    if not consumer.running():
                        consumer.result()
                        raise RuntimeError('Found dead consumer: %r' % (consumer,))

                    try:
                        configuration, events, finish = consumer.batches.get(False)
                        logger.debug('Fetched %s events from %s.', len(events), consumer)

                        self.handler(consumer.group, configuration, events)
                        finish(connection)
                    except Empty:
                        pass  # This consumer doesn't have anything for us.

    def stop_async(self):
        """
        Prepare the coordinator to stop.
        """
        self.__stop_requested.set()

    def stop(self, timeout=None):
        self.stop_async()
        return self.join(timeout)

    def subscribe_async(self, name, configuration):
        """
        Request that a the coordinator subscribes or updates a capture group.
        """
        response = Future()
        self.__queue.put(('subscribe', (name, configuration), response))
        return response

    def subscribe(self, name, configuration, timeout=None):
        return self.subscribe_async(name, configuration).result(timeout)

    def unsubscribe_async(self, name):
        """
        Request that consumer unsubscribes from a capture group.
        """
        response = Future()
        self.__queue.put(('unsubscribe', (name,), response))
        return response

    def unsubscribe(self, name, timeout=None):
        return self.unsubscribe_async(name).result(timeout)
