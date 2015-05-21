import itertools
import logging
import pickle
import threading
import time

import psycopg2
from concurrent.futures import (
    Future,
    wait,
)
from kazoo.client import KazooState
from kazoo.recipe.watchers import DataWatch

from pgshovel import __version__
from pgshovel.database import ManagedDatabase
from pgshovel.events import (
    MutationBatch,
    MutationEvent,
)
from pgshovel.interfaces.configurations_pb2 import (
    ClusterConfiguration,
    ReplicationSetConfiguration,
)
from pgshovel.utilities.protobuf import BinaryCodec


logger = logging.getLogger(__name__)


def deserialize_event((id, payload, timestamp, txid)):
    version, payload = payload.split(':', 1)
    if version != '0':
        raise RuntimeError('Cannot parse payload version: %s', version)
    (schema, table), operation, states, configuration_version = pickle.loads(payload)
    return MutationEvent(id, schema, table, operation, states, txid, timestamp)


BATCH_INFO_STATEMENT = """
SELECT
    txid_snapshot_xmax(last.tick_snapshot) as tx_start,
    txid_snapshot_xmax(cur.tick_snapshot) as tx_end
FROM
    pgq.get_batch_info(%s) b,
    pgq.tick last,
    pgq.tick cur
WHERE
    last.tick_id = b.prev_tick_id AND cur.tick_id = b.tick_id
"""


class Worker(threading.Thread):
    def __init__(self, cluster, dsn, set, consumer, handler):
        super(Worker, self).__init__(name=dsn)
        self.daemon = True

        self.cluster = cluster
        self.database = ManagedDatabase(cluster, dsn)
        self.set = set
        self.consumer = consumer
        self.handler = handler

        self.__stop_requested = threading.Event()

        self.__result = Future()
        self.__result.set_running_or_notify_cancel()  # cannot be cancelled

    def run(self):
        try:
            logger.debug('Started worker.')

            # TODO: this connection needs to timeout in case the lock cannot be
            # grabbed or the connection cannot be established to avoid never
            # exiting
            logger.info('Registering as queue consumer...')
            with self.database.connection() as connection, connection.cursor() as cursor:
                statement = "SELECT * FROM pgq.register_consumer(%s, %s)"
                cursor.execute(statement, (self.cluster.get_queue_name(self.set), self.consumer))
                (new,) = cursor.fetchone()
                logger.info('Registered as queue consumer using %s registration.', 'new' if new else 'existing')
                connection.commit()

            logger.info('Ready to relay events.')
            while True:
                if self.__stop_requested.wait(0.01):
                    break

                # TODO: this needs a timeout as well
                # TODO: this probably should have a lock on consumption
                with self.database.connection() as connection, connection.cursor() as cursor:
                    statement = "SELECT batch_id FROM pgq.next_batch_info(%s, %s)"
                    cursor.execute(statement, (self.cluster.get_queue_name(self.set), self.consumer,))
                    (batch_id,) = cursor.fetchone()
                    if batch_id is None:
                        # TODO: Maybe sleep here to avoid increasing the
                        # transaction ID? (Does this even increase the
                        # transaction ID???)
                        connection.commit()
                        continue  #  There is nothing to consume.

                    with connection.cursor() as c:
                        cursor.execute(BATCH_INFO_STATEMENT, (batch_id,))
                        start_txid, end_txid = cursor.fetchone()
                        batch = MutationBatch(batch_id, start_txid, end_txid, self.database.id)

                    statement = "SELECT ev_id, ev_data, extract(epoch from ev_time), ev_txid FROM pgq.get_batch_events(%s)"
                    cursor.execute(statement, (batch_id,))

                    # TODO: Maybe check to make sure this connection ID == node ID?
                    batch.events.extend(map(deserialize_event, cursor.fetchall()))
                    logger.debug('Retrieved batch %s.', batch)
                    self.handler.push(batch)

                    cursor.execute("SELECT * FROM pgq.finish_batch(%s)", (batch.id,))
                    (success,) = cursor.fetchone()
                    if not success:
                        raise RuntimeError('Could not close batch!')

                    connection.commit()
                    logger.debug('Successfully relayed batch %s.', batch)

        except Exception as error:
            logger.exception('Caught exception in worker: %s', error)
            self.__result.set_exception(error)
        else:
            logger.debug('Stopped.')
            self.__result.set_result(None)

    def result(self, timeout=None):
        return self.__result.result(timeout)

    def stop_async(self):
        logger.debug('Requesting stop...')
        self.__stop_requested.set()
        return self.__result


class StreamWriter(object):
    def __init__(self, stream, serializer=str):
        self.stream = stream
        self.serializer = serializer
        self.__lock = threading.Lock()

    def push(self, batch):
        with self.__lock:
            for serialized in itertools.imap(self.serializer, batch.events):
                self.stream.write(serialized)
                self.stream.write('\n')
            self.stream.flush()


RECOVERABLE_ERRORS = (psycopg2.OperationalError,)


class Relay(threading.Thread):
    def __init__(self, cluster, set, consumer, handler, throttle=10):
        super(Relay, self).__init__(name='relay')
        self.daemon = True

        self.cluster = cluster
        self.set = set
        self.consumer = consumer
        self.handler = handler
        self.throttle = throttle

        self.__stop_requested = threading.Event()

        self.__result = Future()
        self.__result.set_running_or_notify_cancel()  # cannot be cancelled

        self.__worker_lock = threading.Lock()
        self.__workers = {}

    def run(self):
        try:
            logger.debug('Started relay.')

            def __handle_session_state_change(state):
                if state == KazooState.SUSPENDED:
                    # TODO: This should exit cleanly but then raise, maybe?
                    # TODO: Ideally this would pause all processing, and
                    # continue if we recover (rather than lose the session.)
                    logger.warning('Lost connection to ZooKeeper! Requesting exit...')
                    self.__stop_requested.set()

            self.cluster.zookeeper.add_listener(__handle_session_state_change)

            # XXX: This needs to be implemented, but right now there is a race
            # condition that can cause the relay to never exit if the watch is
            # established against a dead/unresponsive ZooKeeper ensemble.

            # def __handle_cluster_version_change(data, stat):
            #     if not data:
            #         logger.warning('Received no cluster configuration data! Requesting exit...')
            #         self.__stop_requested.set()
            #         return False

            #     configuration = BinaryCodec(ClusterConfiguration).decode(data)
            #     if __version__ != configuration.version:
            #         logger.warning('Cluster and local versions do not match (cluster: %s, local: %s)! Requesting exit...', cluster, __version__)
            #         self.__stop_requested.set()
            #         return False

            # logger.debug('Checking cluster version...')
            # DataWatch(
            #     self.cluster.zookeeper,
            #     self.cluster.path,
            #     __handle_cluster_version_change,
            # )

            stopping = []

            def start_worker(dsn):
                worker = Worker(self.cluster, dsn, self.set, self.consumer, self.handler)
                worker.start()
                self.__workers[dsn] = (time.time(), worker)

            def __handle_state_change(data, stat):
                if self.__stop_requested.is_set():
                    return False  # we're exiting anyway, don't do anything

                if data is None:
                    # TODO: it would probably make sense for this to have an exit code
                    logger.warning('Received no replication set configuration data! Requesting exit...')
                    self.__stop_requested.set()
                    return False

                logger.debug('Recieved an update to replication set configuration.')
                configuration = BinaryCodec(ReplicationSetConfiguration).decode(data)
                with self.__worker_lock:
                    current_workers = set(self.__workers.keys())
                    updated_workers = set(d.dsn for d in configuration.databases)

                    for dsn in updated_workers - current_workers:
                        start_worker(dsn)

                    for dsn in current_workers - updated_workers:
                        _, worker = self.__workers.pop(dsn)
                        stopping.append([worker.stop_async(), time.time()])

            logger.debug('Fetching replication set configuration...')
            DataWatch(
                self.cluster.zookeeper,
                self.cluster.get_set_path(self.set),
                __handle_state_change,
            )

            while True:
                if self.__stop_requested.wait(0.01):
                    break

                # TODO: check up on stopping workers

                with self.__worker_lock:
                    for dsn, (started, worker) in self.__workers.items():
                        if not worker.is_alive():
                            try:
                                worker.result(0)
                            except RECOVERABLE_ERRORS as error:
                                pass  # do nothing, we'll eventually restart it
                            else:
                                # otherwise, exit immediately
                                raise RuntimeError('Found unexpected dead worker: %r' % (worker,))

                            if time.time() > (started + self.throttle):
                                logger.info('Trying to restart %r, previously exited with recoverable error: %s', worker, error)
                                start_worker(dsn)

            with self.__worker_lock:
                if self.__workers:
                    logger.debug('Stopping %s active worker(s)...', len(self.__workers))
                    futures = []
                    while self.__workers:
                        dsn, (_, worker) = self.__workers.popitem()
                        futures.append(worker.stop_async())

                    timeout = 10
                    logger.debug('Waiting up to %d seconds for all workers to finish...', timeout)
                    exited, running = wait(futures, timeout=timeout)
                    if running:
                        logger.warning('Exiting with %s workers still running!', len(running))
                    else:
                        logger.info('All workers exited cleanly.')

        except Exception as error:
            logger.exception('Caught exception in relay: %s', error)
            self.__result.set_exception(error)
        else:
            logger.debug('Stopped.')
            self.__result.set_result(None)

    def result(self, timeout=None):
        return self.__result.result(timeout)

    def workers(self):
        with self.__worker_lock:
            return self.__workers.copy()

    def stop_async(self):
        logger.debug('Requesting stop...')
        self.__stop_requested.set()
        return self.__result
