import functools
import itertools
import logging
import pickle
import threading
import time
import uuid
from collections import namedtuple
from datetime import timedelta

import click
import psycopg2
from concurrent.futures import (
    Future,
    TimeoutError,
)
from kazoo.client import KazooState
from kazoo.recipe.watchers import DataWatch

from pgshovel import __version__
from pgshovel.database import ManagedDatabase
from pgshovel.interfaces.common_pb2 import (
    BatchIdentifier,
    Snapshot,
    Tick,
    Timestamp,
)
from pgshovel.interfaces.configurations_pb2 import (
    ClusterConfiguration,
    ReplicationSetConfiguration,
)
from pgshovel.interfaces.streams_pb2 import (
    BeginOperation,
    MutationOperation,
)
from pgshovel.streams.publisher import Publisher
from pgshovel.streams.utilities import FormattedBatchIdentifier
from pgshovel.utilities.conversions import (
    row_converter,
    to_snapshot,
    to_timestamp,
)
from pgshovel.utilities.protobuf import BinaryCodec


logger = logging.getLogger(__name__)


BATCH_INFO_STATEMENT = """
SELECT
    start_tick.tick_id,
    start_tick.tick_snapshot,
    extract(epoch from start_tick.tick_time),
    end_tick.tick_id,
    end_tick.tick_snapshot,
    extract(epoch from end_tick.tick_time)
FROM
    pgq.get_batch_info(%s) batch,
    pgq.tick start_tick,
    pgq.tick end_tick
WHERE
    start_tick.tick_id = batch.prev_tick_id AND end_tick.tick_id = batch.tick_id
"""


def to_mutation(row):
    id, payload, timestamp, transaction = row

    version, payload = payload.split(':', 1)
    if version != '0':
        raise RuntimeError('Cannot parse payload version: %s', version)

    (schema, table), operation, primary_key_columns, (old, new), configuration_version = pickle.loads(payload)

    states = {}
    if old:
        states['old'] = row_converter.to_protobuf(old)

    if new:
        states['new'] = row_converter.to_protobuf(new)

    assert states, 'at least one state must be set'

    return MutationOperation(
        id=id,
        schema=schema,
        table=table,
        operation=getattr(MutationOperation, operation),
        identity_columns=primary_key_columns,
        timestamp=to_timestamp(timestamp),
        transaction=transaction,
        **states
    )


class Worker(threading.Thread):
    def __init__(self, cluster, dsn, set, consumer, stream):
        super(Worker, self).__init__(name=dsn)
        self.daemon = True

        self.cluster = cluster
        self.database = ManagedDatabase(cluster, dsn)
        self.set = set
        self.consumer = consumer
        self.stream = stream

        self.__stop_requested = threading.Event()

        self.__result = Future()
        self.__result.set_running_or_notify_cancel()  # cannot be cancelled

    def run(self):
        publisher = Publisher(self.stream.push)

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
                logger.info('Registered as queue consumer: %s (%s registration).', self.consumer, 'new' if new else 'existing')
                connection.commit()

            logger.info('Ready to relay events.')
            while True:
                if self.__stop_requested.wait(0.01):
                    break

                # TODO: this needs a timeout as well
                # TODO: this probably should have a lock on consumption
                with self.database.connection() as connection:
                    # Check to see if there is a batch available to be relayed.
                    statement = "SELECT batch_id FROM pgq.next_batch_info(%s, %s)"
                    with connection.cursor() as cursor:
                        cursor.execute(statement, (self.cluster.get_queue_name(self.set), self.consumer,))
                        (batch_id,) = cursor.fetchone()
                        if batch_id is None:
                            connection.commit()
                            continue  #  There is nothing to consume.

                    # Fetch the details of the batch.
                    with connection.cursor() as cursor:
                        cursor.execute(BATCH_INFO_STATEMENT, (batch_id,))
                        start_id, start_snapshot, start_timestamp, end_id, end_snapshot, end_timestamp = cursor.fetchone()

                    batch = BatchIdentifier(
                        id=batch_id,
                        node=self.database.id.bytes,
                    )

                    begin = BeginOperation(
                        start=Tick(
                            id=start_id,
                            snapshot=to_snapshot(start_snapshot),
                            timestamp=to_timestamp(start_timestamp),
                        ),
                        end=Tick(
                            id=end_id,
                            snapshot=to_snapshot(end_snapshot),
                            timestamp=to_timestamp(end_timestamp),
                        ),
                    )

                    with publisher.batch(batch, begin) as publish:
                        # Fetch the events for the batch. This uses a named cursor
                        # to avoid having to load the entire event block into
                        # memory at once.
                        with connection.cursor('events') as cursor:
                            statement = "SELECT ev_id, ev_data, extract(epoch from ev_time), ev_txid FROM pgq.get_batch_events(%s)"
                            cursor.execute(statement, (batch_id,))

                            # TODO: Publish these in chunks, the full ack + RTT is a performance killer
                            for mutation in itertools.imap(to_mutation, cursor):
                                publish(mutation)

                        with connection.cursor() as cursor:
                            cursor.execute("SELECT * FROM pgq.finish_batch(%s)", (batch_id,))
                            (success,) = cursor.fetchone()

                        # XXX: Not sure why this could happen?
                        if not success:
                            raise RuntimeError('Could not close batch!')

                    # XXX: Since this is outside of the batch block, this
                    # downstream consumers need to be able to handle receiving
                    # the same transaction multiple times, probably by checking
                    # a metadata table before starting to apply a batch.
                    connection.commit()

                    logger.debug('Successfully relayed batch: %s.', FormattedBatchIdentifier(batch))

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


RECOVERABLE_ERRORS = (psycopg2.OperationalError,)


WorkerState = namedtuple('WorkerState', 'worker time')


class Relay(threading.Thread):
    def __init__(self, cluster, set, consumer, stream, throttle=10):
        super(Relay, self).__init__(name='relay')
        self.daemon = True

        self.cluster = cluster
        self.set = set
        self.consumer = consumer
        self.stream = stream
        self.throttle = throttle

        self.__stop_requested = threading.Event()

        self.__result = Future()
        self.__result.set_running_or_notify_cancel()  # cannot be cancelled

        self.__worker_state_lock = threading.Lock()
        self.__worker_state = None

    def run(self):
        try:
            logger.debug('Started relay (cluster: %s, set: %s) using %s.', self.cluster, self.set, self.stream)

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

            # XXX just store the config
            def start_worker(dsn):
                worker = Worker(self.cluster, dsn, self.set, self.consumer, self.stream)
                worker.start()
                return WorkerState(worker, time.time())

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

                with self.__worker_state_lock:
                    # TODO: this is annoying and repetative and should be cleaned up
                    if self.__worker_state is None:
                        self.__worker_state = start_worker(configuration.database.dsn)
                    elif self.__worker_state.worker.database.dsn != configuration.database.dsn:
                        self.__worker_state.worker.stop_async()
                        stopping.append(WorkerState(self.__worker_state.worker, time.time()))
                        self.__worker_state = start_worker(configuration.database.dsn)

            logger.debug('Fetching replication set configuration...')
            DataWatch(
                self.cluster.zookeeper,
                self.cluster.get_set_path(self.set),
                __handle_state_change,
            )

            while True:
                if self.__stop_requested.wait(0.01):
                    break

                # TODO: check up on stopping workers (ideally there are none)

                with self.__worker_state_lock:
                    if self.__worker_state is not None and not self.__worker_state.worker.is_alive():
                        try:
                            self.__worker_state.worker.result(0)
                        except RECOVERABLE_ERRORS as error:
                            if time.time() > (self.__worker_state.time + self.throttle):
                                logger.info('Trying to restart %r, previously exited with recoverable error: %s', self.__worker_state.worker, error)
                                # TODO: hack, make a restart method
                                self.__worker_state = start_worker(self.__worker_state.worker.database.dsn)
                        else:
                            # otherwise, exit immediately
                            raise RuntimeError('Found unexpected dead worker: %r' % (self.__worker_state.worker,))

            with self.__worker_state_lock:
                if self.__worker_state is not None:
                    logger.debug('Stopping worker...')

                    future = self.__worker_state.worker.stop_async()

                    timeout = 10
                    logger.debug('Waiting up to %d seconds for worker to finish...', timeout)
                    try:
                        future.result(timeout)
                    except TimeoutError:
                        logger.warning('Exiting with worker still running!')
                    else:
                        logger.info('Worker exited cleanly.')

        except Exception as error:
            logger.exception('Caught exception in relay: %s', error)
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
