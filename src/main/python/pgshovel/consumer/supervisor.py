import functools
import logging
import operator
import signal
import threading
from Queue import Queue
from collections import namedtuple

from concurrent.futures import Future
from CloseableQueue import CloseableQueue

from pgshovel.consumer.rebalancer import Rebalancer
from pgshovel.consumer.worker import (
    Coordinator,
    use_defer,
)
from pgshovel.interfaces.groups_pb2 import GroupConfiguration
from pgshovel.utilities.async import (
    Runnable,
    deferred,
)
from pgshovel.utilities.postgresql import ManagedConnection
from pgshovel.utilities.protobuf import BinaryCodec
from pgshovel.utilities.zookeeper import StatefulWatch


logger = logging.getLogger(__name__)


class AssignmentManager(Runnable):
    """
    The assignment manager serves as the glue between a rebalancer and a
    supervisor by receiving assignments, managing the retrieval and update of
    group data associated with the assignment, and notifying the supervisor of
    any changes.
    """
    def __init__(self, application, consumer_identifier):
        super(AssignmentManager, self).__init__(name='assignment-manager', daemon=True)
        self.application = application
        self.consumer_identifier = consumer_identifier

        self.__assignments = CloseableQueue()
        self.__subscription_requests = CloseableQueue()

        self.__stop_requested = threading.Event()

    @use_defer
    def run(self, defer):
        watches = {}

        codec = BinaryCodec(GroupConfiguration)
        subscriptions = set()

        def notify(*args, **kwargs):
            for callback in list(subscriptions):
                if callback(*args, **kwargs) is False:
                    subscriptions.discard(callback)

        def handle_update(name, old, new):
            if new and old is None:
                notify(name, (None, codec.decode(new.data)))
            elif new and old:
                notify(name, (codec.decode(old.data), codec.decode(new.data)))
            elif new is None and old:
                notify(name, (codec.decode(old.data), None))

        stopping = False
        while not stopping:
            if self.__stop_requested.wait(0.01):
                logger.debug('Stop requested, flushing queues and preparing to exit...')
                self.__assignments.close()
                self.__subscription_requests.close()
                stopping = True

            while not self.__subscription_requests.empty():
                # Process all subscription requests.
                callback, response = self.__subscription_requests.get()
                try:
                    subscriptions.add(callback)
                except Exception as exception:
                    response.set_exception(exception)
                    raise
                else:
                    response.set_result(True)  # TODO: Send current states to subscriber.

            # Handle all changes to the assignments.
            # TODO: Roll up multiple assignment changes into a single assignment.
            while not self.__assignments.empty():
                assignments, response = self.__assignments.get()
                try:
                    assignment = set(assignments.get(self.consumer_identifier, []))
                    logger.debug('Recieved updated assignment: %r', assignment)

                    created = assignment - set(watches)
                    for name in created:
                        watch = watches[name] = StatefulWatch(
                            self.application.environment.zookeeper,
                            self.application.get_group_path(name),
                            functools.partial(handle_update, name),
                            # TODO: Handle ZooKeeper disconnect
                        )
                        defer(watch.cancel)

                    deleted = set(watches) - assignment
                    for name in deleted:
                        watches.pop(name).cancel()
                except Exception as exception:
                    response.set_exception(exception)
                    raise
                else:
                    response.set_result(None)

    def update_assignments_async(self, assignments):
        response = Future()
        self.__assignments.put((assignments, response))
        return response

    def update_assignments(self, assignments, timeout=None):
        """
        Notifies the manager of assignment changes in the cluster.

        The ``assignments`` argument should be a mapping, where keys are
        consumer identifiers, and values are a sequence of capture group names.
        """
        return self.update_assignments_async(assignments).result(timeout)

    def register_subscription_async(self, callback):
        response = Future()
        self.__subscription_requests.put((callback, response))
        return response

    def register_subscription(self, callback, timeout=None):
        """
        Registers a callback that will be called when a group is added,
        updated, or removed from the assignment.

        The callback should have a signature of the form:

            def callback(group name, (old configuration, new configuration)):

        Either configuration argument may be ``None`` (to represent creation or
        deletion).
        """
        return self.register_subscription_async(callback).result(timeout)

    def stop_async(self):
        self.__stop_requested.set()

    def stop(self, timeout=None):
        self.stop_async()
        self.join(timeout)


CoordinatorState = namedtuple('CoordinatorState', 'coordinator groups')


class Supervisor(Runnable):
    """
    Monitors consumer assignments and capture group mutations, distributing
    work between worker processes based on the database of the capture group.
    """
    Coordinator = Coordinator

    def __init__(self, application, consumer_group_identifier, consumer_identifier, handler):
        super(Supervisor, self,).__init__(name='supervisor', daemon=True)
        self.application = application
        self.consumer_group_identifier = consumer_group_identifier
        self.consumer_identifier = consumer_identifier
        self.handler = handler

        self.__notifications = CloseableQueue()

        self.__stop_requested = threading.Event()

    def __repr__(self):
        return '<%s: %r/%r>' % (type(self).__name__, self.consumer_group_identifier, self.consumer_identifier)

    @use_defer
    def run(self, defer):
        logger.debug('Registering %r...', self)
        self.application.environment.zookeeper.create(
            self.application.get_consumer_group_membership_path(self.consumer_group_identifier)(self.consumer_identifier),
            ephemeral=True,
        )

        get_dsn = operator.attrgetter('database.connection.dsn')

        coordinators = {}

        def start_coordinator(dsn):
            c = self.Coordinator(
                self.application,
                ManagedConnection(dsn),
                self.consumer_group_identifier,
                self.consumer_identifier,
                self.handler,
            )
            logger.debug('Starting %r...', c)
            c.start()
            defer(c.stop)
            return c

        def get_or_start_coordinator(dsn):
            state = coordinators.get(dsn)
            if state is None:
                state = coordinators[dsn] = CoordinatorState(
                    coordinator=start_coordinator(dsn),
                    groups=set(),
                )
            return state

        def handle_state_change(name, (old, new)):
            if old is None and new:
                dsn = get_dsn(new)
                state = get_or_start_coordinator(dsn)
                state.groups.add(name)
                return (None, state.coordinator.subscribe_async(name, new))
            elif old and new:
                new_dsn, old_dsn = get_dsn(new), get_dsn(old)
                if new_dsn == old_dsn:
                    state = coordinators[old_dsn]
                    state.groups.add(name)
                    future = state.coordinator.subscribe_async(name, new)
                    return (future, future)  # this was technically applied to old *and* new
                else:
                    old_state = coordinators[old_dsn]
                    old_state.groups.remove(name)
                    new_state = get_or_start_coordinator(new_dsn)
                    new_state.groups.add(name)
                    return (
                        old_state.coordinator.unsubscribe_async(name),
                        new_state.coordinator.subscribe_async(name, new),
                    )
            elif old and new is None:
                dsn = get_dsn(old)
                state = coordinators[dsn]
                state.groups.remove(name)
                return (state.coordinator.unsubscribe_async(name), None)
            else:
                return (None, None)

        stopping = False
        while not stopping:
            if self.__stop_requested.wait(0.01):
                logger.debug('Stop requested, flushing queue and preparing to exit...')
                self.__notifications.close()
                stopping = True

            # XXX: Coordinator failures -- like the failure of a single
            # database -- right now cause the entire consumer to crash out,
            # which can potentially degrade the entire cluster since rebalances
            # will occur frequently as consumers start and die (due to the bad
            # database connection.) To protect from this, the supervisor should
            # handle dead consumers more gracefully, trying to restart them
            # when possible, avoiding the necessity to trigger rebalances.
            for state in coordinators.values():
                if not state.coordinator.running():
                    state.coordinator.result()
                    raise RuntimeError('Found unexpectedly dead %r!' % (state.coordinator,))

            while not self.__notifications.empty():
                name, states, response = self.__notifications.get()

                # Any blocking operation here will block the run loop, so
                # subscribe/unsubscribe actions need to be performed
                # asynchronously to deal with workers that may be dead or
                # otherwise blocked.
                # TODO: Probably switch this over to use `invoke`.
                try:
                    response.set_result(handle_state_change(name, states))
                except Exception as exception:
                    response.set_exception(exception)
                    raise

            for key, state in coordinators.items():
                if not state.groups:
                    logger.debug('Stopping worker without groups: %r', state.coordinator)
                    coordinators.pop(key)
                    state.coordinator.stop()

    def notify_group_state_change_async(self, name, states):
        response = Future()
        self.__notifications.put((name, states, response))
        return response

    def notify_group_state_change(self, name, states, timeout=None):
        """
        Notifies the supervisor of a group state change.

        The ``states`` argument should be a two-tuple, containing the old and
        new configurations (or None, if the group was just added or removed).
        """
        return self.notify_group_state_change_async(name, states).result(timeout)

    def stop_async(self):
        self.__stop_requested.set()

    def stop(self, timeout=None):
        self.stop_async()
        self.join(timeout)


def run(application, consumer_group_identifier, consumer_identifier, handler):
    stop_requested = threading.Event()

    def __request_exit(signal, frame):
        logger.debug('Caught signal %s, requesting exit...', signal)
        stop_requested.set()

    # Swap out the default handler for SIGINT (which instead would throw a
    # KeyboardInterrupt exception) and SIGTERM (which causes the process to
    # exit) with our handler that stops the worker gracefully.
    signal.signal(signal.SIGINT, __request_exit)
    signal.signal(signal.SIGTERM, __request_exit)

    path = application.get_consumer_group_membership_path(consumer_group_identifier)
    application.environment.zookeeper.ensure_path(path())

    with deferred() as defer:
        rebalancer = Rebalancer(application, consumer_group_identifier)
        assigner = AssignmentManager(application, consumer_identifier)
        supervisor = Supervisor(
            application,
            consumer_group_identifier,
            consumer_identifier,
            handler,
        )

        # Send assignments to the assignment manager.
        rebalancer.subscribe_async(assigner.update_assignments_async)

        # Send group updates to the supervisor.
        assigner.register_subscription_async(supervisor.notify_group_state_change_async)

        supervisor.start()
        defer(supervisor.stop)

        assigner.start()
        defer(assigner.stop)

        rebalancer.start()
        defer(rebalancer.stop)

        while True:
            # A clean stop was requested, so inform the supervisor and then
            # wait for it to exit.
            if stop_requested.wait(0.01):
                break

            for component in (rebalancer, assigner, supervisor):
                # Check to make sure the component thread is still running -- if
                # no stop was requested, it should still be alive. This block
                # should **always** raise an exception if entered -- we never
                # expect the component to be dead at this point.
                if not component.running():
                    component.result()

                    # If the component is dead but retrieving it's result doesn't
                    # raise an exception, we need to raise our own exception here.
                    # (The only way the component should have a clean exit is if
                    # it was requested to stop, and if we're at this point of the
                    # run loop, that shouldn't be possible.)
                    raise RuntimeError('Found unexpectedly dead %r!' % (component,))
