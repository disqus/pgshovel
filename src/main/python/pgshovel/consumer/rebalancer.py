import functools
import threading
from concurrent.futures import Future
from Queue import Queue

from kazoo.recipe.watchers import ChildrenWatch

from pgshovel.utilities.async import Runnable
from pgshovel.utilities.partitioning import distribute


class Rebalancer(Runnable):
    """
    Observes the cluster state (consumers and capture groups), notifying
    subscribers when assignments have changed.
    """
    def __init__(self, application, consumer_group_identifier):
        super(Rebalancer, self).__init__(name='rebalancer', daemon=True)
        self.application = application
        self.consumer_group_identifier = consumer_group_identifier

        self.__subscription_requests = Queue()

        self.__stop_requested = threading.Event()

    def __repr__(self):
        return '<%s (%s)>' % (
            type(self).__name__,
            'running' if self.running() else 'stopped',
        )

    def run(self):
        def cancel_on_stop(function):
            @functools.wraps(function)
            def watch(*args, **kwargs):
                if self.__stop_requested.is_set():
                    return False
                return function(*args, **kwargs)
            return watch

        consumer_updates = Queue()
        consumers = None
        ChildrenWatch(
            self.application.environment.zookeeper,
            self.application.get_consumer_group_membership_path(self.consumer_group_identifier)(),
            cancel_on_stop(consumer_updates.put),
            # TODO: Handle disconnection from ZooKeeper.
        )

        group_updates = Queue()
        groups = None
        ChildrenWatch(
            self.application.environment.zookeeper,
            self.application.get_group_path(),
            cancel_on_stop(group_updates.put),
            # TODO: Handle disconnection from ZooKeeper.
        )

        assignments = {}
        subscriptions = set()

        while True:
            if self.__stop_requested.wait(0.01):
                break

            while not self.__subscription_requests.empty():
                # Process all subscription requests.
                callback, response = self.__subscription_requests.get()
                callback(assignments)  # TODO: Maybe try/except this for reliability.
                subscriptions.add(callback)
                response.set_result(True)

            if not consumer_updates.empty() or not group_updates.empty():
                while not consumer_updates.empty():
                    consumers = consumer_updates.get()

                while not group_updates.empty():
                    groups = group_updates.get()

                # TODO: This could possibly **become** none if the nodes are
                # deleted (the cluster was deleted), in that case it probably
                # best to shut down.
                if groups is not None and consumers is not None:
                    assignments = distribute(consumers, groups)

                # Notify all subscribers of the updated assignments.
                for callback in list(subscriptions):
                    if callback(assignments) is False:
                        subscriptions.discard(callback)

    def stop(self):
        self.__stop_requested.set()

    def subscribe_async(self, callback):
        response = Future()
        self.__subscription_requests.put((callback, response))
        return response

    def subscribe(self, callback):
        return self.subscribe_async(callback).result()
