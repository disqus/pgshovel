from concurrent.futures import Future
from tests.consumer.worker import application
from pgshovel.consumer.rebalancer import Rebalancer


consumer_group_identifier = 'consumer-group'


def test_rebalancer_lifecycle(application):
    zookeeper = application.environment.zookeeper

    zookeeper.ensure_path(application.get_consumer_group_membership_path(consumer_group_identifier)())
    zookeeper.ensure_path(application.get_group_path())

    rebalancer = Rebalancer(application, consumer_group_identifier)
    rebalancer.start()

    def validate(condition, timeout=3):
        future = Future()

        def subscriber(assignments):
            if condition(assignments):
                future.set_result(True)  # report assertion
                return False  # cancel subscription

        assert rebalancer.subscribe(subscriber), 'subscribe should succeed'

        return future.result(timeout)

    assert validate(lambda a: a == {}), 'assignments should be empty'

    zookeeper.create(application.get_consumer_group_membership_path(consumer_group_identifier)('a'))
    assert validate(lambda a: a == {'a': []})

    zookeeper.create(application.get_group_path('1'))
    assert validate(lambda a: a == {'a': ['1']})

    zookeeper.delete(application.get_group_path('1'))
    assert validate(lambda a: a == {'a': []})

    zookeeper.delete(application.get_consumer_group_membership_path(consumer_group_identifier)('a'))
    assert validate(lambda a: a == {}), 'assignments should be empty'

    rebalancer.stop()
    assert rebalancer.result(3) is None, 'rebalancer should exit cleanly'
