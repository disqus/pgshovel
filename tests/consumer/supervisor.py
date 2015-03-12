from Queue import Queue

import pytest
from kazoo.exceptions import NodeExistsError

from pgshovel.administration import move_groups
from pgshovel.consumer.supervisor import (
    AssignmentManager,
    Supervisor,
)
from pgshovel.consumer.worker import Coordinator
from pgshovel.interfaces.groups_pb2 import GroupConfiguration
from tests.consumer.worker import (
    application,
    database,
    setup_application,
    temporary_database,
)


consumer_group_identifier = 'consumer-group'
consumer_identifier = 'consumer'


class DummyHandler(object):
    def __call__(self, group, configuration, events):
        pass


def test_assignment_manager_lifecycle(application, database):
    groups = setup_application(application, database)

    manager = AssignmentManager(application, consumer_identifier)
    manager.start()

    updates = Queue()
    manager.register_subscription(lambda *args: updates.put(args))

    group = 'users'
    manager.update_assignments({
        consumer_identifier: (group,),
    }, timeout=1)

    name, (old, new) = updates.get(timeout=1)
    assert name == group
    assert old is None
    assert new == groups[group]
    previous = new

    # This doesn't actually have to create the database and move it, but it's a
    # lot more convienient for testing right now.
    with temporary_database() as other:
        move_groups(application, (group,), other)

        name, (old, new) = updates.get(timeout=1)
        assert name == group
        assert old == previous
        assert old.database == database
        assert new.database == other
        previous = new

    manager.update_assignments({})
    name, (old, new) = updates.get(timeout=1)
    assert name == group
    assert old == previous
    assert new is None

    manager.stop(3)
    assert manager.result(0) is None, 'manager should be stopped without error'


def test_supervisor_lifecycle(application, database):
    groups = setup_application(application, database)

    application.environment.zookeeper.ensure_path(
        application.get_consumer_group_membership_path(consumer_group_identifier)()
    )

    supervisor = Supervisor(application, consumer_group_identifier, consumer_identifier, DummyHandler())
    supervisor.start()

    group = 'users'
    configuration = groups[group]

    operations = supervisor.notify_group_state_change(group, (None, configuration), timeout=3)
    assert operations[0] is None, 'no operation to perform on previous state'
    consumer = operations[1].result(timeout=1)
    assert consumer.ready.wait(timeout=3), 'consumer took too long to start'

    operations = supervisor.notify_group_state_change(group, (configuration, configuration), timeout=3)
    assert operations[0] is operations[1]
    assert operations[0].result(timeout=1) is consumer, 'consumer should be the same'

    with temporary_database() as other:
        other_configuration = GroupConfiguration()
        other_configuration.CopyFrom(configuration)
        other_configuration.database.CopyFrom(other)

        # TODO: It would be nice if this actually returned the updated configurations.
        # This needs to happen to ensure that PGQ is actually configured on the
        # destination, but since no rebalancer or assignment manager is
        # associated with the supervisor, the updated configuration needs to be
        # manually provided.
        move_groups(application, (group,), other, force=True)

        operations = supervisor.notify_group_state_change(group, (groups[group], other_configuration), timeout=3)
        assert operations[0].result(timeout=1) is consumer
        assert consumer.result(3) is None, 'consumer should stop cleanly'
        consumer = operations[1].result(timeout=1)  # new consumer
        assert consumer.ready.wait(timeout=3)

        operations = supervisor.notify_group_state_change(group, (other_configuration, None), timeout=3)
        assert operations[0].result(timeout=1) is consumer
        assert consumer.result(3) is None, 'consumer should stop cleanly'
        assert operations[1] is None

    supervisor.stop(3)
    assert supervisor.result(0) is None, 'supervisor should be stopped without error'


def test_supervisor_consumer_failure(application, database):
    groups = setup_application(application, database)

    application.environment.zookeeper.ensure_path(
        application.get_consumer_group_membership_path(consumer_group_identifier)()
    )

    class Explosion(Exception):
        pass

    class ExplodingCoordinator(Coordinator):
        def run(self):
            raise Explosion("I'm dead")

    class ExplodingSupervisor(Supervisor):
        Coordinator = ExplodingCoordinator

    supervisor = ExplodingSupervisor(application, consumer_group_identifier, consumer_identifier, DummyHandler())
    supervisor.start()

    group = 'users'
    supervisor.notify_group_state_change(group, (None, groups[group]), timeout=3)

    with pytest.raises(Explosion):
        supervisor.result(timeout=3)


def test_supervisor_collision(application, database):
    setup_application(application, database)

    zookeeper = application.environment.zookeeper

    path = application.get_consumer_group_membership_path(consumer_group_identifier)

    zookeeper.ensure_path(path())
    zookeeper.create(path(consumer_identifier))
    with pytest.raises(NodeExistsError):
        supervisor = Supervisor(application, consumer_group_identifier, consumer_identifier, DummyHandler())
        supervisor.start()
        supervisor.result(5)
