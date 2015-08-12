import logging
import uuid

from pgshovel.interfaces.replication_pb2 import State
from pgshovel.interfaces.streams_pb2 import (
    BeginOperation,
    CommitOperation,
    MutationOperation,
    RollbackOperation,
)
from pgshovel.streams.utilities import (
    FormattedBatchIdentifier,
    FormattedSnapshot,
)
from pgshovel.utilities.postgresql import txid_visible_in_snapshot
from pgshovel.utilities.protobuf import get_oneof_value


logger = logging.getLogger(__name__)


class Target(object):
    def __init__(self, cluster, set):
        self.cluster = cluster
        self.set = set

    def run(self, loader, stream):
        state = self.get_state()

        if state is None:
            logger.info('Bootstrapping new replication target with %s...', loader)
            with loader.fetch() as (bootstrap_state, loaders):
                for table, records in loaders:
                    logger.info('Loading records from %s...', table.name)
                    self.load(table, records)

                state = State(bootstrap_state=bootstrap_state)
                self.commit(state)
                logger.debug(
                    'Successfully bootstrapped from %s using snapshot: %s',
                    uuid.UUID(bytes=bootstrap_state.node).hex,
                    FormattedSnapshot(bootstrap_state.snapshot),
                )

        logger.info('Starting to consume from %s...', stream)
        for state, offset, message in stream.consume(state):
            operation = get_oneof_value(message.batch_operation, 'operation')
            if isinstance(operation, BeginOperation):
                logger.debug(
                    'Beginning %s (%s to %s)...',
                    FormattedBatchIdentifier(message.batch_operation.batch_identifier),
                    FormattedSnapshot(operation.start.snapshot),
                    FormattedSnapshot(operation.end.snapshot),
                )
            elif isinstance(operation, MutationOperation):
                # Skip any messages that were part of the bootstrap snapshot.
                if state.HasField('bootstrap_state') and txid_visible_in_snapshot(operation.transaction, state.bootstrap_state.snapshot):
                    logger.debug('Skipping operation that was visible in bootstrap snapshot.')
                else:
                    self.apply(state, operation)
            elif isinstance(operation, CommitOperation):
                logger.info(
                    'Committing %s.',
                    FormattedBatchIdentifier(message.batch_operation.batch_identifier),
                )
                self.commit(state)
            elif isinstance(operation, RollbackOperation):
                logger.info(
                    'Rolling back %s.',
                    FormattedBatchIdentifier(message.batch_operation.batch_identifier),
                )
                self.rollback(state)
            else:
                raise AssertionError('Received unexpected operation!')

    def get_state(self):
        raise NotImplementedError

    def set_state(self, state):
        raise NotImplementedError

    def load(self, table, records):
        raise NotImplementedError

    def apply(self, state, mutation):
        raise NotImplementedError

    def commit(self, state):
        raise NotImplementedError

    def rollback(self, state):
        raise NotImplementedError
