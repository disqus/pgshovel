import logging

from pgshovel.interfaces.streams_pb2 import BeginOperation
from pgshovel.utilities.protobuf import get_oneof_value


logger = logging.getLogger(__name__)


def validate_bootstrap_state(state, offset, message):
    if state is None:
        return None

    # can be removed (considered "streaming") when xmin (first still running
    # transaction) of start tick on same node as bootstrap is greater than or
    # equal to bootstrap xmax (first unassigned txid). (this transaction
    # doesn't have to succeed, it just means that the ticker has advanced past
    # the end of the bootstrap snapshot) this could be a little bit more reliable
    # by using commit operation but then those would have to also include the
    # start/stop payloads? (not sure if that's worth it. this does also mean that
    # a batch must be started, even if failed, before mode switch can occur
    # which is a little weird)
    # TODO: this needs to also check correct node, obv
    operation = get_oneof_value(message.batch_operation, 'operation')
    if isinstance(operation, BeginOperation) and operation.start.snapshot.min > state.snapshot.max:
        logger.info('Caught up with replication stream!')
        state = None

    return state
