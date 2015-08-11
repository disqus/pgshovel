from __future__ import absolute_import

import itertools
import uuid

import pytest

from pgshovel.streams.interfaces_pb2 import (
    BatchIdentifier,
    BatchOperation,
    BeginOperation,
    Column,
    CommitOperation,
    Message,
    MutationOperation,
    RollbackOperation,
    Row,
    Snapshot,
    Tick,
    Timestamp,
)


DEFAULT_PUBLISHER = uuid.uuid1().bytes

batch_identifier = BatchIdentifier(id=1, node=uuid.uuid1().bytes)

begin = BeginOperation(
    start=Tick(
        id=1,
        snapshot=Snapshot(min=100, max=200),
        timestamp=Timestamp(seconds=0, nanos=0),
    ),
    end=Tick(
        id=2,
        snapshot=Snapshot(min=150, max=250),
        timestamp=Timestamp(seconds=10, nanos=0),
    ),
)

mutation = MutationOperation(
    id=1,
    schema='public',
    table='users',
    operation=MutationOperation.INSERT,
    identity_columns=['id'],
    new=Row(
        columns=[
            Column(name='id', integer64=1),
            Column(name='username', string='ted'),
        ],
    ),
    timestamp=Timestamp(seconds=0, nanos=0),
    transaction=1,
)

commit = CommitOperation()

rollback = RollbackOperation()


def reserialize(message):
    # This is a hack to get around errors with oneof field initialization in a
    # message constructor:: https://github.com/google/protobuf/issues/147
    return type(message).FromString(message.SerializeToString())


def copy(message, **replacements):
    updated = reserialize(message)
    for key, value in replacements.items():
        setattr(updated, key, value)
    return updated


def make_message(payload, sequence=1, publisher=DEFAULT_PUBLISHER):
    return reserialize(
        Message(
            header=Message.Header(
                publisher=publisher,
                sequence=sequence,
                timestamp=Timestamp(seconds=0, nanos=0),
            ),
            **payload
        ),
    )


def make_messages(payloads, publisher=DEFAULT_PUBLISHER):
    sequence = itertools.count()

    for payload in payloads:
        yield make_message(payload, next(sequence), publisher)


def make_batch_messages(batch_identifier, payloads, **kwargs):
    payloads = ({'batch_operation': BatchOperation(batch_identifier=batch_identifier, **payload)} for payload in payloads)
    return make_messages(payloads, **kwargs)


transaction = make_batch_messages(batch_identifier, (
    {'begin_operation': begin},
    {'mutation_operation': mutation},
    {'commit_operation': commit},
))


@pytest.yield_fixture
def message():
    yield make_message({
        'batch_operation': BatchOperation(
            batch_identifier=batch_identifier,
            commit_operation=commit,
        ),
    })
