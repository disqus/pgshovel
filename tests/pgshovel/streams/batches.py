import itertools
import uuid

import pytest

from pgshovel.streams import states
from pgshovel.streams.batches import (
    TransactionAborted,
    TransactionCancelled,
    batched,
)
from tests.pgshovel.streams.fixtures import (
    batch_identifier,
    begin,
    commit,
    make_batch_messages,
    mutation,
    rollback,
)


def test_batch_iterator():
    messages = make_batch_messages(batch_identifier, [
        {'begin_operation': begin},
        {'mutation_operation': mutation},
        {'mutation_operation': mutation},
        {'mutation_operation': mutation},
        {'commit_operation': commit},
    ])
    batches = batched(states.validate(messages))

    received_batch_identifier, mutations = next(batches)
    assert received_batch_identifier == batch_identifier
    assert list(mutations) == [mutation] * 3


def test_batch_iterator_early_exit():
    messages = make_batch_messages(batch_identifier, [
        {'begin_operation': begin},
        {'mutation_operation': mutation},
    ])
    batches = batched(states.validate(messages))

    received_batch_identifier, mutations = next(batches)
    assert received_batch_identifier == batch_identifier
    assert next(mutations) == mutation
    with pytest.raises(TransactionAborted):
        next(mutations)


def test_batch_iterator_rolled_back():
    messages = make_batch_messages(batch_identifier, [
        {'begin_operation': begin},
        {'mutation_operation': mutation},
        {'rollback_operation': rollback},
    ])
    batches = batched(states.validate(messages))

    received_batch_identifier, mutations = next(batches)
    assert received_batch_identifier == batch_identifier
    assert next(mutations) == mutation
    with pytest.raises(TransactionCancelled):
        next(mutations)


def test_batch_restarted():
    incomplete = make_batch_messages(batch_identifier, [
        {'begin_operation': begin},
        {'mutation_operation': mutation},
    ], publisher=uuid.uuid1().bytes)
    complete = make_batch_messages(batch_identifier, [
        {'begin_operation': begin},
        {'mutation_operation': mutation},
        {'commit_operation': commit},
    ], publisher=uuid.uuid1().bytes)
    messages = itertools.chain(incomplete, complete)

    batches = batched(states.validate(messages))

    # The first batch should be aborted, since it didn't end with a
    # commit/rollback before switching publishers.
    received_batch_identifier, mutations = next(batches)
    assert received_batch_identifier == batch_identifier
    assert next(mutations) == mutation
    with pytest.raises(TransactionAborted):
        next(mutations)

    received_batch_identifier, mutations = next(batches)
    assert received_batch_identifier == batch_identifier
    assert next(mutations) == mutation
    with pytest.raises(StopIteration):
        next(mutations)


def test_batch_no_mutations():
    messages = make_batch_messages(batch_identifier, [
        {'begin_operation': begin},
        {'commit_operation': commit},
    ])
    batches = batched(states.validate(messages))

    received_batch_identifier, mutations = next(batches)
    assert received_batch_identifier == batch_identifier
    with pytest.raises(StopIteration):
        next(mutations)
