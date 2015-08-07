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
    begin,
    commit,
    make_messages,
    mutation,
    rollback,
)


def test_batch_iterator():
    messages = make_messages([
        {'begin': begin},
        {'mutation': mutation},
        {'mutation': mutation},
        {'mutation': mutation},
        {'commit': commit},
    ])
    batches = batched(states.validate(messages))

    batch, mutations = next(batches)
    assert batch == begin.batch
    assert list(mutations) == [mutation] * 3


def test_batch_iterator_early_exit():
    messages = make_messages([
        {'begin': begin},
        {'mutation': mutation},
    ])
    batches = batched(states.validate(messages))

    batch, mutations = next(batches)
    assert batch == begin.batch
    assert next(mutations) == mutation
    with pytest.raises(TransactionAborted):
        next(mutations)


def test_batch_iterator_rolled_back():
    messages = make_messages([
        {'begin': begin},
        {'mutation': mutation},
        {'rollback': rollback},
    ])
    batches = batched(states.validate(messages))

    batch, mutations = next(batches)
    assert batch == begin.batch
    assert next(mutations) == mutation
    with pytest.raises(TransactionCancelled):
        next(mutations)


def test_batch_restarted():
    incomplete = make_messages([
        {'begin': begin},
        {'mutation': mutation},
    ], publisher=uuid.uuid1().bytes)
    complete = make_messages([
        {'begin': begin},
        {'mutation': mutation},
        {'commit': commit},
    ], publisher=uuid.uuid1().bytes)
    messages = itertools.chain(incomplete, complete)

    batches = batched(states.validate(messages))

    # The first batch should be aborted, since it didn't end with a
    # commit/rollback before switching publishers.
    batch, mutations = next(batches)
    assert batch == begin.batch
    assert next(mutations) == mutation
    with pytest.raises(TransactionAborted):
        next(mutations)

    batch, mutations = next(batches)
    assert batch == begin.batch
    assert next(mutations) == mutation
    with pytest.raises(StopIteration):
        next(mutations)


def test_batch_no_mutations():
    messages = make_messages([
        {'begin': begin},
        {'commit': commit},
    ])
    batches = batched(states.validate(messages))

    batch, mutations = next(batches)
    assert batch == begin.batch
    with pytest.raises(StopIteration):
        next(mutations)
