import itertools

from pgshovel.utilities.partitioning import distribute


def test_distribute_simple():
    assert distribute('abc', 'xyz') == {
        'a': ['x'],
        'b': ['y'],
        'c': ['z'],
    }


def test_distribute_uneven():
    assert distribute('abc', 'x') == {
        'a': ['x'],
        'b': [],
        'c': [],
    }
    assert distribute('abc', 'xyz01') == {
        'a': ['0', '1'],  # note sorting
        'b': ['x', 'y'],
        'c': ['z'],
    }


def test_distribute_ordering():
    consumers = list(itertools.islice(itertools.imap(chr, itertools.count(97)), 10))
    resources = range(10)

    assert distribute(consumers, resources) == \
            distribute(reversed(consumers), resources) == \
            distribute(consumers, reversed(resources)), 'order should not matter'
