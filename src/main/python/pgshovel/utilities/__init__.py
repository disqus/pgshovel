import importlib
import sys
from contextlib import contextmanager


def load(path):
    """
    Loads a module member from a lookup path (using ``path.to.module:member``
    syntax.)
    """
    module, name = path.split(':')
    return getattr(importlib.import_module(module), name)


@contextmanager
def import_extras(name):
    try:
        yield
    except ImportError as e:
        s = sys.stderr
        print >> s, '*' * 80
        print >> s, ''
        print >> s, 'This module requires that %r extras are installed.' % (name,)
        print >> s, ''
        print >> s, 'To install the necessary requirements, use:'
        print >> s, ''
        print >> s, '  pip install pgshovel[%s]' % (name,)
        print >> s, ''
        print >> s, '*' * 80
        raise


def unique(sequence):
    """
    Returns a new sequence containing the unique elements from the provided
    sequence, while preserving the same type and order of the original
    sequence.
    """
    result = []
    for item in sequence:
        if item in result:
            continue  # we already have seen this item

        result.append(item)

    return type(sequence)(result)
