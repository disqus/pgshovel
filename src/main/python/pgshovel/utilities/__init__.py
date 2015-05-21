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
