import importlib


def load(path):
    """
    Loads a module member from a lookup path (using ``path.to.module:member``
    syntax.)
    """
    module, name = path.split(':')
    return getattr(importlib.import_module(module), name)
