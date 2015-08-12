import functools

from pgshovel.utilities import load


class Component(object):
    @classmethod
    def configure(cls, configuration):
        raise NotImplementedError


def configure(configuration):
    return functools.partial(
        load(configuration['path']).configure,
        configuration.get('configuration', {}),
    )
