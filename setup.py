try:
    # See: http://bugs.python.org/issue15881#msg170215
    import multiprocessing
except ImportError:
    pass

import os
import operator
import sys
from collections import namedtuple
from setuptools import (
    Command,
    find_packages,
    setup,
)
from setuptools.command.test import test


PACKAGE_DIR = os.path.join('src', 'main', 'python')

Version = namedtuple(
    'Version',
    (
        'range',  # version range, for when used as a pip installed client library
        'strict', # strict version, for when used as a standalone service (or via docker)
    ),
)

packages = {
    'click': Version('~=4.0', '==4.1'),
    'futures': Version('~=3.0', '==3.0.3'),
    'kafka-python': Version('~=0.9', '==0.9.4'),
    'kazoo': Version('~=2.0', '==2.2.1'),
    'protobuf': Version('~=2.6', '==2.6.1'),
    'psycopg2': Version('~=2.6', '==2.6.1'),
    'pytest': Version('~=2.7', '==2.7.2'),
    'pytest-capturelog': Version('~=0.7', '==0.7'),
    'raven': Version('~=5.5', '==5.5.0'),
    'setuptools': Version('>=8.0', '==18.1'),
    'tabulate': Version('~=0.7', '==0.7.5'),
}

setup_requires = (
    'setuptools',
)

install_requires = (
    'click',
    'futures',
    'kazoo',
    'protobuf',
    'psycopg2',
    'tabulate',
)

extras_require = {
    'all': packages.keys(),
    'kafka': (
        'kafka-python',
    ),
    'sentry': (
        'raven',
    ),
}

tests_require = (
    'pytest',
)


def requirements(names, strict=False):
    get_version = operator.attrgetter('strict' if strict else 'range')
    return [''.join((name, get_version(packages[name]))) for name in names]


class TestCommand(test):
    user_options = [('pytest-args=', 'a', "arguments to pass to py.test runner")]

    def initialize_options(self):
        test.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        test.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


class RequirementsCommand(Command):
    user_options = [
        ('extras=', 'e', 'name of extras'),
        ('strict=', 's', 'strict'),
    ]

    def initialize_options(self):
        self.extras = None
        self.strict = False

    def finalize_options(self):
        if self.extras is not None and self.extras not in extras_require:
            raise ValueError('Invalid name of extras group, must be one of {0!r}'.format(extras.keys()))

        self.strict = bool(self.strict)

    def run(self):
        names = extras_require[self.extras] if self.extras is not None else install_requires
        for requirement in requirements(names, self.strict):
            sys.stdout.write('{0}\n'.format(requirement))


setup(
    name='pgshovel',
    version='0.3.0-dev',
    setup_requires=requirements(setup_requires),
    install_requires=requirements(install_requires),
    entry_points={
        'console_scripts': [
            'pgshovel = pgshovel.cli:__main__',
            'pgshovel-kafka-relay = pgshovel.relay.handlers.kafka:__main__ [kafka]',
            'pgshovel-stream-relay = pgshovel.relay.handlers.stream:__main__',
        ],
    },
    include_package_data=True,
    packages=find_packages(PACKAGE_DIR),
    package_dir={
        '': PACKAGE_DIR,
    },
    cmdclass = {
        'requirements': RequirementsCommand,
        'test': TestCommand,
    },
    tests_require=requirements(tests_require),
    extras_require=dict((key, requirements(values)) for key, values in extras_require.items()),
    license='Apache License 2.0',
)
