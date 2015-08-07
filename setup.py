try:
    # See: http://bugs.python.org/issue15881#msg170215
    import multiprocessing
except ImportError:
    pass

import os
import sys
from setuptools import (
    Command,
    find_packages,
    setup,
)
from setuptools.command.test import test


PACKAGE_DIR = os.path.join('src', 'main', 'python')

packages = {
    'click': 'click~=4.0',
    'futures': 'futures~=3.0',
    'kafka-python': 'kafka-python~=0.9',
    'kazoo': 'kazoo~=2.0',
    'msgpack-python': 'msgpack-python~=0.4',
    'protobuf': 'protobuf~=2.6',
    'psycopg2': 'psycopg2~=2.6',
    'pytest': 'pytest~=2.7',
    'pytest-capturelog': 'pytest-capturelog~=0.7',
    'sentry': 'raven~=5.5',
    'setuptools': 'setuptools>=8.0',
    'tabulate': 'tabulate~=0.7',
}

install_requires = (
    packages['click'],
    packages['futures'],
    packages['kazoo'],
    packages['protobuf'],
    packages['psycopg2'],
    packages['tabulate'],
)

extras = {
    'all': tuple(packages.values()),
    'kafka': (
        packages['kafka-python'],
    ),
    'msgpack': (
        packages['msgpack-python'],
    ),
    'sentry': (
        packages['sentry'],
    ),
}


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
    user_options = [('extras=', 'e', 'name of extras')]

    def initialize_options(self):
        self.extras = None

    def finalize_options(self):
        if self.extras is not None and self.extras not in extras:
            raise ValueError('Invalid name of extras group, must be one of {0!r}'.format(extras.keys()))

    def run(self):
        requirements = extras[self.extras] if self.extras is not None else install_requires
        for requirement in requirements:
            sys.stdout.write('{0}\n'.format(requirement))


setup(
    name='pgshovel',
    version='0.3.0-dev',
    setup_requires=(
        packages['setuptools'],
    ),
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'pgshovel = pgshovel.cli:__main__',
            'pgshovel-kafka-relay = pgshovel.contrib.kafka:__main__ [kafka]',
            'pgshovel-stream-relay = pgshovel.relay:__main__',
        ],
    },
    include_package_data=True,
    packages=find_packages(PACKAGE_DIR),
    package_dir={
        '': PACKAGE_DIR,
    },
    classifiers=['Private :: Do Not Upload'],
    cmdclass = {
        'requirements': RequirementsCommand,
        'test': TestCommand,
    },
    tests_require=(
        packages['pytest'],
    ),
    extras_require=extras,
)
