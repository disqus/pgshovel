try:
    # See: http://bugs.python.org/issue15881#msg170215
    import multiprocessing
except ImportError:
    pass

import os
from setuptools import (
    find_packages,
    setup,
)
from setuptools.command.test import test


PACKAGE_DIR = os.path.join('src', 'main', 'python')


class PyTest(test):
    def finalize_options(self):
        test.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest, sys
        errno = pytest.main(self.test_args)
        sys.exit(errno)


setup(
    name='pgshovel',
    version='0.2.3',
    setup_requires=(
        'setuptools>=8.0',
    ),
    install_requires=(
        'click~=4.0',
        'futures~=2.2',
        'kazoo~=2.0',
        'protobuf~=2.6',
        'psycopg2~=2.6',
        'tabulate~=0.7',
    ),
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
        'test': PyTest,
    },
    tests_require=(
        'pytest',
    ),
    extras_require={
        'msgpack': (
            'msgpack-python~=0.4',
        ),
        'kafka': (
            'kafka-python~=0.9',
        ),
        'sentry': (
            'raven',
        ),
    },
)
