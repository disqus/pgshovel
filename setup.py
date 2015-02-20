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
    install_requires=(
        'futures',
        'kazoo',
        'pgqueue',
        'protobuf',
        'psycopg2',
        'tabulate',
    ),
    entry_points={
        'console_scripts': [
            'pgshovel-create-group = pgshovel.cli:create_group',
            'pgshovel-drop-groups = pgshovel.cli:drop_groups',
            'pgshovel-initialize-cluster = pgshovel.cli:initialize_cluster',
            'pgshovel-inspect-group = pgshovel.cli:inspect_group',
            'pgshovel-list-groups = pgshovel.cli:list_groups',
            'pgshovel-move-groups = pgshovel.cli:move_groups',
            'pgshovel-shell = pgshovel.cli:shell',
            'pgshovel-update-group = pgshovel.cli:update_group',
        ],
    },
    packages=find_packages(PACKAGE_DIR),
    package_dir={
        '': PACKAGE_DIR,
    },
    classifier=['Private :: Do Not Upload'],
    cmdclass = {
        'test': PyTest,
    },
    tests_require=(
        'pytest',
        'pytest-cov',
    ),
)
